"""Stream class for AdVideos."""

from __future__ import annotations

import typing as t

from dateutil import parser
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)
from typing_extensions import override

from tap_facebook import BufferDeque
from tap_facebook.client import FacebookStream, SkipAccountError

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class AdVideos(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/ad-image/."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    account_id: facebook account
    path: path which will be added to api url in client.py
    schema: instream schema
    tap_stream_id = stream id
    """

    columns = [  # noqa: RUF012
        "id",
        "updated_time",
        "created_time",
    ]

    name = "advideos"
    path = "advideos"
    tap_stream_id = "videos"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "updated_time"
    primary_keys = ["id"] # noqa: RUF012

    schema = PropertiesList(
        Property("id", StringType),
        Property("created_time", StringType),
        Property("updated_time", DateTimeType),
        Property("account_id", StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/advideos"


    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        params: dict = {"limit": 200, "fields": ",".join(self.columns)}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        bookmark = self.get_starting_timestamp(context)
        account_id = context["_current_account_id"]
        try:
            for record in super().get_records(context):
                record["account_id"] = account_id
                if record.get("updated_time") and (
                    bookmark is None or parser.isoparse(record["updated_time"]) > bookmark
                ):
                    yield record
        except SkipAccountError as e:
            self.logger.warning("Account %s skipped due to server error: %s", account_id, e)
            return  # stops this account, continues next partition

    @override
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self.video_ids_buffer = BufferDeque(maxlen=20)

    @override
    def parse_response(self, response: requests.Response) -> t.Iterator[dict]:
        yield from super().parse_response(response)

        # make sure we process the remaining buffer entries
        self.video_ids_buffer.finalize()

    @override
    def generate_child_contexts(self, record, context):  # noqa: ANN001, ANN201
        self.video_ids_buffer.append(record["id"])

        with self.video_ids_buffer as buf:
            if buf.flush:
                yield {"video_ids": buf}

