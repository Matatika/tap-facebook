"""Stream class for AdVideos."""

from __future__ import annotations

import typing as t

from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
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
        "account_id",
        "ad_breaks",
        "backdated_time",
        "backdated_time_granularity",
        "content_category",
        "content_tags",
        "created_time",
        "custom_labels",
        "description",
        "embed_html",
        "embeddable",
        "event",
        "format",
        "from_object",
        "icon",
        "is_crosspost_video",
        "is_crossposting_eligible",
        "is_episode",
        "is_instagram_eligible",
        # "is_reference_only",
        "length",
        "live_status",
        # "music_video_copyright",
        "permalink_url",
        "place",
        "post_views",
        "premiere_living_room_status",
        "privacy",
        "published",
        "scheduled_publish_time",
        "source",
        "status_processing_progress",
        "status_value",
        "title",
        "universal_video_id",
        "views",
    ]

    name = "advideos"
    path = "advideos"
    tap_stream_id = "videos"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "updated_time"
    primary_keys = ["id"] # noqa: RUF012

    schema = PropertiesList(
        Property("id", StringType),
        Property("account_id", StringType),
        Property("ad_breaks", StringType),
        Property("backdated_time", DateTimeType),
        Property("backdated_time_granularity", StringType),
        Property("content_category", StringType),
        Property("content_tags", StringType),
        Property("created_time", StringType),
        Property("custom_labels", StringType),
        Property("description", StringType),
        Property("embed_html", StringType),
        Property("embeddable", BooleanType),
        Property("event", StringType),
        Property("format", ArrayType(ObjectType())),
        Property("from_object", StringType),
        Property("icon", StringType),
        Property("is_crosspost_video", BooleanType),
        Property("is_crossposting_eligible", BooleanType),
        Property("is_episode", BooleanType),
        Property("is_instagram_eligible", BooleanType),
        Property("is_reference_only", BooleanType),
        Property("length", NumberType),
        Property("live_status", StringType),
        Property("music_video_copyright", StringType),
        Property("permalink_url", StringType),
        Property("place", StringType),
        Property("post_views", IntegerType),
        Property("premiere_living_room_status", StringType),
        Property("privacy", ObjectType()),
        Property("published", BooleanType),
        Property("scheduled_publish_time", DateTimeType),
        Property("source", StringType),
        Property("status_processing_progress", IntegerType),
        Property("status_value", StringType),
        Property("title", StringType),
        Property("universal_video_id", StringType),
        Property("updated_time", DateTimeType),
        Property("views", IntegerType),
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
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 50, "fields": ",".join(self.columns)}
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
        account_id = context["_current_account_id"]
        try:
            for record in super().get_records(context):
                record["account_id"] = account_id
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

