"""Stream class for PageVideos."""

from __future__ import annotations

import typing as t

from dateutil import parser
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.singerlib.catalog import Metadata
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)
from typing_extensions import override

from tap_facebook.client import FacebookStream, SkipAccountError
from tap_facebook.streams.pages import PagesStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class PageVideosStream(FacebookStream):
    """https://developers.facebook.com/docs/graph-api/reference/page/videos/."""

    columns = [  # noqa: RUF012
        "id",
        "updated_time",
        "created_time",
        "title",
        "description",
        "permalink_url",
        "embed_html",
        "format",
        "source",
        "length",
    ]

    name = "page_videos"
    path = "videos"
    tap_stream_id = "page_videos"
    parent_stream_type = PagesStream
    state_partitioning_keys: t.ClassVar[list[str]] = ["page_id"]
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "updated_time"
    primary_keys = ["id"]  # noqa: RUF012

    schema = PropertiesList(
        Property("id", StringType),
        Property("page_id", StringType),
        Property("created_time", StringType),
        Property("updated_time", DateTimeType),
        Property("title", StringType),
        Property("description", StringType),
        Property("permalink_url", StringType),
        Property("embed_html", StringType),
        Property(
            "format",
            ArrayType(
                ObjectType(
                    Property("embed_html", StringType),
                    Property("filter", StringType),
                    Property("height", NumberType),
                    Property("picture", StringType),
                    Property("width", NumberType),
                ),
            ),
        ),
        Property("source", StringType),
        Property("length", NumberType),
    ).to_dict()

    @override
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self._page_limits: dict[str, int] = {}

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        page_id = context["page_id"]
        return f"https://graph.facebook.com/{version}/{page_id}/videos"

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        page_id = context["page_id"]
        fields = {
            c
            for c in self.columns
            if (m := self.metadata[("properties", c)]).selected is not False
            or m.inclusion == Metadata.InclusionType.AUTOMATIC
        }
        params: dict = {
            "limit": self._page_limits[page_id],
            "fields": ",".join(fields),
        }
        if next_page_token is not None:
            params["after"] = next_page_token
        return params

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        bookmark = self.get_starting_timestamp(context)
        page_id = context["page_id"]
        self._page_limits.setdefault(page_id, self.config["limit"])
        try:
            for record in super().get_records(context):
                record["page_id"] = page_id
                if record.get("updated_time") and (
                    bookmark is None or parser.isoparse(record["updated_time"]) > bookmark
                ):
                    yield record
        except SkipAccountError as e:
            self.logger.warning("Page %s skipped due to server error: %s", page_id, e)

    @override
    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code == 500
            and "please reduce the amount of data" in str(response.content).lower()
        ):
            # URL path: /{version}/{page_id}/videos?...
            page_id = response.request.path_url.split("/", 3)[-2]
            new_limit = self._page_limits[page_id] // 2
            self._page_limits[page_id] = new_limit
            self.logger.warning(
                "Response too large; reducing limit to %s and retrying.",
                new_limit,
            )
            msg = f"500 Server Error: data too large, retrying with limit={new_limit}"
            raise RetriableAPIError(msg, response)
        super().validate_response(response)
