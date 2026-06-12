"""Stream class for VideoSource."""

from __future__ import annotations

import typing as t

from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)

from singer_sdk.exceptions import FatalAPIError

from tap_facebook.client import FacebookStream
from tap_facebook.streams.creative import CreativeStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class VideoSourceStream(FacebookStream):
    """https://developers.facebook.com/docs/graph-api/reference/video/#source-url."""

    columns = [  # noqa: RUF012
        "id",
        "source",
        "title",
        "description",
        "created_time",
        "updated_time",
    ]

    name = "video_source"
    path = "video"
    tap_stream_id = "video_source"
    parent_stream_type = CreativeStream
    primary_keys = ["id"]  # noqa: RUF012
    state_partitioning_keys: t.ClassVar[list[str]] = []

    schema = PropertiesList(
        Property("id", StringType),
        Property("source", StringType),
        Property("title", StringType),
        Property("description", StringType),
        Property("created_time", StringType),
        Property("updated_time", DateTimeType),
    ).to_dict()

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        return f"https://graph.facebook.com/{version}/{context['video_id']}"

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None = None,  # noqa: ANN401, ARG002
    ) -> dict[str, str]:
        return {"fields": ",".join(self.columns)}

    def parse_response(self, response: requests.Response) -> list:
        data = response.json()
        if isinstance(data, dict):
            return [data]
        return []

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if not context or not context.get("video_id"):
            return []
        try:
            yield from super().get_records(context)
        except FatalAPIError as e:
            self.logger.warning("Video %s skipped: %s", context.get("video_id"), e)
