"""Stream class for AdVideoThumbnails."""

from __future__ import annotations

import typing as t

from singer_sdk.typing import (
    BooleanType,
    IntegerType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream
from tap_facebook.streams.ad_videos import AdVideos


class AdVideoThumbnails(FacebookStream):
    """https://developers.facebook.com/docs/graph-api/reference/video-thumbnail/."""

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
        "height",
        "is_preferred",
        "name",
        "scale",
        "uri",
        "width",
    ]

    parent_stream_type = AdVideos
    name = "advideothumbnails"
    path = "thumbnails"
    tap_stream_id = "videothumbnails"
    state_partitioning_keys: t.ClassVar[list] = []

    schema = PropertiesList(
        Property("id", StringType),
        Property("height", IntegerType),
        Property("is_preferred", BooleanType),
        Property("name", StringType),
        Property("scale", NumberType),
        Property("uri", StringType),
        Property("width", IntegerType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        return f"https://graph.facebook.com/{version}/{context['video_id']}/thumbnails?fields={self.columns}"
