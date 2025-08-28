"""Stream class for AdVideoThumbnails."""

from __future__ import annotations

import typing as t

import backoff
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

if t.TYPE_CHECKING:
    import requests


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
        "uri",
    ]

    name = "advideothumbnails"
    path = "thumbnails"
    tap_stream_id = "videothumbnails"
    state_partitioning_keys: t.ClassVar[list] = []
    parent_stream_type = AdVideos

    schema = PropertiesList(
        Property("video_id", StringType),
        Property("id", StringType),
        Property("height", IntegerType),
        Property("is_preferred", BooleanType),
        Property("name", StringType),
        Property("scale", NumberType),
        Property("uri", StringType),
        Property("width", IntegerType),
    ).to_dict()

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        video_ids = ",".join(context["video_ids"])
        return f"https://graph.facebook.com/{version}/?ids={video_ids}&fields=thumbnails{{{','.join(self.columns)}}}"

    def parse_response(self, response: requests.response):  # noqa: ANN201
        """Transform the API response into records and attach video_id."""
        data = response.json()  # parse JSON

        for video_id, video_data in data.items():
            for thumb in video_data.get("thumbnails", {}).get("data", []):
                thumb["video_id"] = video_id
                yield thumb

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Setting to None will retry indefinitely.

        Returns:
            int: limit
        """
        return 12
    def backoff_wait_generator(self) -> t.Generator[float, None, None]:
        return backoff.constant(interval=300)
