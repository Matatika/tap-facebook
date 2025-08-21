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

from tap_facebook.client import FacebookStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


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
    replication_key = "id"

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
        Property("updated_time", StringType),
        Property("views", IntegerType),
    ).to_dict()

    def generate_child_contexts(
            self,
            record: Record,
    ) -> t.Iterable[Context | None]:
        """Generates child contexts from a given record.

        This method takes an input record to produce one or more child contexts.
        The operation is aimed at further processing or analysis by organizing
        information into separate contextual pieces.

        Arguments:
            record: A Record instance representing the data to process.

        Yields:
            Context or None: Each generated child context derived from the input record.
        """
        return {"video_id": record["id"]}

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/advideos?fields={self.columns}"

