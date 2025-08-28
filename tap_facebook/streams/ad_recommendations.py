"""Stream class for AdsStream."""

from __future__ import annotations

import typing as t

from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream
from tap_facebook.streams.ads import AdsStream


class AdRecommendationsStream(FacebookStream):
    """Child stream for ad recommendations."""

    name = "ad_recommendations"
    parent_stream_type = AdsStream
    path = None  # no API call, data comes from parent stream
    primary_keys: t.ClassVar[list[str]] = ["ad_id"]
    state_partitioning_keys: t.ClassVar[list] = []


    schema = PropertiesList(
        Property("ad_id", StringType),
        Property("updated_time", DateTimeType),
        Property("blame_field", StringType),
        Property("code", IntegerType),
        Property("confidence", StringType),
        Property("importance", StringType),
        Property("message", StringType),
        Property("title", StringType),
    ).to_dict()

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        if context is None:
            return []

        # Only process recommendation contexts
        if context.get("_child_type") != "recommendation":
            return []

        yield {
            "ad_id": context["ad_id"],
            "updated_time": context["updated_time"],
            "blame_field": context.get("blame_field"),
            "code": context.get("code"),
            "confidence": context.get("confidence"),
            "importance": context.get("importance"),
            "message": context.get("message"),
            "title": context.get("title"),
        }
