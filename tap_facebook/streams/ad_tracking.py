"""Stream class for AdTracking."""

from __future__ import annotations

import typing as t

from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream
from tap_facebook.streams.ads import AdsStream


class AdTrackingStream(FacebookStream):
    """Child stream for ad tracking specs."""

    name = "ad_tracking_specs"
    parent_stream_type = AdsStream  # parent = ads
    primary_keys: t.ClassVar[list[str]] = ["ad_id"]
    state_partitioning_keys: t.ClassVar[list] = []


    schema = PropertiesList(
        Property("ad_id", StringType),
        Property("updated_time", DateTimeType),
        Property("application", ArrayType(StringType)),
        Property("post", ArrayType(StringType)),
        Property("conversion_id", ArrayType(StringType)),
        Property("action_type", ArrayType(StringType)),
        Property("post_type", ArrayType(StringType)),
        Property("page", ArrayType(StringType)),
        Property("creative", ArrayType(StringType)),
        Property("dataset", ArrayType(StringType)),
        Property("event", ArrayType(StringType)),
        Property("event_creator", ArrayType(StringType)),
        Property("event_type", ArrayType(StringType)),
        Property("fb_pixel", ArrayType(StringType)),
        Property("fb_pixel_event", ArrayType(StringType)),
        Property("leadgen", ArrayType(StringType)),
        Property("object", ArrayType(StringType)),
        Property("object_domain", ArrayType(StringType)),
        Property("offer", ArrayType(StringType)),
        Property("offer_creator", ArrayType(StringType)),
        Property("offsite_pixel", ArrayType(StringType)),
        Property("page_parent", ArrayType(StringType)),
        Property("post_object", ArrayType(StringType)),
        Property("post_object_wall", ArrayType(StringType)),
        Property("question", ArrayType(StringType)),
        Property("question_creator", ArrayType(StringType)),
        Property("response", ArrayType(StringType)),
        Property("subtype", ArrayType(StringType)),
    ).to_dict()

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        if not context or context.get("_child_type") != "tracking_spec":
            return []

        yield {
            "ad_id": context["ad_id"],
            "updated_time": context["updated_time"],
            "application": context.get("application"),
            "post": context.get("post"),
            "conversion_id": context.get("conversion_id"),
            "action_type": context.get("action.type"),
            "post_type": context.get("post.type"),
            "page": context.get("page"),
            "creative": context.get("creative"),
            "dataset": context.get("dataset"),
            "event": context.get("event"),
            "event_creator": context.get("event.creator"),
            "event_type": context.get("event_type"),
            "fb_pixel": context.get("fb_pixel"),
            "fb_pixel_event": context.get("fb_pixel_event"),
            "leadgen": context.get("leadgen"),
            "object": context.get("object"),
            "object_domain": context.get("object.domain"),
            "offer": context.get("offer"),
            "offer_creator": context.get("offer.creator"),
            "offsite_pixel": context.get("offsite_pixel"),
            "page_parent": context.get("page.parent"),
            "post_object": context.get("post.object"),
            "post_object_wall": context.get("post.object.wall"),
            "question": context.get("question"),
            "question_creator": context.get("question.creator"),
            "response": context.get("response"),
            "subtype": context.get("subtype"),
        }
