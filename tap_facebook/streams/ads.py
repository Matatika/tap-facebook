"""Stream class for AdsStream."""

from __future__ import annotations

import typing as t

from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook import BufferDeque
from tap_facebook.client import IncrementalAdsStream
from tap_facebook.streams.creative import CreativeStream


class AdsStream(IncrementalAdsStream):
    """Ads stream class.

    columns: columns which will be added to fields parameter in api
    name: stream name
    account_id: facebook account
    path: path which will be added to api url in client.py
    schema: instream schema
    tap_stream_id = stream id.
    """

    columns = [  # noqa: RUF012
        "id",
        "account_id",
        "adset_id",
        "campaign_id",
        "bid_type",
        "bid_info",
        "status",
        "updated_time",
        "created_time",
        "name",
        "effective_status",
        "last_updated_by_app_id",
        "source_ad_id",
        "creative",
        "tracking_specs",
        "conversion_specs",
        "recommendations",
        "configured_status",
        "conversion_domain",
        "bid_amount",
    ]

    columns_remaining = ["adlabels"]  # noqa: RUF012

    name = "ads"
    filter_entity = "ad"

    path = "ads"

    primary_keys = ["id", "updated_time"]  # noqa: RUF012
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "updated_time"
    creative_batch_size = 20
    creative_subrequest_limit = 50

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        # Buffer up to 20 creatives per subrequest and 50 subrequests per Graph batch
        self._creative_ids_buffer = BufferDeque(
            maxlen=self.creative_batch_size * self.creative_subrequest_limit
        )
        self._creative_account: str | None = None

    def _get_creative_stream(self) -> CreativeStream | None:
        """Return the creatives child stream instance if selected."""
        for child_stream in self.child_streams:
            if isinstance(child_stream, CreativeStream) and (
                child_stream.selected or child_stream.has_selected_descendents
            ):
                return child_stream
        return None

    def _flush_creative_buffer(self) -> None:
        """Flush buffered creatives to the creative child stream using batch context."""
        if not self._creative_ids_buffer:
            return

        creative_stream = self._get_creative_stream()
        if not creative_stream:
            self._creative_ids_buffer.clear()
            self._creative_account = None
            return

        unique_ids = list(dict.fromkeys(self._creative_ids_buffer))
        chunk_size = self.creative_batch_size * self.creative_subrequest_limit
        for idx in range(0, len(unique_ids), chunk_size):
            context = {
                "_current_account_id": self._creative_account,
                "_child_type": "creative",
                "creative_ids": unique_ids[idx : idx + chunk_size],
            }
            creative_stream.sync(context=context)

        self._creative_ids_buffer.clear()
        self._creative_account = None


    schema = PropertiesList(
        Property("bid_type", StringType),
        Property("account_id", StringType),
        Property("campaign_id", StringType),
        Property("adset_id", StringType),
        Property("bid_amount", IntegerType),
        Property(
            "bid_info",
            ObjectType(
                Property("CLICKS", IntegerType),
                Property("ACTIONS", IntegerType),
                Property("REACH", IntegerType),
                Property("IMPRESSIONS", IntegerType),
                Property("SOCIAL", IntegerType),
            ),
        ),
        Property("status", StringType),
        Property(
            "creative",
            ObjectType(Property("creative_id", StringType), Property("id", StringType)),
        ),
        Property("id", StringType),
        Property("updated_time", DateTimeType),
        Property("created_time", DateTimeType),
        Property("name", StringType),
        Property("effective_status", StringType),
        Property("last_updated_by_app_id", StringType),
        Property(
            "recommendations",
            ArrayType(
                ObjectType(
                    Property("blame_field", StringType),
                    Property("code", IntegerType),
                    Property("confidence", StringType),
                    Property("importance", StringType),
                    Property("message", StringType),
                    Property("title", StringType),
                ),
            ),
        ),
        Property("source_ad_id", StringType),
        Property(
            "tracking_specs",
            ArrayType(
                ObjectType(
                    Property(
                        "application",
                        ArrayType(ObjectType(Property("items", StringType))),
                    ),
                    Property("post", ArrayType(StringType)),
                    Property("conversion_id", ArrayType(StringType)),
                    Property("action.type", ArrayType(Property("items", StringType))),
                    Property("post.type", ArrayType(Property("items", StringType))),
                    Property("page", ArrayType(Property("items", StringType))),
                    Property("creative", ArrayType(Property("items", StringType))),
                    Property("dataset", ArrayType(Property("items", StringType))),
                    Property("event", ArrayType(Property("items", StringType))),
                    Property("event.creator", ArrayType(Property("items", StringType))),
                    Property("event_type", ArrayType(Property("items", StringType))),
                    Property("fb_pixel", ArrayType(Property("items", StringType))),
                    Property(
                        "fb_pixel_event",
                        ArrayType(Property("items", StringType)),
                    ),
                    Property("leadgen", ArrayType(Property("items", StringType))),
                    Property("object", ArrayType(Property("items", StringType))),
                    Property("object.domain", ArrayType(Property("items", StringType))),
                    Property("offer", ArrayType(Property("items", StringType))),
                    Property("offer.creator", ArrayType(Property("items", StringType))),
                    Property("offsite_pixel", ArrayType(Property("items", StringType))),
                    Property("page.parent", ArrayType(Property("items", StringType))),
                    Property("post.object", ArrayType(Property("items", StringType))),
                    Property(
                        "post.object.wall",
                        ArrayType(Property("items", StringType)),
                    ),
                    Property("question", ArrayType(Property("items", StringType))),
                    Property(
                        "question.creator",
                        ArrayType(Property("items", StringType)),
                    ),
                    Property("response", ArrayType(Property("items", StringType))),
                    Property("subtype", ArrayType(Property("items", StringType))),
                ),
            ),
        ),
        Property(
            "conversion_specs",
            ArrayType(
                ObjectType(
                    Property("action.type", ArrayType(StringType)),
                    Property("conversion_id", ArrayType(StringType)),
                ),
            ),
        ),
        Property("configured_status", StringType),
    ).to_dict()

    tap_stream_id = "ads"
    request_limit = 1000

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/ads?fields={self.columns}"

    def generate_child_contexts(
    self,
    record: dict,
    context: dict | None = None,
    ) -> t.Iterable[dict]:
        base_context = {
            "ad_id": record["id"],
            "updated_time": record["updated_time"],
            "_current_account_id": context.get("_current_account_id") if context else None,
        }

        # Context for creatives child stream
        if "creative" in record and "id" in record["creative"]:
            yield {
                **base_context,
                "creative_id": record["creative"]["id"],
                "_child_type": "creative",
            }

        # Context(s) for recommendations child stream
        for recommendation in record.get("recommendations", []):
            yield {**base_context, **recommendation, "_child_type": "recommendation"}

        # Tracking specs child
        for spec in record.get("tracking_specs", []):
            yield {**base_context, **spec, "_child_type": "tracking_spec"}

    def _sync_children(self, child_context: dict | None) -> None:
        """Batch creative child contexts before syncing."""
        if child_context and child_context.get("_child_type") == "creative":
            account_id = child_context.get("_current_account_id")
            creative_id = child_context.get("creative_id")

            if self._creative_ids_buffer and account_id and account_id != self._creative_account:
                self._flush_creative_buffer()

            self._creative_account = account_id or self._creative_account
            if creative_id:
                self._creative_ids_buffer.append(creative_id)

            # Flush when we hit the max batch capacity (20 ids * 50 subrequests)
            max_capacity = (
                self.creative_batch_size * self.creative_subrequest_limit
            )
            if len(self._creative_ids_buffer) >= max_capacity:
                self._flush_creative_buffer()
            return

        super()._sync_children(child_context)

    def _sync_records(
        self,
        context: dict | None = None,
        *,
        write_messages: bool = True,
    ) -> t.Generator[dict, t.Any, t.Any]:
        try:
            yield from super()._sync_records(context=context, write_messages=write_messages)
        finally:
            self._flush_creative_buffer()
