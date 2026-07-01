"""Stream class for AdsStream."""

from __future__ import annotations

import json
import typing as t

import pendulum

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

from tap_facebook.client import IncrementalAdsStream, SkipAccountError

if t.TYPE_CHECKING:
    import requests


class _ReduceLimitError(Exception):
    """Raised when Facebook returns 500 due to too much data; signals restart with lower limit."""


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
        "creative{id,effective_object_story_id,video_id}",
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
            ObjectType(
                Property("creative_id", StringType),
                Property("id", StringType),
                Property("effective_object_story_id", StringType),
                Property("video_id", StringType),
            ),
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

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self._account_limits: dict[str, int] = {}

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/ads?fields={self.columns}"

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        account_id = context["_current_account_id"]
        self._account_limits.setdefault(account_id, 1000)
        params: dict[str, t.Any] = super().get_url_params(context, next_page_token)
        params["effective_status"] = json.dumps(["ACTIVE", "PAUSED", "ARCHIVED"])
        params["limit"] = self._account_limits[account_id]
        return params

    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code == 500
            and "please reduce the amount of data" in str(response.content).lower()
        ):
            raise _ReduceLimitError
        super().validate_response(response)

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        sync_end_date = pendulum.parse(
            self.config.get("end_date", pendulum.today().to_date_string()),
        ).date()
        today = pendulum.today("UTC").date()
        report_start = self._get_start_date(context)
        account_id = context["_current_account_id"]

        # Start with one chunk covering the full remaining range
        effective_end = min(sync_end_date, today)
        time_increment = max((effective_end - report_start).days, 1)

        self._last_window_end = None
        while report_start <= sync_end_date:
            report_end = min(report_start.add(days=time_increment), today, sync_end_date)
            chunk_context = {
                **context,
                "_since": report_start.to_date_string(),
                "_until": report_end.to_date_string(),
            }
            self.logger.info(
                "Fetching records for account %s from %s to %s (chunk=%d days)",
                account_id,
                chunk_context["_since"],
                chunk_context["_until"],
                time_increment,
            )
            try:
                yield from super(IncrementalAdsStream, self).get_records(chunk_context)
            except SkipAccountError as e:
                self.logger.warning("Account %s skipped due to server error: %s", account_id, e)
                return
            except _ReduceLimitError:
                current_limit = self._account_limits[account_id]
                if current_limit > 1:
                    new_limit = max(current_limit // 2, 1)
                    self._account_limits[account_id] = new_limit
                    self.logger.warning(
                        "Response too large for account %s; reducing limit to %d and retrying.",
                        account_id,
                        new_limit,
                    )
                    continue  # retry same chunk with smaller page limit
                if time_increment > 1:
                    time_increment = max(time_increment // 2, 1)
                    self.logger.warning(
                        "Limit already at minimum for account %s; halving time increment to %d days and retrying.",
                        account_id,
                        time_increment,
                    )
                    continue  # retry same chunk with smaller time window
                self.logger.warning(
                    "Cannot reduce further for account %s; skipping chunk %s-%s.",
                    account_id,
                    report_start,
                    report_end,
                )
                report_start = report_end.add(days=1)

            self._last_window_end = min(report_end, sync_end_date)
            report_start = report_end.add(days=1)

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

        # Context for creative_videos child stream
        creative = record.get("creative", {})
        if creative.get("video_id"):
            child: dict = {**base_context, "video_id": creative["video_id"], "_child_type": "creative_video"}
            # effective_object_story_id format: "{page_id}_{post_id}"
            story_id = creative.get("effective_object_story_id", "")
            if "_" in story_id:
                child["page_id"] = story_id.split("_")[0]
            yield child

        # Context(s) for recommendations child stream
        for recommendation in record.get("recommendations", []):
            yield {**base_context, **recommendation, "_child_type": "recommendation"}

        # Tracking specs child
        for spec in record.get("tracking_specs", []):
            yield {**base_context, **spec, "_child_type": "tracking_spec"}
