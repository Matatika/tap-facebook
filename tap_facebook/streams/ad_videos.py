"""Stream class for AdVideos."""

from __future__ import annotations

import json
import typing as t
from datetime import timedelta

import pendulum
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

from tap_facebook.client import FacebookStream, SkipAccountError

if t.TYPE_CHECKING:
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

    def generate_child_contexts(
        self,
        record: dict,
        context: dict | None = None,
    ) -> t.Iterable[dict]:
        """Generate child contexts from a given record.

        Args:
            record: The parent record.
            context: The parent context (unused here but required by SDK).

        Yields:
            dict: A child context for downstream streams.
        """
        yield {
        "video_id": record["id"],
        "_current_account_id": context.get("_current_account_id") if context else None,
    }

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/advideos?fields={self.columns}"

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 50}
        if context and "_since" in context and "_until" in context:
            params["time_range"] = json.dumps({
                "since": context["_since"],
                "until": context["_until"],
            })
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def _get_start_date(
        self,
        context: Context | None,
    ) -> pendulum.Date:

        config_start_date = pendulum.parse(self.config["start_date"]).date()  # type: ignore[union-attr]
        incremental_start_date = pendulum.parse(  # type: ignore[union-attr]
            self.get_starting_replication_key_value(context),  # type: ignore[arg-type]
        ).date()

        if config_start_date >= incremental_start_date:
            report_start = config_start_date
        else:
            report_start = incremental_start_date

        # Facebook store metrics maximum of 37 months old. Any time range that
        # older that 37 months from current date would result in 400 Bad request
        # HTTP response.
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
        today = pendulum.today().date()
        oldest_allowed_start_date = today.subtract(months=37)
        if report_start < oldest_allowed_start_date:
            report_start = oldest_allowed_start_date
            self.logger.info(
                "Report start date '%s' is older than 37 months. "
                "Using oldest allowed start date '%s' instead.",
                report_start,
                oldest_allowed_start_date,
            )
        return report_start

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        """Yield records in smaller date chunks using since/until (7 days by default)."""
        time_increment = 7  # fixed at 7-day chunks

        sync_end_date = pendulum.parse(
            self.config.get("end_date", pendulum.today().to_date_string()),
        ).date()

        report_start = self._get_start_date(context)
        report_end = report_start.add(days=time_increment)
        account_id = context["_current_account_id"]
        while report_start <= sync_end_date:
            # Add the current window into the context
            chunk_context = dict(context or {})
            chunk_context["_since"] = report_start.to_date_string()
            chunk_context["_until"] = report_end.to_date_string()

            self.logger.info(
                "Fetching records for account %s from %s to %s",
                chunk_context["_current_account_id"],
                chunk_context["_since"],
                chunk_context["_until"],
            )
            self._last_window_end = report_end
            try:
                yield from super().get_records(chunk_context)
            except SkipAccountError as e:
                self.logger.warning("Account %s skipped due to server error: %s", account_id, e)
                return  # stops this account, continues next partition

            # bump the window forward
            report_start = report_end.add(days=1)
            report_end = report_start.add(days=time_increment)

    def _finalize_state(self, state: dict | None = None) -> dict | None:
        if state is not None:
            state.setdefault("replication_key", self.replication_key)
            # use the window end instead of last record's updated_time
            if hasattr(self, "_last_window_end"):
                state["replication_key_value"] = (
                    self._last_window_end - timedelta(days=1)
                ).isoformat()

        return super()._finalize_state(state)

