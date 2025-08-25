"""Stream class for Creative."""
from __future__ import annotations

import json
import typing as t
from datetime import timedelta

import pendulum
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    BooleanType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream, SkipAccountError

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class CreativeStream(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/ad-creative/."""

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
        "account_id",
        "actor_id",
        "applink_treatment",
        "asset_feed_spec",
        "authorization_category",
        "body",
        "branded_content_sponsor_page_id",
        "bundle_folder_id",
        "call_to_action_type",
        "categorization_criteria",
        "category_media_source",
        "degrees_of_freedom_spec",
        "destination_set_id",
        "dynamic_ad_voice",
        "effective_authorization_category",
        "effective_instagram_media_id",
        "effective_instagram_story_id",
        "effective_object_story_id",
        "enable_direct_install",
        "image_hash",
        "image_url",
        "instagram_actor_id",
        "instagram_permalink_url",
        "instagram_story_id",
        "link_destination_display_url",
        "link_og_id",
        "link_url",
        "messenger_sponsored_message",
        "name",
        "object_id",
        "object_store_url",
        "object_story_id",
        "object_story_spec",
        "object_type",
        "object_url",
        "page_link",
        "page_message",
        "place_page_set_id",
        "platform_customizations",
        "playable_asset_id",
        "source_instagram_media_id",
        "status",
        "template_url",
        "thumbnail_id",
        "thumbnail_url",
        "title",
        "url_tags",
        "use_page_actor_override",
        "video_id",
    ]

    name = "creatives"
    path = "adcreatives"
    tap_stream_id = "creatives"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "id"

    schema = PropertiesList(
        Property("id", StringType),
        Property("account_id", StringType),
        Property("actor_id", StringType),
        Property("applink_treatment", StringType),
        Property(
            "asset_feed_spec",
            ObjectType(),
        ),
        Property("authorization_category", StringType),
        Property("body", StringType),
        Property("branded_content_sponsor_page_id", StringType),
        Property("bundle_folder_id", StringType),
        Property("call_to_action_type", StringType),
        Property("categorization_criteria", StringType),
        Property("category_media_source", StringType),
        Property("degrees_of_freedom_spec", ObjectType()),
        Property("destination_set_id", StringType),
        Property("dynamic_ad_voice", StringType),
        Property("effective_authorization_category", StringType),
        Property("effective_instagram_media_id", StringType),
        Property("effective_object_story_id", StringType),
        Property("enable_direct_install", BooleanType),
        Property("image_hash", StringType),
        Property("image_url", StringType),
        Property("instagram_actor_id", StringType),
        Property("instagram_permalink_url", StringType),
        Property("instagram_story_id", StringType),
        Property("link_destination_display_url", StringType),
        Property("link_og_id", StringType),
        Property("link_url", StringType),
        Property("messenger_sponsored_message", StringType),
        Property("name", StringType),
        Property("object_id", StringType),
        Property("object_store_url", StringType),
        Property("object_story_id", StringType),
        Property("object_story_spec", ObjectType()),
        Property("object_type", StringType),
        Property("object_url", StringType),
        Property("page_link", StringType),
        Property("page_message", StringType),
        Property("place_page_set_id", IntegerType),
        Property("platform_customizations", StringType),
        Property("playable_asset_id", IntegerType),
        Property("source_instagram_media_id", StringType),
        Property("status", StringType),
        Property("template_url", StringType),
        Property("thumbnail_id", StringType),
        Property("thumbnail_url", StringType),
        Property("title", StringType),
        Property("url_tags", StringType),
        Property("use_page_actor_override", BooleanType),
        Property("video_id", StringType),
        Property(
            "template_url_spec",
            ObjectType(
                Property(
                    "android",
                    ObjectType(
                        Property("app_name", StringType),
                        Property("package", StringType),
                        Property("url", StringType),
                    ),
                ),
                Property(
                    "config",
                    ObjectType(
                        Property("app_id", StringType),
                    ),
                ),
                Property(
                    "ios",
                    ObjectType(
                        Property("app_name", StringType),
                        Property("app_store_id", StringType),
                        Property("url", StringType),
                    ),
                ),
                Property(
                    "ipad",
                    ObjectType(
                        Property("app_name", StringType),
                        Property("app_store_id", StringType),
                        Property("url", StringType),
                    ),
                ),
                Property(
                    "iphone",
                    ObjectType(
                        Property("app_name", StringType),
                        Property("app_store_id", StringType),
                        Property("url", StringType),
                    ),
                ),
                Property(
                    "web",
                    ObjectType(
                        Property("should_fallback", StringType),
                        Property("url", StringType),
                    ),
                ),
                Property(
                    "windows_phone",
                    ObjectType(
                        Property("app_id", StringType),
                        Property("app_name", StringType),
                        Property("url", StringType),
                    ),
                ),
            ),
        ),
        Property("product_set_id", StringType),
        Property("carousel_ad_link", StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/adcreatives?fields={self.columns}"

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
