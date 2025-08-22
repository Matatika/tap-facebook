"""Stream class for Adsets."""

from __future__ import annotations

import json
import typing as t

import pendulum
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import IncrementalFacebookStream, SkipAccountError

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class AdsetsStream(IncrementalFacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/."""

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
        "campaign_id",
        "updated_time",
        "created_time",
        "start_time",
        "end_time",
        "name",
        "effective_status",
        "daily_budget",
        "budget_remaining",
        "lifetime_budget",
        "configured_status",
        "promoted_object",
        "attribution_spec",
        "billing_event",
        "campaign_attribution",
        "destination_type",
        "is_dynamic_creative",
        "learning_stage_info",
        "lifetime_imps",
        "multi_optimization_goal_weight",
        "optimization_goal",
        "optimization_sub_event",
        "pacing_type",
        "recurring_budget_semantics",
        "source_adset_id",
        "status",
        "bid_amount",
        "bid_strategy",
        "targeting",
        "bid_info",
        "instagram_actor_id",
    ]

    columns_remaining = [  # noqa: RUF012
        "adlabels",
        "adset_schedule",
        "asset_feed_id",
        "attribution_spec",
        "bid_adjustments",
        "bid_amount",
        "bid_constraints",
        "bid_info",
        "bid_strategy",
        "contextual_bundling_spec",
        "creative_sequence",
        "daily_min_spend_target",
        "spend_cap",
        "frequency_control_specs",
        "issues_info",
        "lifetime_min_spend_target",
        "lifetime_spend_cap",
        "recommendations",
        "review_feedback",
        "rf_prediction_id",
        "time_based_ad_rotation_id_blocks",
        "time_based_ad_rotation_intervals",
    ]

    name = "adsets"
    filter_entity = "adset"

    path = "adsets"
    primary_keys = ["id", "updated_time"]  # noqa: RUF012
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "updated_time"

    schema = PropertiesList(
        Property("name", StringType),
        Property("end_time", StringType),
        Property("billing_event", StringType),
        Property("campaign_attribution", StringType),
        Property("destination_type", StringType),
        Property("is_dynamic_creative", BooleanType),
        Property("lifetime_imps", IntegerType),
        Property("multi_optimization_goal_weight", StringType),
        Property("optimization_goal", StringType),
        Property("optimization_sub_event", StringType),
        Property("pacing_type", ArrayType(StringType)),
        Property("recurring_budget_semantics", BooleanType),
        Property("source_adset_id", StringType),
        Property("status", StringType),
        Property("targeting_optimization_types", StringType),
        Property("use_new_app_click", BooleanType),
        Property(
            "promoted_object",
            ObjectType(
                Property("custom_event_type", StringType),
                Property("pixel_id", StringType),
                Property("pixel_rule", StringType),
                Property("page_id", StringType),
                Property("object_store_url", StringType),
                Property("application_id", StringType),
                Property("product_set_id", StringType),
                Property("offer_id", StringType),
                Property("custom_conversion_id", IntegerType),
                Property("custom_event_str", StringType),
                Property("event_id", IntegerType),
                Property("offline_conversion_data_set_id", IntegerType),
                Property("pixel_aggregation_rule", StringType),
                Property("place_page_set_id", IntegerType),
                Property("product_catalog_id", IntegerType),
                Property("retention_days", StringType),
                Property("application_type", StringType),
            ),
        ),
        Property("id", StringType),
        Property("account_id", StringType),
        Property("updated_time", DateTimeType),
        Property("daily_budget", StringType),
        Property("budget_remaining", StringType),
        Property("effective_status", StringType),
        Property("campaign_id", StringType),
        Property("created_time", StringType),
        Property("start_time", StringType),
        Property("lifetime_budget", StringType),
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
        Property(
            "adlabels",
            ArrayType(
                Property(
                    "items",
                    ObjectType(
                        Property("id", StringType),
                        Property("name", StringType),
                        Property("created_time", DateTimeType),
                    ),
                ),
            ),
        ),
        Property(
            "attribution_spec",
            ArrayType(
                ObjectType(
                    Property("event_type", StringType),
                    Property("window_days", IntegerType),
                ),
            ),
        ),
        Property(
            "learning_stage_info",
            ObjectType(
                Property("attribution_windows", ArrayType(StringType)),
                Property("conversions", IntegerType),
                Property("last_sig_edit_ts", IntegerType),
                Property("status", StringType),
            ),
        ),
        Property("configured_status", StringType),
        Property("asset_feed_id", StringType),
        Property("daily_min_spend_target", StringType),
        Property("daily_spend_cap", StringType),
        Property("instagram_actor_id", StringType),
        Property("review_feedback", StringType),
        Property("rf_prediction_id", StringType),
        Property("bid_amount", IntegerType),
        Property("bid_strategy", StringType),
        Property(
            "targeting",
            ObjectType(
                Property("age_max", IntegerType),
                Property("age_min", IntegerType),
                Property(
                    "custom_audiences",
                    ArrayType(
                        ObjectType(
                            Property("id", StringType),
                            Property("name", StringType),
                        ),
                    ),
                ),
                Property(
                    "excluded_custom_audiences",
                    ArrayType(
                        ObjectType(
                            Property("id", StringType),
                            Property("name", StringType),
                        ),
                    ),
                ),
                Property(
                    "geo_locations",
                    ObjectType(
                        Property("countries", ArrayType(StringType)),
                        Property("location_types", ArrayType(StringType)),
                    ),
                ),
                Property("genders", ArrayType(IntegerType)),
                Property("brand_safety_content_filter_levels", ArrayType(StringType)),
                Property("publisher_platforms", ArrayType(StringType)),
                Property("facebook_positions", ArrayType(StringType)),
                Property("instagram_positions", ArrayType(StringType)),
                Property("device_platforms", ArrayType(StringType)),
                Property("app_install_state", StringType),
                Property("audience_network_positions", ArrayType(StringType)),
                Property("behaviors", ArrayType(ObjectType())),
                Property("college_years", ArrayType(StringType)),
                Property("connections", ArrayType(ObjectType())),
                Property("education_majors", ArrayType(ObjectType())),
                Property("education_schools", ArrayType(StringType)),
                Property("education_statuses", ArrayType(StringType)),
                Property(
                    "effective_audience_network_positions",
                    ArrayType(StringType),
                ),
                Property("excluded_connections", ArrayType(ObjectType())),
                Property(
                    "excluded_geo_locations",
                    ObjectType(
                        Property("countries", ArrayType(StringType)),
                        Property(
                            "country_groups",
                            ArrayType(StringType),
                        ),
                        Property(
                            "custom_locations",
                            ArrayType(ObjectType()),
                        ),
                        Property(
                            "electoral_district",
                            ArrayType(StringType),
                        ),
                        Property("geo_markets", ArrayType(ObjectType())),
                        Property(
                            "location_types",
                            ArrayType(StringType),
                        ),
                        Property("places", ArrayType(StringType)),
                        Property("regions", ArrayType(ObjectType())),
                        Property("cities", ArrayType(ObjectType())),
                        Property("zips", ArrayType(ObjectType())),
                    ),
                ),
                Property("excluded_publisher_categories", ArrayType(StringType)),
                Property("excluded_publisher_list_ids", ArrayType(StringType)),
                Property("excluded_user_device", ArrayType(StringType)),
                Property("exclusions", ArrayType(ObjectType())),
                Property("family_statuses", ArrayType(ObjectType())),
                Property("flexible_spec", ArrayType(ObjectType())),
                Property("friends_of_connections", ArrayType(ObjectType())),
                Property(
                    "geo_locations",
                    ObjectType(
                        Property("cities", ArrayType(ObjectType())),
                        Property("country_groups", ArrayType(StringType)),
                        Property("custom_locations", ArrayType(StringType)),
                        Property("electoral_district", ArrayType(StringType)),
                        Property("geo_markets", ArrayType(ObjectType())),
                        Property("places", ArrayType(StringType)),
                        Property("regions", ArrayType(ObjectType())),
                        Property("zips", ArrayType(ObjectType())),
                    ),
                ),
                Property("income", ArrayType(ObjectType())),
                Property("industries", ArrayType(ObjectType())),
                Property("interests", ArrayType(StringType)),
                Property("life_events", ArrayType(ObjectType())),
                Property("locales", ArrayType(IntegerType)),
                Property("relationship_statuses", ArrayType(StringType)),
                Property("user_adclusters", ArrayType(ObjectType())),
                Property("user_device", ArrayType(StringType)),
                Property("user_os", ArrayType(StringType)),
                Property("wireless_carrier", ArrayType(StringType)),
                Property("work_employers", ArrayType(ObjectType())),
                Property("work_positions", ArrayType(ObjectType())),
            ),
        ),
        Property("lifetime_min_spend_target", StringType),
        Property("lifetime_spend_cap", StringType),
    ).to_dict()

    tap_stream_id = "adsets"

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/adsets?fields={self.columns}"

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
        params: dict = {"limit": 100}
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
        """Yield Ads records in smaller date chunks using since/until (7 days by default)."""
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
                "Fetching Adsets for account %s from %s to %s",
                chunk_context["_current_account_id"],
                chunk_context["_since"],
                chunk_context["_until"],
            )

            try:
                yield from super().get_records(chunk_context)
            except SkipAccountError as e:
                self.logger.warning("Account %s skipped due to server error: %s", account_id, e)
                return  # stops this account, continues next partition

            # bump the window forward
            report_start = report_end.add(days=1)
            report_end = report_start.add(days=time_increment)

