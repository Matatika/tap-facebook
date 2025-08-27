"""Stream classes for CustomAudiences."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone

import pendulum
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    BooleanType,
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


class CustomAudiences(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/custom-audience/."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    account_id: facebook account
    path: path which will be added to api url in client.py
    schema: instream schema
    tap_stream_id = stream id
    """

    name = "customaudiences"
    primary_keys = ["id"]  # noqa: RUF012

    path = "customaudiences"
    replication_key = "time_updated"
    replication_method = REPLICATION_INCREMENTAL

    @property
    def columns(self) -> list[str]:
        return [
            "account_id",
            "id",
            "approximate_count_lower_bound",
            "approximate_count_upper_bound",
            "time_updated",
            "time_created",
            "customer_file_source",
            "data_source",
            "delivery_status",
            "description",
            "lookalike_spec",
            "is_value_based",
            "operation_status",
            "permission_for_actions",
            "pixel_id",
            "retention_days",
            "subtype",
            "rule_aggregation",
            "opt_out_link",
            "name",
            "rule",
        ]

    schema = PropertiesList(
        Property("account_id", StringType),
        Property("id", StringType),
        Property("approximate_count_lower_bound", IntegerType),
        Property("approximate_count_upper_bound", IntegerType),
        Property("time_updated", IntegerType),
        Property("time_created", IntegerType),
        Property("time_content_updated", StringType),
        Property("customer_file_source", StringType),
        Property("data_source", ObjectType()),
        Property(
            "data_source",
            ObjectType(
                Property("type", StringType),
                Property("sub_type", StringType),
                ),
        ),
        Property("delivery_status", ObjectType()),
        Property("description", StringType),
        Property(
            "lookalike_spec",
            ObjectType(
                Property("country", StringType),
                Property("is_financial_service", BooleanType),
                Property("origin_event_name", StringType),
                Property("origin_event_source_name", StringType),
                Property("product_set_name", StringType),
                Property("ratio", NumberType),
                Property("starting_ratio", NumberType),
                Property("type", StringType),
            ),
        ),
        Property("is_value_based", BooleanType),
        Property(
            "operation_status",
            ObjectType(
        Property("code", IntegerType),
        Property("description", StringType),
            ),
        ),
        Property("permission_for_actions", ObjectType()),
        Property("pixel_id", StringType),
        Property("retention_days", IntegerType),
        Property("subtype", StringType),
        Property("rule_aggregation", StringType),
        Property("opt_out_link", StringType),
        Property("name", StringType),
        Property("rule", StringType),
    ).to_dict()

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
        if context and "_since":
            params["updated_since"] = int(datetime.strptime(context["_since"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc).timestamp())
        if next_page_token is not None:
            params["after"] = next_page_token

        return params

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/customaudiences?fields={self.columns}"


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
