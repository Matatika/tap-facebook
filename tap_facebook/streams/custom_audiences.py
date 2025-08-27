"""Stream classes for CustomAudiences."""

from __future__ import annotations

import typing as t

from singer_sdk.typing import (
    BooleanType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream
from tap_facebook.streams.adsets import AdsetsStream

if t.TYPE_CHECKING:
    import requests


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
    parent_stream_type = AdsetsStream
    state_partitioning_keys: t.ClassVar[list] = []

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
        context: dict | None,  # noqa: ARG002
        next_page_token: str | None = None,
    ) -> dict[str, str]:
        """Return query parameters for the request."""
        params = {
            "fields": ",".join(self.columns),
        }
        if next_page_token:
            params["after"] = next_page_token
        return params

    def parse_response(self, response: requests.Response) -> list:
        data = response.json()
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data
        return []

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        return f"https://graph.facebook.com/{version}/{context['custom_audience_id']}"
