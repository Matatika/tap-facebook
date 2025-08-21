"""Stream class for AdLabels."""
from __future__ import annotations

import typing as t

from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream


class AdLabelsStream(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/ad-creative/."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    account_id: facebook account
    path: path which will be added to api url in client.py
    schema: instream schema
    tap_stream_id = stream id
    """

    columns = ["id", "account", "created_time", "updated_time", "name"]  # noqa: RUF012

    name = "adlabels"
    path = "adlabels"
    primary_keys = ["id", "updated_time"]  # noqa: RUF012
    tap_stream_id = "adlabels"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "updated_time"

    schema = PropertiesList(
        Property("id", StringType),
        Property(
            "account",
            ObjectType(
                Property("account_id", StringType),
                Property("id", StringType),
            ),
        ),
        Property("created_time", StringType),
        Property("updated_time", StringType),
        Property("name", StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/adlabels?fields={self.columns}"

