"""Stream class for CustomConversions."""

from __future__ import annotations

import typing as t

from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    BooleanType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream


class CustomConversions(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/custom-audience/."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    account_id: facebook account
    path: path which will be added to api url in client.py
    schema: instream schema
    tap_stream_id = stream id
    """

    columns = [  # noqa: RUF012
        "account_id",
        "id",
        "creation_time",
        "name",
        "business",
        "is_archived",
        "is_unavailable",
        "last_fired_time",
    ]

    name = "customconversions"
    path = "customconversions"
    tap_stream_id = "customconversions"
    primary_keys = ["id"]  # noqa: RUF012
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "creation_time"

    schema = PropertiesList(
        Property("account_id", StringType),
        Property("id", StringType),
        Property("name", StringType),
        Property("creation_time", StringType),
        Property("business", ObjectType()),
        Property("is_archived", BooleanType),
        Property("is_unavailable", BooleanType),
        Property("last_fired_time", StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/customconversions?fields={self.columns}"
