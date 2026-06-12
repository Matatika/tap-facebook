"""Stream class for Pages."""

from __future__ import annotations

import typing as t

from singer_sdk.typing import (
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream, SkipAccountError

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PagesStream(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/reference/ad-account/promote_pages/."""

    columns = [  # noqa: RUF012
        "id",
        "name",
        "category",
        "fan_count",
        "followers_count",
        "link",
    ]

    name = "pages"
    path = "promote_pages"
    tap_stream_id = "pages"
    primary_keys = ["id"]  # noqa: RUF012

    schema = PropertiesList(
        Property("id", StringType),
        Property("name", StringType),
        Property("category", StringType),
        Property("fan_count", IntegerType),
        Property("followers_count", IntegerType),
        Property("link", StringType),
        Property("account_id", StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/promote_pages"

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        params: dict = {
            "fields": ",".join(self.columns),
            "limit": self.config["limit"],
        }
        if next_page_token is not None:
            params["after"] = next_page_token
        return params

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        account_id = context["_current_account_id"]
        try:
            for record in super().get_records(context):
                record["account_id"] = account_id
                yield record
        except SkipAccountError as e:
            self.logger.warning("Account %s skipped: %s", account_id, e)

    def generate_child_contexts(
        self,
        record: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> t.Iterable[dict]:
        yield {"page_id": record["id"]}
