"""Stream class for AdCreatives."""
from __future__ import annotations

import typing as t

from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.streams.core import REPLICATION_FULL_TABLE
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
    import requests


class AdCreativesStream(FacebookStream):
    """https://developers.facebook.com/docs/marketing-api/creative/."""

    columns = [  # noqa: RUF012
        "id",
        "account_id",
        "call_to_action_type",
        "image_hash",
        "image_url",
        "instagram_permalink_url",
        "link_og_id",
        "link_url",
        "name",
        "object_id",
        "object_story_id",
        "status",
        "template_url",
        "thumbnail_url",
        "title",
        "video_id",
        "body",
    ]
    columns_config_key = "ad_creatives_fields"
    field_chunk_size_config_key = "ad_creatives_field_chunk_size"

    name = "adcreatives"
    path = "adcreatives"
    tap_stream_id = "adcreatives"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_method = REPLICATION_FULL_TABLE
    state_partitioning_keys: t.ClassVar[list] = []

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

    @property
    def _requested_fields(self) -> list[str]:
        """Return configured field list or fall back to defaults."""
        fields = self.config.get(self.columns_config_key)
        if isinstance(fields, str):
            fields = [f.strip() for f in fields.split(",") if f.strip()]
        if isinstance(fields, list) and fields:
            return [f for f in fields if f]
        return self.columns

    @property
    def _field_chunks(self) -> list[list[str]]:
        """Split fields into fixed-size groups; keep heavy fields isolated."""
        chunk_size = 5
        heavy_fields = {"body"}
        chunks: list[list[str]] = []
        current: list[str] = ["id"]

        for field in self._requested_fields:
            if field == "id":
                continue

            if field in heavy_fields:
                if len(current) > 1:
                    chunks.append(current)
                    current = ["id"]
                chunks.append(["id", field])
                continue

            if len(current) >= chunk_size:
                chunks.append(current)
                current = ["id"]

            current.append(field)

        if len(current) > 1:
            chunks.append(current)

        return chunks or [["id"]]

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        account_id = context["_current_account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}/adcreatives"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: str | None = None,
    ) -> dict[str, str]:
        """Return query parameters for the request."""
        params: dict[str, str] = {
            # _current_fields is set by get_records when chunking fields.
            "fields": ",".join(getattr(self, "_current_fields", self._requested_fields)),
            "limit": str(getattr(self, "_current_limit", 1000)),
        }
        self.logger.info(
            "Requesting adcreatives with fields=%s, limit=%s",
            params["fields"],
            params["limit"],
        )
        if next_page_token:
            params["after"] = next_page_token
        return params

    def parse_response(self, response: requests.Response) -> list:
        data = response.json()
        if isinstance(data, dict):
            return data.get("data", []) or [data]
        if isinstance(data, list):
            return data
        return []

    def backoff_handler(self, details: dict) -> None:  # type: ignore[override]
        """On backoff, drop the limit stepwise (1000->500->300->100) and retry."""
        current_limit = getattr(self, "_current_limit", 1000)
        current_fields = getattr(self, "_current_fields", self._requested_fields)
        if current_limit > 500:  # noqa: PLR2004
            self.logger.warning(
                "Backoff after %s tries on adcreatives fields %s with limit %s; "
                "reducing limit to 500 for retry.",
                details.get("tries"),
                ",".join(current_fields) if isinstance(current_fields, list) else current_fields,
                current_limit,
            )
            self._current_limit = 500
        elif current_limit > 300: # noqa: PLR2004
            self.logger.warning(
                "Backoff after %s tries on adcreatives fields %s with limit %s; "
                "reducing limit to 300 for retry.",
                details.get("tries"),
                ",".join(current_fields) if isinstance(current_fields, list) else current_fields,
                current_limit,
            )
            self._current_limit = 300
        elif current_limit > 100: # noqa: PLR2004
            self.logger.warning(
                "Backoff after %s tries on adcreatives fields %s with limit %s; "
                "reducing limit to 100 for retry.",
                details.get("tries"),
                ",".join(current_fields) if isinstance(current_fields, list) else current_fields,
                current_limit,
            )
            self._current_limit = 100

        super().backoff_handler(details)

    def backoff_max_tries(self) -> int:  # type: ignore[override]
        """Fail fast so the outer retry loop can rebuild the request with a lower limit."""
        return 1

    def backoff_wait_generator(self) -> t.Generator[float, None, None]:  # type: ignore[override]
        """Disable internal retry backoff; let outer loop handle retries."""
        while True:
            yield 0

    def get_records(self, context: dict | None) -> t.Iterable[dict]:  # noqa: C901
        """Request all fields by chunking the field list across multiple passes."""
        account_id = context["_current_account_id"]
        merged_records: dict[str, dict] = {}
        default_limit = 1000
        self._current_limit = default_limit

        for field_chunk in self._field_chunks:
            # Reset the limit for each field chunk so retries on one chunk
            # do not force subsequent chunks to start with a reduced limit.
            self._current_limit = default_limit
            while True:
                self._current_fields = field_chunk
                try:
                    for record in super().get_records(context):
                        if not isinstance(record, dict):
                            continue
                        record_id = record.get("id")
                        if not record_id:
                            continue
                        merged = merged_records.setdefault(record_id, {"id": record_id})
                        merged.update(record)
                        merged.setdefault("account_id", account_id)
                    break
                except RetriableAPIError as e:
                    if getattr(self, "_current_limit", 1000) > 500:  # noqa: PLR2004
                        self.logger.warning(
                            "Retriable error on adcreatives fields %s with limit %s; "
                            "reducing limit to 500 and retrying. Error: %s",
                            ",".join(field_chunk),
                            self._current_limit,
                            e,
                        )
                        self._current_limit = 500
                        continue
                    if getattr(self, "_current_limit", 1000) > 300: # noqa: PLR2004
                        self.logger.warning(
                            "Retriable error on adcreatives fields %s with limit %s; "
                            "reducing limit to 300 and retrying. Error: %s",
                            ",".join(field_chunk),
                            self._current_limit,
                            e,
                        )
                        self._current_limit = 300
                        continue
                    if getattr(self, "_current_limit", 1000) > 100: # noqa: PLR2004
                        self.logger.warning(
                            "Retriable error on adcreatives fields %s with limit %s; "
                            "reducing limit to 100 and retrying. Error: %s",
                            ",".join(field_chunk),
                            self._current_limit,
                            e,
                        )
                        self._current_limit = 100
                        continue
                    raise
                except SkipAccountError as e:
                    self.logger.warning(
                        "Account %s skipped due to server error while fetching fields %s: %s",
                        account_id,
                        ",".join(field_chunk),
                        e,
                    )
                    return  # stops this account, continues next partition
                finally:
                    self._current_fields = None

        yield from merged_records.values()
