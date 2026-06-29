"""Stream class for CreativeVideoStream."""

from __future__ import annotations

import typing as t

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.typing import (
    DateTimeType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import FacebookStream
from tap_facebook.streams.ads import AdsStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class CreativeVideoStream(FacebookStream):
    """Video details fetched per unique video surfaced by ads."""

    columns = [  # noqa: RUF012
        "id",
        "title",
        "description",
        "created_time",
        "updated_time",
        "permalink_url",
        "embed_html",
        "source",
        "length",
    ]

    name = "creative_videos"
    path = ""
    tap_stream_id = "creative_videos"
    parent_stream_type = AdsStream
    state_partitioning_keys: t.ClassVar[list[str]] = []
    primary_keys: t.ClassVar[list[str]] = ["id"]

    schema = PropertiesList(
        Property("id", StringType),
        Property("title", StringType),
        Property("description", StringType),
        Property("created_time", DateTimeType),
        Property("updated_time", DateTimeType),
        Property("permalink_url", StringType),
        Property("embed_html", StringType),
        Property("source", StringType),
        Property("length", NumberType),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self._page_token_cache: dict[str, str] = {}
        self._current_page_id: str | None = None
        self._me_accounts_fetched = False
        self._seen_video_ids: set[str] = set()

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        if self._current_page_id and self._current_page_id in self._page_token_cache:
            return BearerTokenAuthenticator.create_for_stream(
                self,
                token=self._page_token_cache[self._current_page_id],
            )
        return super().authenticator

    def _fetch_all_page_tokens(self) -> None:
        """Populate page token cache from /me/accounts.

        Works when the system user has been granted a role on the page in Business Manager.
        One call fetches tokens for all accessible pages at once.
        """
        version = self.config["api_version"]
        url = f"https://graph.facebook.com/{version}/me/accounts"
        params: dict = {
            "fields": "id,access_token",
            "limit": 200,
            "access_token": self.config["access_token"],
        }
        while url:
            response = self.requests_session.get(url, auth=False, params=params)
            if not response.ok:
                self.logger.warning(
                    "Failed to fetch page tokens from /me/accounts: %s", response.content
                )
                return
            data = response.json()
            for page in data.get("data", []):
                if page.get("access_token"):
                    self._page_token_cache[page["id"]] = page["access_token"]
            next_url = data.get("paging", {}).get("next")
            url = next_url  # type: ignore[assignment]
            params = {}

    def _fetch_page_token(self, page_id: str) -> str | None:
        """Fetch a page access token directly from the page node.

        Requires the system user to have admin/editor access to the page.
        """
        version = self.config["api_version"]
        response = self.requests_session.get(
            f"https://graph.facebook.com/{version}/{page_id}",
            auth=False,  # bypass session.auth so access_token query param is the only credential
            params={
                "fields": "access_token",
                "access_token": self.config["access_token"],
            },
        )
        if response.ok:
            return response.json().get("access_token")
        self.logger.warning("Failed to fetch page token for page %s: %s", page_id, response.content)
        return None

    def get_url(self, context: dict | None) -> str:
        version = self.config["api_version"]
        video_id = context["video_id"] if context else ""
        return f"https://graph.facebook.com/{version}/{video_id}"

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401, ARG002
    ) -> dict[str, t.Any]:
        # auth is handled via the authenticator property (Bearer header), not as a query param
        return {"fields": ",".join(self.columns)}

    def parse_response(self, response: requests.Response) -> t.Iterator[dict]:
        data = response.json()
        if isinstance(data, dict) and "id" in data:
            yield data

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if not context or context.get("_child_type") != "creative_video":
            return
        video_id = context["video_id"]
        if video_id in self._seen_video_ids:
            return
        self._seen_video_ids.add(video_id)
        page_id = context.get("page_id")
        if page_id:
            self._current_page_id = page_id
            if page_id not in self._page_token_cache:
                # Try /me/accounts first — one call populates tokens for all accessible pages
                if not self._me_accounts_fetched:
                    self._fetch_all_page_tokens()
                    self._me_accounts_fetched = True
                # Fall back to per-page fetch if still not found
                if page_id not in self._page_token_cache:
                    token = self._fetch_page_token(page_id)
                    if token:
                        self._page_token_cache[page_id] = token
                    else:
                        self.logger.warning(
                            "No page token for page %s — system user may not have page admin "
                            "access. Falling back to system user token (source field may be empty).",
                            page_id,
                        )
        try:
            yield from super().get_records(context)
        except FatalAPIError as e:
            self.logger.warning("Skipping video %s: %s", context.get("video_id"), e)
