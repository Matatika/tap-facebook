"""REST client handling, including facebookStream base class."""

from __future__ import annotations

import abc
import json
import typing as t
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from urllib.parse import urlparse

import pendulum
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class FacebookStream(RESTStream):
    """facebook stream class."""

    # add account id in the url
    # path and fields will be added to this url in streams.pys

    @property
    def url_base(self) -> str:
        version: str = self.config["api_version"]
        return f"https://graph.facebook.com/{version}/"

    records_jsonpath = "$.data[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.paging.cursors.after"  # noqa: S105

    tolerated_http_errors: list[int] = []  # noqa: RUF012

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config["access_token"],
        )

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,  # noqa: ARG002, ANN401
    ) -> t.Any | None:  # noqa: ANN401
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        if not self.next_page_token_jsonpath:
            return response.headers.get("X-Next-Page", None)

        all_matches = extract_jsonpath(
            self.next_page_token_jsonpath,
            response.json(),
        )
        return next(iter(all_matches), None)

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
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
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.
        """
        full_path = urlparse(response.url).path
        if response.status_code in self.tolerated_http_errors:
            msg = (
                f"{response.status_code} Tolerated Status Code "
                f"(Reason: {response.reason}) for path: {full_path}"
            )
            self.logger.info(msg)
            return

        if HTTPStatus.BAD_REQUEST <= response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"
            )
            # Retry on reaching rate limit
            if (
                response.status_code == HTTPStatus.BAD_REQUEST
                and "too many calls" in str(response.content).lower()
            ) or (
                response.status_code in (HTTPStatus.BAD_REQUEST, HTTPStatus.FORBIDDEN)
                and "request limit reached" in str(response.content).lower()
            ):
                raise RetriableAPIError(msg, response)

            if (response.status_code == HTTPStatus.BAD_REQUEST
            and "permission" in str(response.content).lower()):
                raise SkipAccountError(msg)

            raise FatalAPIError(msg)

        if response.status_code > HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"
            )
            raise RetriableAPIError(msg, response)

        if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"
            )
            # If message hints at too much data, retry
            if "please reduce the amount of data" in str(response.content).lower():
                raise RetriableAPIError(msg, response)

            # Otherwise, skip the account
            raise SkipAccountError(msg)

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Setting to None will retry indefinitely.

        Returns:
            int: limit
        """
        return 20

class SkipAccountError(Exception):
    """Raised when an account should be skipped due to a server error."""

class IncrementalFacebookStream(FacebookStream, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def filter_entity(self) -> str:
        """The entity to filter on."""

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
        params: dict = {"limit": 1000}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
            ts = pendulum.parse(self.get_starting_replication_key_value(context))  # type: ignore[arg-type]
            params["filtering"] = json.dumps(
                [
                    {
                        "field": f"{self.filter_entity}.{self.replication_key}",
                        "operator": "GREATER_THAN",
                        "value": int(ts.timestamp()),  # type: ignore[union-attr]
                    },
                ],
            )

        return params

class IncrementalAdsStream(IncrementalFacebookStream):
    """Incremental ads stream class."""

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
        params: dict = {"limit": 25}
        if context and "_since":
            params["updated_since"] = int(datetime.strptime(context["_since"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc).timestamp())
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

        today = pendulum.today("UTC").date()
        report_start = self._get_start_date(context)
        report_end = min(report_start.add(days=time_increment),today)
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
            self._last_window_end = sync_end_date
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
