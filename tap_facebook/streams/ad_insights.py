"""Stream class for AdInsights."""

from __future__ import annotations

import time
import typing as t
from functools import lru_cache

import facebook_business.adobjects.user as fb_user
import pendulum
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsactionstats import AdsActionStats
from facebook_business.adobjects.adshistogramstats import AdsHistogramStats
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from singer_sdk import typing as th
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, Stream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

EXCLUDED_FIELDS = [
    "total_postbacks",
    "adset_end",
    "adset_start",
    "conversion_lead_rate",
    "cost_per_conversion_lead",
    "cost_per_dda_countby_convs",
    "cost_per_one_thousand_ad_impression",
    "cost_per_unique_conversion",
    "creative_media_type",
    "dda_countby_convs",
    "dda_results",
    "instagram_upcoming_event_reminders_set",
    "interactive_component_tap",
    "marketing_messages_cost_per_delivered",
    "marketing_messages_cost_per_link_btn_click",
    "marketing_messages_spend",
    "place_page_name",
    "total_postbacks",
    "total_postbacks_detailed",
    "total_postbacks_detailed_v4",
    "unique_conversions",
    "unique_video_continuous_2_sec_watched_actions",
    "unique_video_view_15_sec",
    "video_thruplay_watched_actions",
    "__module__",
    "__doc__",
    "__dict__",
    "__firstlineno__",  # Python 3.13+
    "__static_attributes__",  # Python 3.13+
    # No longer available >= v19.0: https://developers.facebook.com/docs/marketing-api/marketing-api-changelog/version19.0/
    "age_targeting",
    "gender_targeting",
    "labels",
    "location",
    "estimated_ad_recall_rate_lower_bound",
    "estimated_ad_recall_rate_upper_bound",
    "estimated_ad_recallers_lower_bound",
    "estimated_ad_recallers_upper_bound",
    "marketing_messages_media_view_rate",
    "marketing_messages_phone_call_btn_click_rate",
    "wish_bid",
]

SLEEP_TIME_INCREMENT = 5
INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60
MAX_RETRIES_START = 3
MAX_RETRIES_FINISH = 2
WAIT_BETWEEN_RETRIES = 30  # seconds


class AdsInsightStream(Stream):
    name = "adsinsights"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "date_start"

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the stream."""
        self._report_definition = kwargs.pop("report_definition")
        kwargs["name"] = f"{self.name}_{self._report_definition['name']}"
        super().__init__(*args, **kwargs)

    @property
    def primary_keys(self) -> t.Sequence[str]:
        return ["date_start", "account_id", "ad_id"] + self._report_definition["breakdowns"]

    @primary_keys.setter
    def primary_keys(self, new_value: t.Sequence[str] | None) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    @staticmethod
    def _get_datatype(field: str) -> th.JSONTypeHelper | None:
        d_type = AdsInsights._field_types[field]  # noqa: SLF001
        if d_type == "string":
            return th.StringType()
        if d_type.startswith("list"):
            sub_props: list[th.Property]
            if "AdsActionStats" in d_type:
                sub_props = [
                    th.Property(field.replace("field_", ""), th.StringType())
                    for field in list(AdsActionStats.Field.__dict__)
                    if field not in EXCLUDED_FIELDS
                ]
                return th.ArrayType(th.ObjectType(*sub_props))
            if "AdsHistogramStats" in d_type:
                sub_props = []
                for f in list(AdsHistogramStats.Field.__dict__):
                    if f not in EXCLUDED_FIELDS:
                        clean_field = f.replace("field_", "")
                        if AdsHistogramStats._field_types[clean_field] == "string":  # noqa: SLF001
                            sub_props.append(th.Property(clean_field, th.StringType()))
                        else:
                            sub_props.append(
                                th.Property(
                                    clean_field,
                                    th.ArrayType(th.IntegerType()),
                                ),
                            )
                return th.ArrayType(th.ObjectType(*sub_props))
            return th.ArrayType(th.ObjectType())
        msg = f"Type not found for field: {field}"
        raise RuntimeError(msg)

    @property
    @lru_cache  # noqa: B019
    def schema(self) -> dict:
        properties: list[th.Property] = []
        columns = list(AdsInsights.Field.__dict__)[1:]
        for field in columns:
            if field in EXCLUDED_FIELDS:
                continue
            if data_type := self._get_datatype(field):
                properties.append(th.Property(field, data_type))

        properties.extend(
            [
                th.Property(breakdown, th.StringType())
                for breakdown in self._report_definition["breakdowns"]
            ],
        )

        return th.PropertiesList(*properties).to_dict()

    def _initialize_client(self) -> None:
        FacebookAdsApi.init(
            access_token=self.config["access_token"],
            timeout=300,
            api_version=self.config["api_version"],
        )
        fb_user.User(fbid="me")

    def _run_job_to_completion(self,account: AdAccount, params: dict) -> None:
        job = account.get_insights(params=params, is_async=True)
        status = None
        time_start = time.time()
        retries_start = 0
        retries_finish = 0
        while status != "Job Completed":
            duration = time.time() - time_start
            job = job.api_get()
            status = job[AdReportRun.Field.async_status]
            percent_complete = job[AdReportRun.Field.async_percent_completion]

            job_id = job["id"]
            self.logger.info(
                "%s for %s - %s. %s%% done. ",
                status,
                params["time_range"]["since"],
                params["time_range"]["until"],
                percent_complete,
            )

            if status == "Job Completed":
                return job
            if status == "Job Failed":
                self.logger.error(
                    "Async insights job failed for account %s: %s",
                    account["id"],
                    dict(job),
                )
                break
            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                retries_start += 1
                self.logger.warning(
                "Job %s did not start after %s seconds (account %s, time range %s-%s). Retry %s/%s",
                job_id,
                INSIGHTS_MAX_WAIT_TO_START_SECONDS,
                account["id"],
                params["time_range"]["since"],
                params["time_range"]["until"],
                retries_start,
                MAX_RETRIES_START,
            )
                if retries_start >= MAX_RETRIES_START:
                    self.logger.error("Max retries reached for job start. Skipping this slice.")
                    return None
                time.sleep(WAIT_BETWEEN_RETRIES)
                continue

            if duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS:
                retries_finish += 1
                self.logger.warning(
                (
                    "Job %s did not complete after %s seconds "
                    "(account %s, time range %s-%s). Retry %s/%s"
                ),
                job_id,
                INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS,
                account["id"],
                params["time_range"]["since"],
                params["time_range"]["until"],
                retries_finish,
                MAX_RETRIES_FINISH,
            )
                if retries_finish >= MAX_RETRIES_FINISH:
                    self.logger.error("Max retries reached for job finish. Skipping this slice.")
                    return None
                time.sleep(WAIT_BETWEEN_RETRIES)
                continue

            self.logger.info(
                "Sleeping for %s seconds until job is done",
                SLEEP_TIME_INCREMENT,
            )
            time.sleep(SLEEP_TIME_INCREMENT)

        return None

    def _get_selected_columns(self) -> list[str]:

        fields_to_select = []
        for report in self.config.get("insight_reports_list", []):
            fields_to_select.extend(report.get("fields_to_select", []))

        if self.name != "adsinsights_default":
        # Custom report → force only fields_to_select
        # Reset all selections first
            for data in self.metadata.values():
                data.selected = False

        # Select only configured fields
            for keys, data in self.metadata.items():
                if keys and keys[-1] in fields_to_select:
                    data.selected = True

        columns = [
            keys[1] for keys, data in self.metadata.items() if data.selected and len(keys) > 0
        ]
        if not columns and self.name == "adsinsights_default":
            columns = list(self.schema["properties"])
        return columns

    def _get_start_date(
        self,
        context: Context | None,
    ) -> pendulum.Date:
        lookback_window = self._report_definition["lookback_window"]

        config_start_date = pendulum.parse(self.config["start_date"]).date()  # type: ignore[union-attr]
        incremental_start_date = pendulum.parse(  # type: ignore[union-attr]
            self.get_starting_replication_key_value(context),  # type: ignore[arg-type]
        ).date()
        lookback_start_date = incremental_start_date.subtract(days=lookback_window)

        # Don't use lookback if this is the first sync. Just start where the user requested.
        if config_start_date >= incremental_start_date:
            report_start = config_start_date
            self.logger.info("Using configured start_date as report start filter.")
        else:
            self.logger.info(
                "Incremental sync, applying lookback '%s' to the "
                "bookmark start_date '%s'. Syncing "
                "reports starting on '%s'.",
                lookback_window,
                incremental_start_date,
                lookback_start_date,
            )
            report_start = lookback_start_date

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

    @property
    def partitions(self) -> list[dict[str, t.Any]]:
        return [{"_current_account_id": account_id} for account_id in self.config["account_ids"]]

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        self._initialize_client()

        time_increment = self._report_definition["time_increment_days"]

        sync_end_date = pendulum.parse(  # type: ignore[union-attr]
            self.config.get("end_date", pendulum.today().to_date_string()),
        ).date()

        report_start = self._get_start_date(context)
        report_end = report_start.add(days=time_increment)

        columns = self._get_selected_columns()

        account_id = context["_current_account_id"] if context else None
        if not account_id:
            msg = "No account_id found in partition context."
            raise RuntimeError(msg)

        account = AdAccount(f"act_{account_id}").api_get()
        if not account:
            msg = f"Couldn't find account with id {account_id}"
            raise RuntimeError(msg)

        # --- Pre-check if this account has insights ---
        if not self._account_has_insights(account):
            self.logger.info("Skipping account %s - no insights data.", account_id)
            return  # skip this partition entirely
        while report_start <= sync_end_date:
            params = {
                "level": self._report_definition["level"],
                "action_breakdowns": self._report_definition["action_breakdowns"],
                "action_report_time": self._report_definition["action_report_time"],
                "breakdowns": self._report_definition["breakdowns"],
                "fields": columns,
                "time_increment": time_increment,
                "limit": 1000,
                "action_attribution_windows": [
                    self._report_definition["action_attribution_windows_view"],
                    self._report_definition["action_attribution_windows_click"],
                ],
                "time_range": {
                    "since": report_start.to_date_string(),
                    "until": report_end.to_date_string(),
                },
            }
            job = self._run_job_to_completion(account,params)
            if not job:
                self.logger.warning(
                    (
                        "Skipping records for account %s, "
                        "time range %s-%s due to failed job or retries."
                    ),
                    account["id"],
                    report_start,
                    report_end,
                )
                continue
            else:
                for obj in job.get_result():
                    yield obj.export_all_data()
            # Bump to the next increment
            report_start = report_start.add(days=time_increment)
            report_end = report_end.add(days=time_increment)

    def _account_has_insights(self, account: AdAccount) -> bool:
        params = {
            "level": "account",
            "fields": "account_id",
            "limit": 1,
        }
        response = account.get_insights(params=params)
        for _ in response:
            return True  # Found at least one insight
        return False
