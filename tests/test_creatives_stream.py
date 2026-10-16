"""Tests for the `creatives` stream's handling of creatives Facebook refuses to return."""

from __future__ import annotations

from singer_sdk.exceptions import FatalAPIError

from tap_facebook.streams.creative import DOF_MISMATCH_FIELD, CreativeStream
from tap_facebook.tap import TapFacebook

BASE_CONFIG = {
    "access_token": "token",
    "account_ids": ["123"],
    "api_version": "v23.0",
    "start_date": "2024-01-01T00:00:00Z",
}

DOF_ERROR = (
    "400 Client Error: b'{\"error\":{\"message\":\"Invalid parameter\",\"code\":100,"
    "\"error_subcode\":2446211}}' (Reason: Bad Request) for path: /v23.0/1111"
)
OTHER_ERROR = (
    "400 Client Error: b'{\"error\":{\"message\":\"Unsupported get request\",\"code\":100,"
    "\"error_subcode\":33}}' (Reason: Bad Request) for path: /v23.0/1111"
)


def build_stream():
    tap = TapFacebook(config=BASE_CONFIG, parse_env_config=False)
    return CreativeStream(tap=tap)


def creative_context(creative_id="1111"):
    return {"creative_id": creative_id, "_child_type": "creative", "_current_account_id": "123"}


def requested_fields(stream):
    return set(stream.get_url_params(None)["fields"].split(","))


def test_object_story_spec_requested_by_default():
    stream = build_stream()

    assert DOF_MISMATCH_FIELD in requested_fields(stream)


def test_get_records_retries_without_object_story_spec_on_dof_mismatch(monkeypatch):
    stream = build_stream()
    calls = []

    def fake_get_records(self, context):
        fields = requested_fields(self)
        calls.append(fields)
        if DOF_MISMATCH_FIELD in fields:
            raise FatalAPIError(DOF_ERROR)
        yield {"id": context["creative_id"]}

    monkeypatch.setattr("tap_facebook.client.FacebookStream.get_records", fake_get_records)

    assert list(stream.get_records(creative_context())) == [{"id": "1111"}]
    assert len(calls) == 2
    assert DOF_MISMATCH_FIELD in calls[0]
    # only object_story_spec is dropped; the other spec fields are still synced
    assert calls[1] == calls[0] - {DOF_MISMATCH_FIELD}
    assert {"asset_feed_spec", "degrees_of_freedom_spec"} <= calls[1]
    # the next creative goes back to requesting every field
    assert DOF_MISMATCH_FIELD in requested_fields(stream)


def test_get_records_skips_creative_when_retry_also_fails(monkeypatch):
    stream = build_stream()

    def fake_get_records(self, context):  # noqa: ARG001
        raise FatalAPIError(DOF_ERROR)
        yield

    monkeypatch.setattr("tap_facebook.client.FacebookStream.get_records", fake_get_records)

    assert list(stream.get_records(creative_context())) == []
    assert DOF_MISMATCH_FIELD in requested_fields(stream)


def test_get_records_skips_creative_on_other_client_errors(monkeypatch):
    stream = build_stream()
    calls = []

    def fake_get_records(self, context):  # noqa: ARG001
        calls.append(context["creative_id"])
        raise FatalAPIError(OTHER_ERROR)
        yield

    monkeypatch.setattr("tap_facebook.client.FacebookStream.get_records", fake_get_records)

    assert list(stream.get_records(creative_context())) == []
    assert calls == ["1111"]  # no retry for unrelated errors
