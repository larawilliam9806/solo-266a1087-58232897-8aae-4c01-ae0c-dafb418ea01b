"""Completion reporting, daily rates, incremental maintenance and migration."""

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.db import connect, init_db
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(tmp_path / "test.sqlite3")
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


def post_exposure(client, exposure_id, session_id, occurred_at, group):
    return client.post(
        "/exposures",
        json={
            "exposure_id": exposure_id,
            "session_id": session_id,
            "occurred_at": occurred_at,
            "variant": group,
        },
    )


def post_completion(client, completion_id, session_id, occurred_at):
    return client.post(
        "/completions",
        json={
            "completion_id": completion_id,
            "session_id": session_id,
            "occurred_at": occurred_at,
        },
    )


def daily(client, **params):
    response = client.get("/analysis/daily", params=params)
    assert response.status_code == 200
    return response.json()["daily"]


def test_completion_can_arrive_before_binding(client):
    # Completion first, then exposure, then binding.
    created = post_completion(client, "C1", "S1", "2026-01-01T01:00:00Z")
    assert created.status_code == 201
    assert created.json()["status"] == "created"
    assert created.json()["completion_no"] == "C1"

    # Not bound yet: no sample, no daily row.
    assert client.get("/analysis").json()["samples"] == []
    assert daily(client) == []

    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})

    rows = daily(client, day="2026-01-01", group="A")
    assert rows == [
        {
            "exposure_day": "2026-01-01",
            "group": "A",
            "variant": "A",
            "sample_count": 1,
            "completer_count": 1,
            "completion_rate": 1.0,
        }
    ]


def test_completion_replay_must_match_exactly(client):
    payload = {
        "completion_id": "C1",
        "session_id": "S1",
        "occurred_at": "2026-01-01T01:00:00Z",
    }
    first = client.post("/completions", json=payload)
    assert first.status_code == 201
    replay = client.post("/completions", json=payload)
    assert replay.status_code == 200
    assert replay.json()["status"] == "existing"

    conflicting_time = client.post(
        "/completions", json={**payload, "occurred_at": "2026-01-01T02:00:00Z"}
    )
    assert conflicting_time.status_code == 409
    assert conflicting_time.json()["detail"]["error"] == "completion_conflict"

    conflicting_session = client.post(
        "/completions", json={**payload, "session_id": "S9"}
    )
    assert conflicting_session.status_code == 409

    assert client.get("/completions").json()["count"] == 1


def test_completion_window_includes_start_excludes_end(client):
    # First exposure at 00:00:00Z.
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    post_exposure(client, "E2", "S2", "2026-01-02T00:00:00Z", "A")
    post_exposure(client, "E3", "S3", "2026-01-03T00:00:00Z", "A")
    client.post(
        "/bindings",
        json=[
            {"session_id": "S1", "account_id": "U1"},
            {"session_id": "S2", "account_id": "U2"},
            {"session_id": "S3", "account_id": "U3"},
        ],
    )

    # Exactly at the first-exposure instant -> counted.
    post_completion(client, "C1", "S1", "2026-01-01T00:00:00Z")
    # Exactly 24h after -> excluded.
    post_completion(client, "C2", "S2", "2026-01-03T00:00:00Z")
    # Just before the 24h boundary -> counted.
    post_completion(client, "C3", "S3", "2026-01-03T23:59:59.999999Z")

    by_day = {row["exposure_day"]: row for row in daily(client)}
    assert by_day["2026-01-01"]["completer_count"] == 1
    assert by_day["2026-01-02"]["completer_count"] == 0
    assert by_day["2026-01-02"]["completion_rate"] == 0.0
    assert by_day["2026-01-03"]["completer_count"] == 1


def test_day_is_utc_day_of_first_exposure(client):
    # 23:30Z on Jan 1 in UTC, which is Jan 2 in +02:00 local time.
    post_exposure(client, "E1", "S1", "2026-01-01T23:30:00+00:00", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    rows = daily(client)
    assert [row["exposure_day"] for row in rows] == ["2026-01-01"]

    # A timestamp with an offset is normalised to UTC before the day is taken.
    post_exposure(client, "E2", "S2", "2026-01-02T01:30:00+02:00", "B")
    client.post("/bindings", json={"session_id": "S2", "account_id": "U2"})
    rows = daily(client)
    assert {(row["exposure_day"], row["group"]) for row in rows} == {
        ("2026-01-01", "A"),
        ("2026-01-01", "B"),
    }


def test_any_bound_session_counts_and_groups_excluded(client):
    # U1 is bound to S1 and S2; the completion arrives on the later session.
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    post_exposure(client, "E2", "S2", "2026-01-01T00:10:00Z", "A")
    client.post(
        "/bindings",
        json=[
            {"session_id": "S1", "account_id": "U1"},
            {"session_id": "S2", "account_id": "U1"},
        ],
    )
    post_completion(client, "C1", "S2", "2026-01-01T05:00:00Z")
    assert daily(client)[0]["completer_count"] == 1

    # A cross-group account is not a sample, so it contributes nothing.
    post_exposure(client, "E3", "S3", "2026-01-01T00:00:00Z", "B")
    client.post("/bindings", json={"session_id": "S3", "account_id": "U1"})
    rows = daily(client)
    assert rows == []
    assert client.get("/analysis").json()["excluded_accounts"][0]["account_id"] == "U1"


def test_no_sample_means_null_rate(client):
    rows = daily(client, day="2026-01-01", group="A")
    assert len(rows) == 1
    assert rows[0]["sample_count"] == 0
    assert rows[0]["completer_count"] == 0
    assert rows[0]["completion_rate"] is None


def test_late_exposure_moves_account_to_new_day(client):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    post_completion(client, "C1", "S1", "2026-01-01T01:00:00Z")
    assert daily(client)[0]["exposure_day"] == "2026-01-01"

    # Backfilled exposure is earlier: first exposure (and thus day) changes.
    post_exposure(client, "E0", "S1", "2025-12-30T00:00:00Z", "A")
    rows = daily(client)
    assert len(rows) == 1
    assert rows[0]["exposure_day"] == "2025-12-30"
    # Completion is more than 24h after the new first exposure -> not counted.
    assert rows[0]["sample_count"] == 1
    assert rows[0]["completer_count"] == 0


def test_late_completion_also_affects_old_day(client):
    # First exposure on Jan 1, completion arrives after a second exposure moved
    # the account's first exposure to Dec 31; the old Jan row must be gone.
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    post_exposure(client, "E0", "S1", "2025-12-30T00:00:00Z", "A")
    assert [row["exposure_day"] for row in daily(client)] == ["2025-12-30"]

    post_completion(client, "C1", "S1", "2026-01-01T02:00:00Z")
    rows = daily(client)
    assert len(rows) == 1
    assert rows[0]["exposure_day"] == "2025-12-30"
    # 2026-01-01T02:00Z is 50h after 2025-12-30T00:00Z -> outside the window.
    assert rows[0]["completer_count"] == 0
    assert daily(client, day="2026-01-01", group="A")[0]["completion_rate"] is None


def test_rebinding_moves_contribution_between_accounts_and_groups(client):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    post_completion(client, "C1", "S1", "2026-01-01T01:00:00Z")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    assert daily(client) == [
        {
            "exposure_day": "2026-01-01",
            "group": "A",
            "variant": "A",
            "sample_count": 1,
            "completer_count": 1,
            "completion_rate": 1.0,
        }
    ]

    # Moving U1's only session to another account removes U1 entirely.
    client.post("/bindings", json={"session_id": "S1", "account_id": "U2"})
    rows = daily(client)
    assert len(rows) == 1
    assert rows[0]["sample_count"] == 1
    assert rows[0]["completer_count"] == 1

    client.delete("/bindings/S1")
    assert daily(client) == []


def test_rebinding_across_groups_moves_daily_bucket(client):
    # U1 sees A via S1 and B via S2 -> cross-group, no sample.  Unbinding S2
    # turns U1 into an A sample; rebinding S2 to U2 makes U1 A and U2 B.
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    post_exposure(client, "E2", "S2", "2026-01-02T00:00:00Z", "B")
    client.post(
        "/bindings",
        json=[
            {"session_id": "S1", "account_id": "U1"},
            {"session_id": "S2", "account_id": "U1"},
        ],
    )
    assert daily(client) == []

    client.post("/bindings", json={"session_id": "S2", "account_id": "U2"})
    rows = {(row["exposure_day"], row["group"]): row for row in daily(client)}
    assert set(rows) == {("2026-01-01", "A"), ("2026-01-02", "B")}
    assert rows[("2026-01-01", "A")]["sample_count"] == 1
    assert rows[("2026-01-02", "B")]["sample_count"] == 1


def test_rebuild_after_manual_sample_repair_also_rebuilds_daily(client, tmp_path):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    post_completion(client, "C1", "S1", "2026-01-01T01:00:00Z")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    assert daily(client)[0]["completer_count"] == 1

    # Corrupt only the daily table behind the API's back.
    db_file = tmp_path / "test.sqlite3"
    raw = sqlite3.connect(db_file)
    raw.execute("DELETE FROM analysis_daily")
    raw.commit()
    raw.close()
    assert daily(client) == []

    rebuilt = client.post("/analysis")
    assert rebuilt.status_code == 200
    assert daily(client)[0]["completer_count"] == 1


def test_completion_write_failure_rolls_back_everything(client, tmp_path):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:00Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    before_daily = daily(client)
    before_completions = client.get("/completions").json()

    db_file = tmp_path / "test.sqlite3"
    raw = sqlite3.connect(db_file)
    raw.execute(
        "CREATE TRIGGER fail_daily BEFORE INSERT ON analysis_daily "
        "BEGIN SELECT RAISE(ABORT, 'forced daily failure'); END"
    )
    raw.commit()
    raw.close()

    try:
        response = post_completion(client, "C1", "S1", "2026-01-01T01:00:00Z")
        assert response.status_code == 500
        # The completion row, the sample flags and the daily rows all survive.
        assert client.get("/completions").json() == before_completions
        assert daily(client) == before_daily
    finally:
        raw = sqlite3.connect(db_file)
        raw.execute("DROP TRIGGER fail_daily")
        raw.commit()
        raw.close()
        post_completion(client, "C1", "S1", "2026-01-01T01:00:00Z")
        assert daily(client)[0]["completer_count"] == 1


def test_upgrade_from_old_database_backfills_daily(tmp_path):
    """An existing v1 database becomes queryable without a rebuild call."""

    db_file = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE exposures (
            exposure_id TEXT PRIMARY KEY,
            session_id  TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            occurred_us INTEGER NOT NULL,
            variant     TEXT NOT NULL
        );
        CREATE TABLE session_bindings (
            session_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL
        );
        CREATE TABLE analysis_samples (
            account_id       TEXT PRIMARY KEY,
            variant          TEXT NOT NULL,
            first_exposure_id TEXT NOT NULL
        );
        CREATE TABLE analysis_cross_group (
            account_id       TEXT NOT NULL,
            variant          TEXT NOT NULL,
            first_exposure_id TEXT NOT NULL,
            PRIMARY KEY (account_id, variant)
        );
        CREATE TABLE analysis_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        INSERT INTO exposures VALUES
            ('E1', 'S1', '2026-02-01T00:00:00Z', 1769904000000000, 'A');
        INSERT INTO session_bindings VALUES ('S1', 'U1');
        INSERT INTO analysis_samples VALUES ('U1', 'A', 'E1');
        INSERT INTO analysis_meta VALUES ('updated_at', '2026-02-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    init_db(db_file)

    check = connect(db_file)
    try:
        columns = {
            row["name"] for row in check.execute("PRAGMA table_info(analysis_samples)")
        }
        assert "is_completer" in columns
        rows = check.execute(
            "SELECT exposure_day, variant, sample_count, completer_count "
            "FROM analysis_daily"
        ).fetchall()
        assert [tuple(row) for row in rows] == [("2026-02-01", "A", 1, 0)]
    finally:
        check.close()

    app = create_app(db_file)
    with TestClient(app) as upgraded:
        assert upgraded.get("/analysis/daily").json()["daily"][0]["sample_count"] == 1
