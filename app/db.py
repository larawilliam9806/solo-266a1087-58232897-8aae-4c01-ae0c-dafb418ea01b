"""SQLite persistence for the news-guide A/B experiment service.

The schema deliberately separates raw experiment input (``exposures`` and
``session_bindings``) from the materialised analysis result
(``analysis_samples`` and ``analysis_cross_group``).  Every mutation writes
the raw rows and the affected analysis rows inside one ``BEGIN IMMEDIATE``
transaction, so a failed analysis write can never leave the raw input or the
previous result half-updated.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS exposures (
    exposure_id TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    occurred_us INTEGER NOT NULL,
    variant     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_exposures_session
    ON exposures (session_id);
CREATE INDEX IF NOT EXISTS idx_exposures_variant
    ON exposures (variant);

CREATE TABLE IF NOT EXISTS session_bindings (
    session_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bindings_account
    ON session_bindings (account_id);

-- Accounts that only saw one variant.  One row per account.  ``is_completer``
-- records whether any session currently bound to the account finished reading
-- within 24 hours of the account's first exposure; it makes the daily summary
-- reversible when the account is re-derived.
CREATE TABLE IF NOT EXISTS analysis_samples (
    account_id       TEXT PRIMARY KEY,
    variant          TEXT NOT NULL,
    first_exposure_id TEXT NOT NULL,
    is_completer     INTEGER NOT NULL DEFAULT 0
);

-- Accounts that saw more than one variant ("cross-group" accounts).  They are
-- kept out of the sample and stored here with their first exposure per group
-- so they can be investigated.  One row per account/variant pair.
CREATE TABLE IF NOT EXISTS analysis_cross_group (
    account_id       TEXT NOT NULL,
    variant          TEXT NOT NULL,
    first_exposure_id TEXT NOT NULL,
    PRIMARY KEY (account_id, variant)
);

CREATE TABLE IF NOT EXISTS analysis_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Completion reports ("finished reading").  ``completion_id`` is the reported
-- completion number; a replay must match the stored row exactly.  Reports are
-- accepted before the session is bound and are matched against the binding at
-- derivation time.
CREATE TABLE IF NOT EXISTS completions (
    completion_id TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL,
    occurred_at   TEXT NOT NULL,
    occurred_us   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_completions_session
    ON completions (session_id);

-- Per UTC first-exposure day and group: how many sampled accounts have that
-- day/group as their first exposure and how many of them completed reading.
-- Kept in step with ``analysis_samples`` incrementally.
CREATE TABLE IF NOT EXISTS analysis_daily (
    exposure_day TEXT NOT NULL,
    variant      TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    completer_count INTEGER NOT NULL,
    PRIMARY KEY (exposure_day, variant)
);
"""

SCHEMA_VERSION = 2

COMPLETION_WINDOW_US = 24 * 60 * 60 * 1_000_000

DEFAULT_DATABASE_PATH = Path("data") / "experiment.sqlite3"


def database_path_from_env() -> Path:
    raw = (
        os.environ.get("DATABASE_PATH")
        or os.environ.get("DB_PATH")
        or os.environ.get("SQLITE_PATH")
    )
    return Path(raw) if raw else DEFAULT_DATABASE_PATH


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(path),
        isolation_level=None,  # explicit transactions
        timeout=30.0,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        _migrate(conn)
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """Create the schema and upgrade a database written by an older version.

    ``CREATE TABLE IF NOT EXISTS`` leaves an old ``analysis_samples`` table
    alone, so the new completer flag is added explicitly.  The daily summary is
    then rebuilt from the stored sample, which makes it queryable immediately
    after an upgrade.  Both steps are idempotent: a crash in between simply
    means the next start repeats them.

    A v1 database cannot tell which sampled accounts completed (completion
    reports were not collected yet), so upgraded accounts start as
    non-completers; the first re-derivation of each account fills in the flag
    from whatever reports are stored by then.
    """

    conn.executescript(SCHEMA)

    # Imported lazily: the analysis module is built on top of this module.
    from .experiment import rebuild_daily_summary

    # One writer at a time, so concurrent starts cannot race on the ALTER or
    # the summary rebuild.
    with transaction(conn):
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(analysis_samples)")
        }
        if "is_completer" not in columns:
            conn.execute(
                "ALTER TABLE analysis_samples "
                "ADD COLUMN is_completer INTEGER NOT NULL DEFAULT 0"
            )
        rebuild_daily_summary(conn)
        conn.execute(
            "INSERT INTO analysis_meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(SCHEMA_VERSION),),
        )


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialise writers and roll back cleanly on any failure."""

    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def read_snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Read several tables from one consistent SQLite snapshot."""

    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
