"""Core experiment rules.

Two operations matter:

``bind``
    A session may be bound to exactly one account.  Re-binding replaces the
    account and unbinding removes the row.  After a change only the moved
    session and the old/new accounts are re-derived; other accounts' stored
    analysis rows are left untouched.

``analysis``
    Every bound exposure contributes to its account.  An account that saw a
    single variant is a sample row.  An account that saw more than one
    variant is excluded from the sample and listed separately with its first
    exposure in each variant for investigation.  Within a variant the first
    exposure is the earliest UTC timestamp; ties are broken by exposure
    number order (natural sort, so ``E10`` > ``E9``).

``completions`` and the daily summary
    A completion report belongs to a session and may arrive before the session
    is bound.  A sampled account counts as a completer when any session
    currently bound to it has a completion at or after the account's first
    exposure but before 24 hours later.  The per-day/per-group summary is
    derived from the sample table; every re-derivation removes that account's
    old contribution before adding the new one, so completions, exposures and
    binding changes only ever touch affected accounts.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

from .db import COMPLETION_WINDOW_US

_NATURAL_PARTS = re.compile(r"(\d+)")


def natural_key(value: str) -> tuple:
    """Natural ordering key: ``E9`` sorts before ``E10``."""

    parts: list[tuple[int, object]] = []
    for part in _NATURAL_PARTS.split(value):
        if not part:
            continue
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part))
    return (tuple(parts), value)


def exposure_sort_key(exposure_id: str, occurred_us: int) -> tuple:
    """Deterministic ordering: UTC timestamp first, then exposure number."""

    return (occurred_us, natural_key(exposure_id))


class ExposureConflict(Exception):
    """Same exposure id re-uploaded with different content."""

    def __init__(self, exposure_id: str) -> None:
        super().__init__(exposure_id)
        self.exposure_id = exposure_id


class CompletionConflict(Exception):
    """Same completion id re-uploaded with different content."""

    def __init__(self, completion_id: str) -> None:
        super().__init__(completion_id)
        self.completion_id = completion_id


def save_completion(conn: sqlite3.Connection, completion) -> bool:
    """Insert a completion; return ``True`` if new, ``False`` for a replay.

    A replay is only accepted when every field matches the stored row; a
    conflicting replay raises :class:`CompletionConflict`.
    """

    existing = conn.execute(
        "SELECT session_id, occurred_at FROM completions WHERE completion_id = ?",
        (completion.completion_id,),
    ).fetchone()
    if existing is not None:
        if (
            existing["session_id"] != completion.session_id
            or existing["occurred_at"] != completion.occurred_at
        ):
            raise CompletionConflict(completion.completion_id)
        return False

    conn.execute(
        "INSERT INTO completions "
        "(completion_id, session_id, occurred_at, occurred_us) "
        "VALUES (?, ?, ?, ?)",
        (
            completion.completion_id,
            completion.session_id,
            completion.occurred_at,
            completion.occurred_us,
        ),
    )
    return True


def save_exposure(conn: sqlite3.Connection, exposure) -> bool:
    """Insert an exposure; return ``True`` if new, ``False`` for a replay.

    A replay is only accepted when every field matches the stored row; a
    conflicting replay raises :class:`ExposureConflict`.
    """

    existing = conn.execute(
        "SELECT session_id, occurred_at, variant "
        "FROM exposures WHERE exposure_id = ?",
        (exposure.exposure_id,),
    ).fetchone()
    if existing is not None:
        if (
            existing["session_id"] != exposure.session_id
            or existing["occurred_at"] != exposure.occurred_at
            or existing["variant"] != exposure.variant
        ):
            raise ExposureConflict(exposure.exposure_id)
        return False

    conn.execute(
        "INSERT INTO exposures "
        "(exposure_id, session_id, occurred_at, occurred_us, variant) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            exposure.exposure_id,
            exposure.session_id,
            exposure.occurred_at,
            exposure.occurred_us,
            exposure.variant,
        ),
    )
    return True


def _first_per_variant(
    rows: Iterable[sqlite3.Row],
) -> list[tuple[str, str, int]]:
    """Collapse exposures to the earliest per variant (sorted by variant).

    Returns ``(variant, exposure_id, occurred_us)`` triples.
    """

    best: dict[str, tuple[tuple, str, int]] = {}
    for row in rows:
        variant = row["variant"]
        key = exposure_sort_key(row["exposure_id"], row["occurred_us"])
        if variant not in best or key < best[variant][0]:
            best[variant] = (key, row["exposure_id"], row["occurred_us"])
    return sorted(
        (variant, value[1], value[2]) for variant, value in best.items()
    )


def is_completer(conn: sqlite3.Connection, account_id: str, first_us: int) -> bool:
    """Did any session bound to ``account_id`` finish within 24h of ``first_us``?

    The window includes the first-exposure instant and excludes the instant
    24 hours later.
    """

    row = conn.execute(
        "SELECT 1 FROM session_bindings b "
        "JOIN completions c ON c.session_id = b.session_id "
        "WHERE b.account_id = ? AND c.occurred_us >= ? AND c.occurred_us < ? "
        "LIMIT 1",
        (account_id, first_us, first_us + COMPLETION_WINDOW_US),
    ).fetchone()
    return row is not None


def exposure_day(occurred_us: int) -> str:
    """UTC calendar day (``YYYY-MM-DD``) of a microsecond epoch instant."""

    return datetime.fromtimestamp(occurred_us / 1_000_000, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )


def _adjust_daily(
    conn: sqlite3.Connection,
    day: str,
    variant: str,
    *,
    samples: int,
    completers: int,
) -> None:
    """Add (or, with negative deltas, remove) one account's contribution."""

    if samples == 0 and completers == 0:
        return
    conn.execute(
        "INSERT INTO analysis_daily "
        "(exposure_day, variant, sample_count, completer_count) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(exposure_day, variant) DO UPDATE SET "
        "sample_count = sample_count + excluded.sample_count, "
        "completer_count = completer_count + excluded.completer_count",
        (day, variant, samples, completers),
    )
    conn.execute(
        "DELETE FROM analysis_daily "
        "WHERE exposure_day = ? AND variant = ? AND sample_count <= 0 "
        "AND completer_count <= 0",
        (day, variant),
    )


def _remove_daily_contribution(conn: sqlite3.Connection, account_id: str) -> None:
    """Subtract the account's currently stored sample row from the summary."""

    row = conn.execute(
        "SELECT s.variant, s.is_completer, e.occurred_us "
        "FROM analysis_samples s "
        "JOIN exposures e ON e.exposure_id = s.first_exposure_id "
        "WHERE s.account_id = ?",
        (account_id,),
    ).fetchone()
    if row is None:
        return
    _adjust_daily(
        conn,
        exposure_day(row["occurred_us"]),
        row["variant"],
        samples=-1,
        completers=-1 if row["is_completer"] else 0,
    )


def _replace_account_rows(
    conn: sqlite3.Connection, account_id: str, per_variant: Sequence[tuple[str, str, int]]
) -> None:
    """Replace the stored analysis rows for one account.

    One variant -> sample row; several variants -> cross-group rows; none ->
    the account disappears from both tables.  The old sample contribution is
    removed from the daily summary first; the new one is added back after the
    sample row is written.
    """

    _remove_daily_contribution(conn, account_id)
    conn.execute("DELETE FROM analysis_samples WHERE account_id = ?", (account_id,))
    conn.execute(
        "DELETE FROM analysis_cross_group WHERE account_id = ?", (account_id,)
    )
    if not per_variant:
        return
    if len(per_variant) == 1:
        variant, exposure_id, first_us = per_variant[0]
        completer = is_completer(conn, account_id, first_us)
        conn.execute(
            "INSERT INTO analysis_samples "
            "(account_id, variant, first_exposure_id, is_completer) "
            "VALUES (?, ?, ?, ?)",
            (account_id, variant, exposure_id, int(completer)),
        )
        _adjust_daily(
            conn,
            exposure_day(first_us),
            variant,
            samples=1,
            completers=1 if completer else 0,
        )
    else:
        conn.executemany(
            "INSERT INTO analysis_cross_group "
            "(account_id, variant, first_exposure_id) VALUES (?, ?, ?)",
            [
                (account_id, variant, exposure_id)
                for variant, exposure_id, _ in per_variant
            ],
        )


def derive_account(
    conn: sqlite3.Connection, account_id: str, *, exclude_session_id: str | None = None
) -> None:
    """Recompute one account from its currently bound sessions only.

    ``exclude_session_id`` treats that session as unbound, which is needed
    when an unbind has already removed the row inside the same transaction.
    Only this account's exposures are read; no global rescan takes place.
    """

    sql = (
        "SELECT e.exposure_id, e.variant, e.occurred_us AS occurred_us "
        "FROM session_bindings b JOIN exposures e ON e.session_id = b.session_id "
        "WHERE b.account_id = ?"
    )
    params: list[str] = [account_id]
    if exclude_session_id is not None:
        sql += " AND e.session_id <> ?"
        params.append(exclude_session_id)
    _replace_account_rows(conn, account_id, _first_per_variant(conn.execute(sql, params)))


def bind(conn: sqlite3.Connection, session_id: str, account_id: str | None) -> str:
    """Create, move or remove a session binding and refresh affected accounts.

    Returns ``"unchanged"``, ``"bound"`` or ``"unbound"``.
    """

    previous = conn.execute(
        "SELECT account_id FROM session_bindings WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    previous_account = previous["account_id"] if previous else None

    if account_id is None:
        if previous_account is None:
            return "unchanged"
        conn.execute(
            "DELETE FROM session_bindings WHERE session_id = ?", (session_id,)
        )
        derive_account(conn, previous_account, exclude_session_id=session_id)
        return "unbound"

    if previous_account == account_id:
        return "unchanged"

    conn.execute(
        "INSERT INTO session_bindings (session_id, account_id) VALUES (?, ?) "
        "ON CONFLICT(session_id) DO UPDATE SET account_id = excluded.account_id",
        (session_id, account_id),
    )
    if previous_account is not None:
        derive_account(conn, previous_account)
    derive_account(conn, account_id)
    return "bound"


def compute_analysis(conn: sqlite3.Connection) -> tuple[list[dict], list[dict]]:
    """Compute the full result from bound exposures, without writing it."""

    rows = conn.execute(
        "SELECT b.account_id, e.exposure_id, e.variant, e.occurred_us "
        "FROM session_bindings b JOIN exposures e ON e.session_id = b.session_id"
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["account_id"], []).append(row)

    samples: list[dict] = []
    cross: list[dict] = []
    for account_id in sorted(grouped):
        per_variant = _first_per_variant(grouped[account_id])
        if len(per_variant) == 1:
            variant, exposure_id, first_us = per_variant[0]
            samples.append(
                {
                    "account_id": account_id,
                    "variant": variant,
                    "first_exposure_id": exposure_id,
                    "is_completer": int(is_completer(conn, account_id, first_us)),
                }
            )
        else:
            for variant, exposure_id, _ in per_variant:
                cross.append(
                    {
                        "account_id": account_id,
                        "variant": variant,
                        "first_exposure_id": exposure_id,
                    }
                )
    return samples, cross


def persist_analysis(conn: sqlite3.Connection, updated_at: str) -> None:
    """Replace the stored analysis result with a freshly computed one.

    Must be called inside the caller's transaction; any failure propagates so
    the caller rolls back and the previously stored result survives intact.
    """

    samples, cross = compute_analysis(conn)
    write_analysis(conn, samples, cross, updated_at)


def write_analysis(
    conn: sqlite3.Connection,
    samples: Sequence[dict],
    cross: Sequence[dict],
    updated_at: str,
) -> None:
    """Atomically replace the stored sample, cross-group and daily rows.

    Callers must supply the rows inside an open transaction; the raw input
    tables are never touched here.
    """

    conn.execute("DELETE FROM analysis_samples")
    conn.execute("DELETE FROM analysis_cross_group")
    conn.execute("DELETE FROM analysis_daily")
    rows = [
        {
            "account_id": sample["account_id"],
            "variant": sample["variant"],
            "first_exposure_id": sample["first_exposure_id"],
            "is_completer": int(sample.get("is_completer", 0)),
        }
        for sample in samples
    ]
    conn.executemany(
        "INSERT INTO analysis_samples "
        "(account_id, variant, first_exposure_id, is_completer) "
        "VALUES (:account_id, :variant, :first_exposure_id, :is_completer)",
        rows,
    )
    conn.executemany(
        "INSERT INTO analysis_cross_group (account_id, variant, first_exposure_id) "
        "VALUES (:account_id, :variant, :first_exposure_id)",
        cross,
    )
    rebuild_daily_summary(conn)
    touch_analysis(conn, updated_at)


def rebuild_daily_summary(conn: sqlite3.Connection) -> None:
    """Recompute the daily summary from the stored sample rows."""

    conn.execute("DELETE FROM analysis_daily")
    conn.execute(
        "INSERT INTO analysis_daily "
        "(exposure_day, variant, sample_count, completer_count) "
        "SELECT date(e.occurred_us / 1000000, 'unixepoch'), s.variant, "
        "COUNT(*), SUM(s.is_completer) "
        "FROM analysis_samples s "
        "JOIN exposures e ON e.exposure_id = s.first_exposure_id "
        "GROUP BY 1, 2"
    )


def touch_analysis(conn: sqlite3.Connection, updated_at: str) -> None:
    """Record that the stored result was refreshed within this transaction."""

    conn.execute(
        "INSERT INTO analysis_meta (key, value) VALUES ('updated_at', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (updated_at,),
    )


@dataclass(frozen=True)
class StoredAnalysis:
    samples: list[dict]
    cross_group: list[dict]
    updated_at: str | None

    @property
    def generated(self) -> bool:
        return self.updated_at is not None


def load_analysis(conn: sqlite3.Connection) -> StoredAnalysis:
    """Read the stored sample and cross-group rows from one snapshot."""

    samples = [
        dict(row)
        for row in conn.execute(
            "SELECT account_id, variant, first_exposure_id, is_completer "
            "FROM analysis_samples ORDER BY account_id"
        ).fetchall()
    ]
    cross = [
        dict(row)
        for row in conn.execute(
            "SELECT account_id, variant, first_exposure_id "
            "FROM analysis_cross_group ORDER BY account_id, variant"
        ).fetchall()
    ]
    meta = conn.execute(
        "SELECT value FROM analysis_meta WHERE key = 'updated_at'"
    ).fetchone()
    return StoredAnalysis(
        samples=samples,
        cross_group=cross,
        updated_at=meta["value"] if meta else None,
    )


def load_daily_summary(conn: sqlite3.Connection) -> list[dict]:
    """Read the stored daily summary, ordered by day then group."""

    return [
        {
            "exposure_day": row["exposure_day"],
            "group": row["variant"],
            "variant": row["variant"],
            "sample_count": row["sample_count"],
            "completer_count": row["completer_count"],
            "completion_rate": (
                row["completer_count"] / row["sample_count"]
                if row["sample_count"]
                else None
            ),
        }
        for row in conn.execute(
            "SELECT exposure_day, variant, sample_count, completer_count "
            "FROM analysis_daily ORDER BY exposure_day, variant"
        ).fetchall()
    ]
