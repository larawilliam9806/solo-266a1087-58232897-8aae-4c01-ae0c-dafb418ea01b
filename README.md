# News Guide A/B experiment service

FastAPI + SQLite service that records exposures, session/account bindings and
finished-reading reports, and serves the de-duplicated sample plus a daily
completion summary.

## Run with Docker Compose

```bash
docker compose up --build
# service on http://localhost:8000  (override with PORT=...)
# SQLite file persisted in the `experiment-data` volume
```

## Run locally

```bash
pip install -r requirements.txt
DATABASE_PATH=data/experiment.sqlite3 uvicorn app.main:app --port 8000
```

Tests: `pip install -r requirements-dev.txt && pytest`

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/exposures` | one exposure or a batch; idempotent by exposure number |
| `POST` | `/completions` | one completion or a batch; idempotent by completion number |
| `POST` | `/bindings` | bind/rebind sessions (`null` unbinds) |
| `PUT` | `/bindings/{session_id}` | bind/rebind one session |
| `DELETE` | `/bindings/{session_id}` | unbind one session |
| `GET` | `/analysis` | stored sample / cross-group rows |
| `POST` | `/analysis` | full rebuild of sample, cross-group and daily rows |
| `GET` | `/analysis/daily` | sample/completer counts and rate per UTC day + group |
| `GET` | `/exposures`, `/completions`, `/bindings` | inspection |

### Completion report

```json
POST /completions
{"completion_id": "C1001", "session_id": "S42", "occurred_at": "2026-01-01T09:30:00Z"}
```

The session need not be bound yet. Re-sending the same completion number is
accepted only when session and UTC time match the stored record (otherwise
`409 completion_conflict`).

### Daily summary

```json
GET /analysis/daily?day=2026-01-01&group=A
{"daily": [{"exposure_day": "2026-01-01", "group": "A", "variant": "A",
            "sample_count": 120, "completer_count": 48,
            "completion_rate": 0.4}], ...}
```

`day`/`date` and `group`/`variant` are accepted as query parameter aliases.
Rows are always keyed by the UTC day of the account's first exposure.

## Rules

* An account enters the sample when all its bound exposures are in one group;
  accounts that saw several groups are excluded and listed separately.
* A sampled account is a completer when **any** session currently bound to it
  has a completion at or after the first exposure and **before** 24 hours
  later (start inclusive, end exclusive).
* Daily counts are incremental: each account's old day/group contribution is
  removed before its new one is added, so late exposures, late completions
  and binding changes never leave stale rows.
* Input, sample, cross-group and daily rows are written in one transaction;
  any failure leaves the previous state intact.
* An existing database is upgraded on start (new completer flag, daily summary
  backfilled from the stored sample) and is immediately queryable.
