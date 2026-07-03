"""Notebook storage: discovery, JSONL entries, and the SQLite query index.

Depends on `schema`; never imported by it. `cli` depends on this module.
"""
from __future__ import annotations

import json
import os
import secrets
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from .schema import CORE_FIELDS, RETRACT_TYPE, SchemaSQL, build_sql, load_schema

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

LNB_ENV_FILE = ".lnb.env"


def _find_lnb_env(start: Path | None = None) -> Path | None:
    """Walk up from *start* looking for .lnb.env, stop at $HOME or /."""
    cur = (start or Path.cwd()).resolve()
    home = Path.home()
    while True:
        candidate = cur / LNB_ENV_FILE
        if candidate.is_file():
            return candidate
        if cur == home or cur == cur.parent:
            return None
        cur = cur.parent


def _parse_lnb_env(env_file: Path) -> str | None:
    """Extract LAB_NOTEBOOK_DIR value from a .lnb.env file."""
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip optional 'export ' prefix
        if line.startswith("export "):
            line = line[7:]
        if line.startswith("LAB_NOTEBOOK_DIR="):
            val = line.split("=", 1)[1]
            # Strip surrounding quotes
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            return val
    return None


def get_notebook_dir(hint: str = "") -> Path:
    # 1. Explicit $LAB_NOTEBOOK_DIR wins (standard Unix precedence)
    d = os.environ.get("LAB_NOTEBOOK_DIR")
    if d:
        return Path(d)
    # 2. Fall back to nearest .lnb.env walking up from CWD
    env_file = _find_lnb_env()
    if env_file:
        val = _parse_lnb_env(env_file)
        if val:
            return Path(val)
    # 3. Error
    print("Error: LAB_NOTEBOOK_DIR is not set and no .lnb.env found.", file=sys.stderr)
    if hint:
        print(hint, file=sys.stderr)
    else:
        print("Set $LAB_NOTEBOOK_DIR, or run 'lab-notebook init' to create\n"
              "a project-local notebook (writes .lnb.env in the current directory).",
              file=sys.stderr)
    sys.exit(1)


def get_writer_id() -> str:
    return os.environ.get("LAB_NOTEBOOK_WRITER") or os.environ.get("USER", "unknown")


def generate_id() -> str:
    now = datetime.now()
    ts = now.strftime("%Y%m%dT%H%M%S")
    rand = secrets.token_hex(2)
    return f"{ts}-{rand}"


def entries_dir(notebook_dir: Path) -> Path:
    return notebook_dir / "entries"


def index_path(notebook_dir: Path) -> Path:
    return notebook_dir / "index.sqlite"


def flatten_entry(entry: dict, schema: dict) -> dict:
    fields = schema.get("fields", {})
    list_fields = {name for name, spec in fields.items() if spec["type"] == "list"}
    all_known = set(CORE_FIELDS) | set(fields.keys())

    flat = {}
    extras = {}
    for key, val in entry.items():
        if key in all_known:
            if key in list_fields and val is not None and not isinstance(val, str):
                flat[key] = json.dumps(val)
            else:
                flat[key] = val
        else:
            extras[key] = val

    # Ensure all schema columns are present (as None if missing)
    for col in all_known:
        flat.setdefault(col, None)
    flat["extra"] = json.dumps(extras) if extras else None
    return flat


def upsert_entry(conn: sqlite3.Connection, entry: dict, sql: SchemaSQL,
                  commit: bool = True) -> None:
    conn.execute(sql.upsert, entry)
    row = conn.execute(
        "SELECT rowid FROM entries WHERE id = :id", {"id": entry["id"]}
    ).fetchone()
    if row:
        rowid = row[0]
        conn.execute("DELETE FROM entries_fts WHERE rowid = ?", (rowid,))
        fts_params = {"rowid": rowid}
        for col in sql.fts_cols:
            fts_params[col] = entry.get(col, "")
        conn.execute(sql.fts_insert, fts_params)
    if commit:
        conn.commit()


def delete_entry(conn: sqlite3.Connection, target_id: str) -> bool:
    """Hard-delete a target entry and its FTS row. No-op if it isn't present
    (already retracted, or the tombstone references an id that never existed).
    Returns True if a row was actually removed, False on the no-op path."""
    row = conn.execute(
        "SELECT rowid FROM entries WHERE id = ?", (target_id,)
    ).fetchone()
    if row:
        conn.execute("DELETE FROM entries_fts WHERE rowid = ?", (row[0],))
        conn.execute("DELETE FROM entries WHERE id = ?", (target_id,))
        return True
    return False


def _atomic_rebuild(notebook_dir: Path, dbp: Path,
                     schema: dict, sql: SchemaSQL) -> int:
    """Rebuild the index into a temp file, then atomically rename into place."""
    fd, tmp = tempfile.mkstemp(dir=str(notebook_dir), suffix='.sqlite')
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        conn.executescript(sql.create)
        rebuild_from_jsonl(conn, notebook_dir, schema, sql)
        # Report the net active count (rows actually in the index after
        # tombstones are applied), not the number of lines ingested.
        count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        conn.close()
        os.rename(tmp, str(dbp))
        return count
    except BaseException:
        os.unlink(tmp)
        raise


def _schema_newer_than_index(notebook_dir: Path, dbp: Path) -> bool:
    schema_file = notebook_dir / "schema.yaml"
    return schema_file.exists() and schema_file.stat().st_mtime >= dbp.stat().st_mtime


class _IngestFenceTripped(Exception):
    """Raised when a JSONL file's size dropped below its recorded ingest offset."""


def incremental_ingest(conn: sqlite3.Connection, notebook_dir: Path,
                       schema: dict, sql: SchemaSQL) -> tuple[int, int]:
    """Per-file byte-offset incremental ingest. Returns (added, retracted): the
    count of newly-inserted entries and the count of entries actually removed by
    tombstones in this pass.

    Raises _IngestFenceTripped if any JSONL file is smaller than its recorded
    offset (truncation / external rewrite); caller should fall back to full rebuild.
    """
    edir = entries_dir(notebook_dir)
    if not edir.exists():
        return 0, 0
    # Defensive: an existing pre-_ingest_state index opened by new code reaches
    # this path; create the table so absent rows = offset 0 works transparently.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _ingest_state "
        "(file TEXT PRIMARY KEY, offset INTEGER NOT NULL)"
    )
    # Two-phase, single-transaction: scan every file first (upserting content
    # entries and collecting tombstone targets + new offsets), then apply all
    # deletes and offset writes in one commit. Deferring deletes makes retraction
    # order-independent — a tombstone may target an entry in a later-sorted file
    # (any writer can retract any id), and on a full rebuild the target is only
    # inserted later in the same pass. Committing once keeps the tombstone's
    # offset advance atomic with its delete, so a crash can't strand a target.
    total = 0
    new_offsets: dict[str, int] = {}
    pending_deletes: list[str] = []
    try:
        for jsonl_file in sorted(edir.glob("*.jsonl")):
            size = jsonl_file.stat().st_size
            row = conn.execute(
                "SELECT offset FROM _ingest_state WHERE file = ?",
                (jsonl_file.name,),
            ).fetchone()
            offset = row[0] if row else 0
            if size < offset:
                raise _IngestFenceTripped(jsonl_file.name)
            if size == offset:
                continue
            with open(jsonl_file, "rb") as f:
                f.seek(offset)
                chunk = f.read(size - offset)
            last_safe_pos = offset
            cursor = 0
            while True:
                nl = chunk.find(b"\n", cursor)
                if nl < 0:
                    break  # partial trailing line — leave for next read
                line_bytes = chunk[cursor:nl]
                cursor = nl + 1
                line_end_pos = offset + cursor  # byte position AFTER the \n
                stripped = line_bytes.strip()
                if not stripped:
                    last_safe_pos = line_end_pos
                    continue
                try:
                    entry = json.loads(stripped)
                except json.JSONDecodeError as e:
                    print(
                        f"Warning: {jsonl_file.name}: skipping malformed line: {e}",
                        file=sys.stderr,
                    )
                    last_safe_pos = line_end_pos
                    continue
                if entry.get("type") == RETRACT_TYPE:
                    target = entry.get("retracts")
                    if target:
                        pending_deletes.append(target)
                    last_safe_pos = line_end_pos
                    continue
                flat = flatten_entry(entry, schema)
                upsert_entry(conn, flat, sql, commit=False)
                total += 1
                last_safe_pos = line_end_pos
            new_offsets[jsonl_file.name] = last_safe_pos
        # Deferred-offset safety relies on cmd_retract validating that the target
        # is already indexed before it writes the tombstone, so a tombstone can
        # never outrun its target into a later-sorted file and resurrect it.
        retracted = 0
        for target in pending_deletes:
            if delete_entry(conn, target):
                retracted += 1
        for name, pos in new_offsets.items():
            conn.execute(
                "INSERT OR REPLACE INTO _ingest_state (file, offset) VALUES (?, ?)",
                (name, pos),
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return total, retracted


def ensure_db(notebook_dir: Path, schema: dict | None = None) -> tuple[sqlite3.Connection, dict, SchemaSQL]:
    if schema is None:
        schema = load_schema(notebook_dir)
    sql = build_sql(schema)
    dbp = index_path(notebook_dir)
    if not dbp.exists() or _schema_newer_than_index(notebook_dir, dbp):
        count = _atomic_rebuild(notebook_dir, dbp, schema, sql)
        print(f"Index rebuilt: {count} entries", file=sys.stderr)
    else:
        conn = sqlite3.connect(str(dbp))
        fence_tripped = None
        added = retracted = 0
        try:
            added, retracted = incremental_ingest(conn, notebook_dir, schema, sql)
        except _IngestFenceTripped as e:
            fence_tripped = str(e)
        finally:
            conn.close()
        if fence_tripped is not None:
            print(f"Ingest fence tripped on {fence_tripped}; "
                  f"falling back to full rebuild", file=sys.stderr)
            count = _atomic_rebuild(notebook_dir, dbp, schema, sql)
            print(f"Index rebuilt: {count} entries", file=sys.stderr)
        elif added and retracted:
            print(f"Index updated: +{added} entries, -{retracted} retracted",
                  file=sys.stderr)
        elif added:
            print(f"Index updated: +{added} entries", file=sys.stderr)
        elif retracted:
            print(f"Index updated: -{retracted} retracted", file=sys.stderr)
    conn = sqlite3.connect(str(dbp))
    return conn, schema, sql


def rebuild_from_jsonl(conn: sqlite3.Connection, notebook_dir: Path,
                       schema: dict, sql: SchemaSQL) -> tuple[int, int]:
    """Clear the index and re-ingest every JSONL from byte 0.

    Implemented by clearing entries/entries_fts/_ingest_state and delegating
    to incremental_ingest. That gets EOF-safe offset bookkeeping for free,
    so a fresh index ends with _ingest_state populated to current file sizes.
    """
    edir = entries_dir(notebook_dir)
    if not edir.exists():
        return 0, 0
    conn.execute("DELETE FROM entries")
    conn.execute("DELETE FROM entries_fts")
    conn.execute("DELETE FROM _ingest_state")
    conn.commit()
    return incremental_ingest(conn, notebook_dir, schema, sql)
