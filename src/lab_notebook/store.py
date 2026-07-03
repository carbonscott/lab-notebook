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

from .schema import (
    CORE_FIELDS,
    INDEX_USER_VERSION,
    RETRACT_TYPE,
    LnbError,
    SchemaSQL,
    build_sql,
    load_schema,
)

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
    msg = "Error: LAB_NOTEBOOK_DIR is not set and no .lnb.env found."
    if hint:
        msg += "\n" + hint
    else:
        msg += ("\nSet $LAB_NOTEBOOK_DIR, or run 'lab-notebook init' to create\n"
                "a project-local notebook (writes .lnb.env in the current directory).")
    raise LnbError(msg)


def get_writer_id() -> str:
    return os.environ.get("LAB_NOTEBOOK_WRITER") or os.environ.get("USER", "unknown")


def generate_id() -> str:
    now = datetime.now()
    ts = now.strftime("%Y%m%dT%H%M%S")
    rand = secrets.token_hex(4)
    return f"{ts}-{rand}"


def entries_dir(notebook_dir: Path) -> Path:
    return notebook_dir / "entries"


def index_path(notebook_dir: Path) -> Path:
    return notebook_dir / "index.sqlite"


def _connect(dbp: Path) -> sqlite3.Connection:
    """Open the index with recursive_triggers ON.

    entries_fts is kept in sync by triggers on entries. INSERT OR REPLACE in
    upsert_entry deletes the replaced row before inserting the new one, but that
    implicit delete only fires the AFTER DELETE trigger when recursive_triggers
    is enabled. Without it, the replaced row's FTS postings orphan in the index
    and can corrupt search once SQLite reuses the freed rowid. Every connection
    that may write to entries goes through here.
    """
    conn = sqlite3.connect(str(dbp))
    conn.execute("PRAGMA recursive_triggers = ON")
    return conn


def _index_user_version(dbp: Path) -> int:
    """Return the index's stamped layout version (0 for indexes predating it)."""
    conn = sqlite3.connect(str(dbp))
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


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
    # entries_fts is external-content and kept in sync by triggers on entries.
    # INSERT OR REPLACE fires the delete trigger (for the replaced row) and the
    # insert trigger (for the new row); the connection must have
    # recursive_triggers ON so REPLACE's implicit delete fires — see ensure_db.
    conn.execute(sql.upsert, entry)
    if commit:
        conn.commit()


def delete_entry(conn: sqlite3.Connection, target_id: str) -> bool:
    """Hard-delete a target entry. The AFTER DELETE trigger keeps entries_fts in
    sync. No-op if it isn't present (already retracted, or the tombstone
    references an id that never existed). Returns True if a row was actually
    removed, False on the no-op path."""
    return conn.execute(
        "DELETE FROM entries WHERE id = ?", (target_id,)
    ).rowcount > 0


def _atomic_rebuild(notebook_dir: Path, dbp: Path,
                     schema: dict, sql: SchemaSQL) -> int:
    """Rebuild the index into a temp file, then atomically rename into place."""
    fd, tmp = tempfile.mkstemp(dir=str(notebook_dir), suffix='.sqlite')
    os.close(fd)
    try:
        conn = _connect(Path(tmp))
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
    # Rebuild from scratch when the index is missing, older than the schema, or
    # written in a prior index layout (user_version mismatch). The index is
    # disposable, so a version bump migrates old indexes transparently.
    stale = (
        not dbp.exists()
        or _schema_newer_than_index(notebook_dir, dbp)
        or _index_user_version(dbp) != INDEX_USER_VERSION
    )
    if stale:
        count = _atomic_rebuild(notebook_dir, dbp, schema, sql)
        print(f"Index rebuilt: {count} entries", file=sys.stderr)
    else:
        conn = _connect(dbp)
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
    conn = _connect(dbp)
    return conn, schema, sql


class Notebook:
    """Programmatic API over a notebook directory.

    All CLI commands are thin wrappers around this class. Field validation and
    coercion live in `emit` (not in the CLI layer), so every caller — the CLI,
    tests, or other Python code — gets the same enforcement.

    The schema is loaded eagerly in `__init__`; a missing or invalid
    `schema.yaml` raises `LnbError` up front.
    """

    def __init__(self, dir: Path):
        """`dir` is the resolved notebook directory. Loads schema eagerly
        (raises LnbError if schema.yaml is missing/invalid)."""
        self.dir = Path(dir)
        self.writer_id = get_writer_id()
        self.schema = load_schema(self.dir)
        self._conn: sqlite3.Connection | None = None

    def _append(self, record: dict) -> None:
        """Append one JSON record to the current writer's JSONL file, fsynced."""
        edir = entries_dir(self.dir)
        edir.mkdir(exist_ok=True)
        writer_file = edir / f"{self.writer_id}.jsonl"
        with open(writer_file, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def emit(self, context: str, type: str, content: str,
             fields: dict | None = None, extra: dict | None = None) -> dict:
        """Validate type against schema, validate/coerce fields, append to the
        writer's JSONL (fsync), return the entry dict.

        `fields` maps schema field names to values. List fields accept either a
        comma-separated string (CLI convention) or an already-split list;
        integer/real fields are coerced. `extra` holds undeclared key/value
        pairs; a key that collides with a core or schema field raises LnbError.
        """
        if type not in self.schema["types"]:
            raise LnbError(
                f"Error: type must be one of {self.schema['types']}, got '{type}'"
            )

        entry = {
            "id": generate_id(),
            "ts": datetime.now().astimezone().isoformat(timespec="seconds"),
            "writer_id": self.writer_id,
            "context": context,
            "type": type,
            "content": content,
        }

        schema_fields = self.schema.get("fields", {})
        fields = fields or {}

        # Reject unknown field names up front. schema_fields already includes the
        # merged built-ins (e.g. artifacts), so this catches typos and values
        # meant for the schema that were never declared. Point the caller at
        # --extra, the escape hatch for undeclared keys.
        for name in fields:
            if name not in schema_fields:
                declared = ", ".join(schema_fields) or "(none)"
                raise LnbError(
                    f"Error: unknown field '{name}'. Declared fields: {declared}. "
                    f"Use --extra {name}=... for an undeclared field."
                )

        for name, spec in schema_fields.items():
            val = fields.get(name)
            if val is not None:
                if spec["type"] == "list":
                    if isinstance(val, str):
                        val = [v.strip() for v in val.split(",") if v.strip()]
                elif spec["type"] == "integer":
                    try:
                        val = int(val)
                    except (TypeError, ValueError):
                        raise LnbError(
                            f"Error: field '{name}' expects an integer, got '{val}'"
                        )
                elif spec["type"] == "real":
                    try:
                        val = float(val)
                    except (TypeError, ValueError):
                        raise LnbError(
                            f"Error: field '{name}' expects a real number, got '{val}'"
                        )
            entry[name] = val

        reserved_keys = set(CORE_FIELDS) | set(schema_fields.keys()) | {"extra"}
        for key, value in (extra or {}).items():
            if key in reserved_keys:
                raise LnbError(
                    f"Error: --extra key '{key}' conflicts with a declared field. "
                    f"Use --{key} instead."
                )
            entry[key] = value

        self._append(entry)
        return entry

    def retract(self, target_id: str, reason: str) -> dict:
        """Verify the target exists in the index (LnbError if not), append a
        tombstone to the writer's JSONL, and return the tombstone dict.

        The deletion is applied lazily on the next indexed read, the same way
        emit indexes lazily.
        """
        conn, _, _ = ensure_db(self.dir, self.schema)
        try:
            found = conn.execute(
                "SELECT id FROM entries WHERE id = ?", (target_id,)
            ).fetchone()
        finally:
            conn.close()
        if not found:
            raise LnbError(
                f"Error: entry '{target_id}' not found "
                f"(already retracted or never existed)"
            )

        tombstone = {
            "id": generate_id(),
            "ts": datetime.now().astimezone().isoformat(timespec="seconds"),
            "writer_id": self.writer_id,
            "type": RETRACT_TYPE,
            "retracts": target_id,
            "reason": reason,
        }
        self._append(tombstone)
        return tombstone

    def query(self, sql: str, params=()) -> sqlite3.Cursor:
        """Ensure index freshness (incremental ingest / rebuild), then execute.
        Caller is responsible for closing via `close()`."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._conn, _, _ = ensure_db(self.dir, self.schema)
        return self._conn.execute(sql, params)

    def get(self, entry_id: str) -> dict:
        """Return one entry as an ordered ``{column: value}`` dict, or raise
        LnbError if the id is not in the index (retracted or never existed).

        Column order follows the table definition — core fields, then schema
        fields, then the raw ``extra`` JSON blob — which callers decode for
        display.
        """
        cursor = self.query("SELECT * FROM entries WHERE id = ?", (entry_id,))
        row = cursor.fetchone()
        if row is None:
            raise LnbError(
                f"Error: entry '{entry_id}' not found "
                f"(retracted or never existed)"
            )
        cols = [d[0] for d in cursor.description]
        return dict(zip(cols, row))

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def rebuild_from_jsonl(conn: sqlite3.Connection, notebook_dir: Path,
                       schema: dict, sql: SchemaSQL) -> tuple[int, int]:
    """Clear the index and re-ingest every JSONL from byte 0.

    Implemented by clearing entries/_ingest_state and delegating to
    incremental_ingest. That gets EOF-safe offset bookkeeping for free, so a
    fresh index ends with _ingest_state populated to current file sizes.
    Deleting the entries rows fires the AFTER DELETE trigger, which clears the
    external-content entries_fts in step (no separate FTS delete needed).
    """
    edir = entries_dir(notebook_dir)
    if not edir.exists():
        return 0, 0
    conn.execute("DELETE FROM entries")
    conn.execute("DELETE FROM _ingest_state")
    conn.commit()
    return incremental_ingest(conn, notebook_dir, schema, sql)
