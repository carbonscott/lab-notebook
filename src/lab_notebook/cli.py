"""Lab notebook: append-only JSONL entries + SQLite query index.

Usage:
    lab-notebook init [path]
    lab-notebook emit --context X --type Y "content"
    lab-notebook sql "SELECT ..."
    lab-notebook search "query"
    lab-notebook schema
    lab-notebook rebuild
    lab-notebook contexts
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ALLOWED_TYPES = ("observation", "decision", "dead-end", "question", "milestone")

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS entries (
    id         TEXT PRIMARY KEY,
    ts         TEXT NOT NULL,
    writer_id  TEXT NOT NULL,
    context    TEXT NOT NULL,
    type       TEXT NOT NULL,
    repo       TEXT,
    branch     TEXT,
    tags       TEXT,
    artifacts  TEXT,
    content    TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(content, context);
CREATE INDEX IF NOT EXISTS idx_entries_context ON entries(context);
CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);
CREATE INDEX IF NOT EXISTS idx_entries_ts ON entries(ts);
CREATE INDEX IF NOT EXISTS idx_entries_repo ON entries(repo);
"""

UPSERT_SQL = """\
INSERT OR REPLACE INTO entries
    (id, ts, writer_id, context, type, repo, branch, tags, artifacts, content)
VALUES
    (:id, :ts, :writer_id, :context, :type, :repo, :branch, :tags, :artifacts, :content);
"""

FTS_INSERT_SQL = """\
INSERT INTO entries_fts (rowid, content, context)
VALUES (:rowid, :content, :context);
"""

SCHEMA_HELP = """\
Table: entries
--------------
  id         TEXT PRIMARY KEY   -- e.g. 20260321T143022-a7f2
  ts         TEXT NOT NULL      -- ISO 8601 local time
  writer_id  TEXT NOT NULL      -- e.g. cong, agent-claude-01
  context    TEXT NOT NULL      -- e.g. maxie/ssl-comparison
  type       TEXT NOT NULL      -- observation, decision, dead-end, question, milestone
  repo       TEXT               -- e.g. research-lrn091
  branch     TEXT               -- e.g. phase0/data-loading
  tags       TEXT               -- JSON array, e.g. '["mae","masking"]'
  artifacts  TEXT               -- JSON array, e.g. '["repo:path/to/file.csv"]'
  content    TEXT NOT NULL      -- free-text notebook prose

FTS table: entries_fts (content, context)

Example queries
---------------
-- Recent entries
SELECT ts, type, substr(content, 1, 80) FROM entries ORDER BY ts DESC LIMIT 10;

-- All decisions in a context
SELECT ts, substr(content, 1, 80) FROM entries
WHERE context = 'maxie/ssl-comparison' AND type = 'decision' ORDER BY ts;

-- Dead ends across all contexts
SELECT context, ts, substr(content, 1, 80) FROM entries
WHERE type = 'dead-end' ORDER BY ts DESC;

-- Full-text search
SELECT e.ts, e.context, e.type, substr(e.content, 1, 80) FROM entries e
JOIN entries_fts f ON f.rowid = e.rowid
WHERE entries_fts MATCH 'broker manifest';

-- Filter by tag
SELECT ts, context, type FROM entries
WHERE EXISTS (SELECT 1 FROM json_each(tags) WHERE value = 'scaling');

-- Entries per context
SELECT context, COUNT(*) AS n, MIN(ts) AS first, MAX(ts) AS latest
FROM entries GROUP BY context ORDER BY latest DESC;

-- Entries by a specific writer
SELECT ts, context, type, substr(content, 1, 80) FROM entries
WHERE writer_id = 'cong' ORDER BY ts DESC LIMIT 10;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_notebook_dir() -> Path:
    d = os.environ.get("LAB_NOTEBOOK_DIR")
    if d:
        return Path(d)
    print("Error: LAB_NOTEBOOK_DIR environment variable is not set.", file=sys.stderr)
    print("Run 'lab-notebook init' to create a notebook, then source the .env file.", file=sys.stderr)
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


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def flatten_entry(entry: dict) -> dict:
    flat = dict(entry)
    for key in ("tags", "artifacts"):
        val = flat.get(key)
        if val is not None and not isinstance(val, str):
            flat[key] = json.dumps(val)
    return flat


def upsert_entry(conn: sqlite3.Connection, entry: dict) -> None:
    conn.execute(UPSERT_SQL, entry)
    row = conn.execute(
        "SELECT rowid FROM entries WHERE id = :id", {"id": entry["id"]}
    ).fetchone()
    if row:
        rowid = row[0]
        conn.execute("DELETE FROM entries_fts WHERE rowid = ?", (rowid,))
        conn.execute(FTS_INSERT_SQL, {
            "rowid": rowid, "content": entry["content"], "context": entry["context"],
        })
    conn.commit()


def ensure_db(notebook_dir: Path) -> sqlite3.Connection:
    dbp = index_path(notebook_dir)
    needs_rebuild = not dbp.exists()
    conn = sqlite3.connect(str(dbp))
    init_db(conn)
    if needs_rebuild:
        rebuild_from_jsonl(conn, notebook_dir)
    return conn


def rebuild_from_jsonl(conn: sqlite3.Connection, notebook_dir: Path) -> int:
    edir = entries_dir(notebook_dir)
    if not edir.exists():
        return 0
    conn.execute("DELETE FROM entries")
    conn.execute("DELETE FROM entries_fts")
    count = 0
    for jsonl_file in sorted(edir.glob("*.jsonl")):
        with open(jsonl_file) as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Warning: {jsonl_file.name}:{lineno}: skipping malformed line: {e}",
                          file=sys.stderr)
                    continue
                flat = flatten_entry(entry)
                conn.execute(UPSERT_SQL, flat)
                row = conn.execute(
                    "SELECT rowid FROM entries WHERE id = ?", (flat["id"],)
                ).fetchone()
                if row:
                    conn.execute(FTS_INSERT_SQL, {
                        "rowid": row[0], "content": flat["content"], "context": flat["context"],
                    })
                count += 1
    conn.commit()
    return count


def print_table(cursor: sqlite3.Cursor) -> None:
    rows = cursor.fetchall()
    if not rows:
        print("(no results)")
        return
    cols = [desc[0] for desc in cursor.description]
    widths = [len(c) for c in cols]
    str_rows = []
    for row in rows:
        str_row = [str(v) if v is not None else "" for v in row]
        str_rows.append(str_row)
        for i, v in enumerate(str_row):
            widths[i] = max(widths[i], len(v))
    max_col = 80
    widths = [min(w, max_col) for w in widths]
    header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
    sep = "  ".join("-" * widths[i] for i in range(len(cols)))
    print(header)
    print(sep)
    for str_row in str_rows:
        line = "  ".join(str_row[i][:max_col].ljust(widths[i]) for i in range(len(cols)))
        print(line)
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_init(args: argparse.Namespace) -> None:
    target = Path(args.path or ".").resolve()
    if not target.exists():
        print(f"Error: directory does not exist: {target}", file=sys.stderr)
        sys.exit(1)
    if not target.is_dir():
        print(f"Error: not a directory: {target}", file=sys.stderr)
        sys.exit(1)

    edir = target / "entries"
    edir.mkdir(exist_ok=True)

    gitignore = target / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("index.sqlite\n")
    elif "index.sqlite" not in gitignore.read_text():
        with open(gitignore, "a") as f:
            f.write("index.sqlite\n")

    writer = os.environ.get("USER", "unknown")
    env_file = target / ".env"
    env_file.write_text(
        f"export LAB_NOTEBOOK_DIR={target}\n"
        f"export LAB_NOTEBOOK_WRITER={writer}\n"
    )

    print(f"Initialized lab notebook in {target}")
    print(f"  entries/     — per-writer JSONL files")
    print(f"  .gitignore   — ignores index.sqlite")
    print(f"  .env         — LAB_NOTEBOOK_DIR={target}")
    print(f"                  LAB_NOTEBOOK_WRITER={writer}")
    print(f"\nNext: source {env_file}")


def cmd_emit(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    writer_id = get_writer_id()

    if args.type not in ALLOWED_TYPES:
        print(f"Error: type must be one of {ALLOWED_TYPES}, got '{args.type}'",
              file=sys.stderr)
        sys.exit(1)

    tags = [t.strip() for t in args.tags.split(",") if t.strip()] if args.tags else None
    artifacts = [a.strip() for a in args.artifacts.split(",") if a.strip()] if args.artifacts else None

    entry = {
        "id": generate_id(),
        "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "writer_id": writer_id,
        "context": args.context,
        "type": args.type,
        "repo": args.repo,
        "branch": args.branch,
        "tags": tags,
        "artifacts": artifacts,
        "content": args.content,
    }

    edir = entries_dir(notebook_dir)
    edir.mkdir(exist_ok=True)
    writer_file = edir / f"{writer_id}.jsonl"
    with open(writer_file, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())

    flat = flatten_entry(entry)
    conn = ensure_db(notebook_dir)
    try:
        upsert_entry(conn, flat)
    finally:
        conn.close()

    print(f"[{entry['type']}] {entry['id']}  {entry['context']}")


def cmd_sql(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    conn = ensure_db(notebook_dir)
    try:
        cursor = conn.execute(args.query)
        print_table(cursor)
    except sqlite3.OperationalError as e:
        print(f"SQL error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def cmd_search(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    conn = ensure_db(notebook_dir)
    try:
        sql = """SELECT e.ts, e.context, e.type, e.writer_id, substr(e.content, 1, 120)
                 FROM entries e
                 JOIN entries_fts f ON f.rowid = e.rowid
                 WHERE entries_fts MATCH :query"""
        params: dict = {"query": args.query}
        if args.context:
            sql += " AND e.context = :context"
            params["context"] = args.context
        if args.type:
            sql += " AND e.type = :type"
            params["type"] = args.type
        sql += " ORDER BY e.ts DESC"
        cursor = conn.execute(sql, params)
        print_table(cursor)
    except sqlite3.OperationalError as e:
        print(f"Search error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def cmd_schema(args: argparse.Namespace) -> None:
    print(SCHEMA_HELP)


def cmd_rebuild(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    dbp = index_path(notebook_dir)
    if dbp.exists():
        dbp.unlink()
    conn = sqlite3.connect(str(dbp))
    init_db(conn)
    try:
        count = rebuild_from_jsonl(conn, notebook_dir)
        print(f"Rebuilt index: {count} entries from {entries_dir(notebook_dir)}")
    finally:
        conn.close()


def cmd_contexts(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    conn = ensure_db(notebook_dir)
    try:
        cursor = conn.execute("""\
            SELECT context, COUNT(*) AS entries, MIN(ts) AS first, MAX(ts) AS latest
            FROM entries GROUP BY context ORDER BY latest DESC
        """)
        print_table(cursor)
    except sqlite3.OperationalError as e:
        print(f"SQL error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lab-notebook",
        description="Lab notebook: append-only JSONL + SQLite query index",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- init --
    p_init = sub.add_parser("init", help="Initialize a notebook directory")
    p_init.add_argument("path", nargs="?", default=None,
                        help="Directory to initialize (default: current directory)")
    p_init.set_defaults(func=cmd_init)

    # -- emit --
    p_emit = sub.add_parser("emit", help="Write a notebook entry")
    p_emit.add_argument("--context", required=True, help="Research context (e.g. maxie/ssl-comparison)")
    p_emit.add_argument("--type", required=True, help=f"Entry type: {', '.join(ALLOWED_TYPES)}")
    p_emit.add_argument("--repo", help="Repository name")
    p_emit.add_argument("--branch", help="Branch name")
    p_emit.add_argument("--tags", help="Comma-separated tags")
    p_emit.add_argument("--artifacts", help="Comma-separated artifact paths")
    p_emit.add_argument("content", help="Entry content (notebook prose)")
    p_emit.set_defaults(func=cmd_emit)

    # -- sql --
    p_sql = sub.add_parser("sql", help="Run a SQL query against the index")
    p_sql.add_argument("query", help="SQL query string")
    p_sql.set_defaults(func=cmd_sql)

    # -- search --
    p_search = sub.add_parser("search", help="Full-text search entries")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--context", help="Filter by context")
    p_search.add_argument("--type", help="Filter by entry type")
    p_search.set_defaults(func=cmd_search)

    # -- schema --
    p_schema = sub.add_parser("schema", help="Print table schema and example queries")
    p_schema.set_defaults(func=cmd_schema)

    # -- rebuild --
    p_rebuild = sub.add_parser("rebuild", help="Rebuild SQLite index from JSONL files")
    p_rebuild.set_defaults(func=cmd_rebuild)

    # -- contexts --
    p_contexts = sub.add_parser("contexts", help="List active research contexts")
    p_contexts.set_defaults(func=cmd_contexts)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
