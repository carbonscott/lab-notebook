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
from collections import namedtuple
from datetime import datetime
from pathlib import Path

import yaml

CORE_FIELDS = ("id", "ts", "writer_id", "context", "type", "content")
VALID_FIELD_TYPES = ("text", "integer", "real", "list")
TYPE_MAP = {"text": "TEXT", "integer": "INTEGER", "real": "REAL", "list": "TEXT"}

SchemaSQL = namedtuple("SchemaSQL", ["create", "upsert", "fts_insert", "fts_cols"])

SCHEMA_HELP = """\
Table: entries
--------------
  id         TEXT PRIMARY KEY   -- e.g. 20260321T143022-a7f2
  ts         TEXT NOT NULL      -- ISO 8601 local time
  writer_id  TEXT NOT NULL      -- e.g. cong, agent-claude-01
  context    TEXT NOT NULL      -- e.g. maxie/ssl-comparison
  type       TEXT NOT NULL      -- defined in schema.yaml types list
  content    TEXT NOT NULL      -- free-text notebook prose
  extra      TEXT               -- JSON blob for undeclared --extra fields
  (+ custom fields from schema.yaml)

FTS table: entries_fts (content + fields with fts: true)

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
# Schema loading and SQL generation
# ---------------------------------------------------------------------------

def load_schema(notebook_dir: Path) -> dict:
    schema_file = notebook_dir / "schema.yaml"
    if not schema_file.exists():
        print(f"Error: {schema_file} not found. Run 'lab-notebook init' first.",
              file=sys.stderr)
        sys.exit(1)
    with open(schema_file) as f:
        schema = yaml.safe_load(f)
    if not isinstance(schema.get("types"), list) or not schema["types"]:
        print("Error: schema.yaml must have a non-empty 'types' list.", file=sys.stderr)
        sys.exit(1)
    fields = schema.get("fields", {})
    reserved = set(CORE_FIELDS) | {"extra"}
    for name, spec in fields.items():
        if name in reserved:
            print(f"Error: field '{name}' conflicts with a core field.", file=sys.stderr)
            sys.exit(1)
        ftype = spec.get("type")
        if ftype not in VALID_FIELD_TYPES:
            print(f"Error: field '{name}' has invalid type '{ftype}'. "
                  f"Must be one of {VALID_FIELD_TYPES}.", file=sys.stderr)
            sys.exit(1)
    return schema


def build_sql(schema: dict) -> SchemaSQL:
    fields = schema.get("fields", {})

    # -- CREATE TABLE --
    col_defs = [
        "id         TEXT PRIMARY KEY",
        "ts         TEXT NOT NULL",
        "writer_id  TEXT NOT NULL",
        "context    TEXT NOT NULL",
        "type       TEXT NOT NULL",
        "content    TEXT NOT NULL",
    ]
    for name, spec in fields.items():
        col_defs.append(f"{name} {TYPE_MAP[spec['type']]}")
    col_defs.append("extra TEXT")

    fts_cols = ["content"]
    for name, spec in fields.items():
        if spec.get("fts"):
            fts_cols.append(name)

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS entries (\n    "
        + ",\n    ".join(col_defs)
        + "\n);\n"
        + f"CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5({', '.join(fts_cols)});\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_context ON entries(context);\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_ts ON entries(ts);\n"
    )

    # -- UPSERT --
    all_cols = list(CORE_FIELDS) + list(fields.keys()) + ["extra"]
    placeholders = ", ".join(f":{c}" for c in all_cols)
    upsert_sql = (
        f"INSERT OR REPLACE INTO entries\n"
        f"    ({', '.join(all_cols)})\n"
        f"VALUES\n"
        f"    ({placeholders});\n"
    )

    # -- FTS INSERT --
    fts_placeholders = ", ".join(f":{c}" for c in fts_cols)
    fts_insert_sql = (
        f"INSERT INTO entries_fts (rowid, {', '.join(fts_cols)})\n"
        f"VALUES (:rowid, {fts_placeholders});\n"
    )

    return SchemaSQL(create=create_sql, upsert=upsert_sql,
                     fts_insert=fts_insert_sql, fts_cols=fts_cols)


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


def init_db(conn: sqlite3.Connection, create_sql: str) -> None:
    conn.executescript(create_sql)


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


def upsert_entry(conn: sqlite3.Connection, entry: dict, sql: SchemaSQL) -> None:
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
    conn.commit()


def ensure_db(notebook_dir: Path, schema: dict | None = None) -> tuple[sqlite3.Connection, dict, SchemaSQL]:
    if schema is None:
        schema = load_schema(notebook_dir)
    sql = build_sql(schema)
    dbp = index_path(notebook_dir)
    needs_rebuild = not dbp.exists()
    conn = sqlite3.connect(str(dbp))
    init_db(conn, sql.create)
    if needs_rebuild:
        rebuild_from_jsonl(conn, notebook_dir, schema, sql)
    return conn, schema, sql


def rebuild_from_jsonl(conn: sqlite3.Connection, notebook_dir: Path,
                       schema: dict, sql: SchemaSQL) -> int:
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
                flat = flatten_entry(entry, schema)
                conn.execute(sql.upsert, flat)
                row = conn.execute(
                    "SELECT rowid FROM entries WHERE id = ?", (flat["id"],)
                ).fetchone()
                if row:
                    fts_params = {"rowid": row[0]}
                    for col in sql.fts_cols:
                        fts_params[col] = flat.get(col, "")
                    conn.execute(sql.fts_insert, fts_params)
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


DEFAULT_SCHEMA_YAML = """\
types:
  - observation
  - decision
  - dead-end
  - question
  - milestone

fields:
  repo:       {type: text}
  branch:     {type: text}
  tags:       {type: list}
  artifacts:  {type: list}
"""


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

    sf = target / "schema.yaml"
    if not sf.exists():
        sf.write_text(DEFAULT_SCHEMA_YAML)

    writer = os.environ.get("USER", "unknown")
    env_file = target / ".env"
    env_file.write_text(
        f"export LAB_NOTEBOOK_DIR={target}\n"
        f"export LAB_NOTEBOOK_WRITER={writer}\n"
    )

    print(f"Initialized lab notebook in {target}")
    print(f"  entries/       per-writer JSONL files")
    print(f"  schema.yaml    field definitions")
    print(f"  .gitignore     ignores index.sqlite")
    print(f"  .env           LAB_NOTEBOOK_DIR={target}")
    print(f"                 LAB_NOTEBOOK_WRITER={writer}")
    print(f"\nNext: source {env_file}")


def cmd_emit(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    writer_id = get_writer_id()
    schema = getattr(args, "_schema", None) or load_schema(notebook_dir)

    if args.type not in schema["types"]:
        print(f"Error: type must be one of {schema['types']}, got '{args.type}'",
              file=sys.stderr)
        sys.exit(1)

    entry = {
        "id": generate_id(),
        "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "writer_id": writer_id,
        "context": args.context,
        "type": args.type,
        "content": args.content,
    }

    # Schema-defined fields
    fields = schema.get("fields", {})
    for name, spec in fields.items():
        val = getattr(args, name, None)
        if val is not None:
            if spec["type"] == "list":
                val = [v.strip() for v in val.split(",") if v.strip()]
            elif spec["type"] == "integer":
                val = int(val)
            elif spec["type"] == "real":
                val = float(val)
        entry[name] = val

    # --extra key=value pairs
    reserved_keys = set(CORE_FIELDS) | set(fields.keys()) | {"extra"}
    if args.extra:
        for item in args.extra:
            key, _, value = item.partition("=")
            if not key or not _:
                print(f"Error: --extra must be key=value, got '{item}'", file=sys.stderr)
                sys.exit(1)
            if key in reserved_keys:
                print(f"Error: --extra key '{key}' conflicts with a declared field. "
                      f"Use --{key} instead.", file=sys.stderr)
                sys.exit(1)
            entry[key] = value

    # Write JSONL
    edir = entries_dir(notebook_dir)
    edir.mkdir(exist_ok=True)
    writer_file = edir / f"{writer_id}.jsonl"
    with open(writer_file, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())

    # Update index
    flat = flatten_entry(entry, schema)
    conn, _, sql = ensure_db(notebook_dir, schema)
    try:
        upsert_entry(conn, flat, sql)
    finally:
        conn.close()

    print(f"[{entry['type']}] {entry['id']}  {entry['context']}")


def cmd_sql(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    conn, schema, sql = ensure_db(notebook_dir)
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
    conn, schema, sql = ensure_db(notebook_dir)
    try:
        query_sql = """SELECT e.ts, e.context, e.type, e.writer_id, substr(e.content, 1, 120)
                 FROM entries e
                 JOIN entries_fts f ON f.rowid = e.rowid
                 WHERE entries_fts MATCH :query"""
        params: dict = {"query": args.query}
        if args.context:
            query_sql += " AND e.context = :context"
            params["context"] = args.context
        if args.type:
            query_sql += " AND e.type = :type"
            params["type"] = args.type
        query_sql += " ORDER BY e.ts DESC"
        cursor = conn.execute(query_sql, params)
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
    schema = load_schema(notebook_dir)
    sql = build_sql(schema)
    dbp = index_path(notebook_dir)
    if dbp.exists():
        dbp.unlink()
    conn = sqlite3.connect(str(dbp))
    init_db(conn, sql.create)
    try:
        count = rebuild_from_jsonl(conn, notebook_dir, schema, sql)
        print(f"Rebuilt index: {count} entries from {entries_dir(notebook_dir)}")
    finally:
        conn.close()


def cmd_contexts(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    conn, schema, sql = ensure_db(notebook_dir)
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
    p_emit.add_argument("--type", required=True, help="Entry type (defined in schema.yaml)")
    p_emit.add_argument("--extra", action="append", metavar="KEY=VALUE",
                        help="Extra undeclared field (repeatable)")
    p_emit.add_argument("content", help="Entry content (notebook prose)")

    # Dynamically add schema-defined fields if LAB_NOTEBOOK_DIR is set
    _parsed_schema = None
    notebook_env = os.environ.get("LAB_NOTEBOOK_DIR")
    if notebook_env:
        nb_dir = Path(notebook_env)
        sf = nb_dir / "schema.yaml"
        if sf.exists():
            _parsed_schema = load_schema(nb_dir)
            for name, spec in _parsed_schema.get("fields", {}).items():
                ftype = spec.get("type", "text")
                help_text = f"Schema field ({ftype})"
                if ftype == "list":
                    help_text = f"Schema field ({ftype}, comma-separated)"
                p_emit.add_argument(f"--{name}", default=None, help=help_text)

    p_emit.set_defaults(func=cmd_emit, _schema=_parsed_schema)

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
