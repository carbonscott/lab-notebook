"""Lab notebook: append-only JSONL entries + SQLite query index.

Usage:
    lab-notebook init [path] [--template NAME]
    lab-notebook emit --context X --type Y "content"
    lab-notebook sql "SELECT ..."
    lab-notebook search "query"
    lab-notebook schema
    lab-notebook rebuild
    lab-notebook contexts
    lab-notebook template [name] [--force]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import secrets
import sqlite3
import sys
import tempfile
from collections import namedtuple
from datetime import datetime
from importlib.resources import files
from pathlib import Path

import yaml

CORE_FIELDS = ("id", "ts", "writer_id", "context", "type", "content")
BUILTIN_FIELDS = {"artifacts": {"type": "list"}}  # always present, nullable; merged into every schema
VALID_FIELD_TYPES = ("text", "integer", "real", "list")
TYPE_MAP = {"text": "TEXT", "integer": "INTEGER", "real": "REAL", "list": "TEXT"}

SchemaSQL = namedtuple("SchemaSQL", ["create", "upsert", "fts_insert", "fts_cols"])

EXAMPLE_QUERIES = """\
Example queries
---------------
-- Recent entries
SELECT ts, type, substr(content, 1, 80) FROM entries ORDER BY ts DESC LIMIT 10;

-- All decisions in a context
SELECT ts, substr(content, 1, 80) FROM entries
WHERE context = 'my/context' AND type = 'decision' ORDER BY ts;

-- Full-text search
SELECT e.ts, e.context, e.type, substr(e.content, 1, 80) FROM entries e
JOIN entries_fts f ON f.rowid = e.rowid
WHERE entries_fts MATCH 'search term';

-- Entries per context
SELECT context, COUNT(*) AS n, MIN(ts) AS first, MAX(ts) AS latest
FROM entries GROUP BY context ORDER BY latest DESC;
"""


def format_schema_help(schema: dict) -> str:
    fields = schema.get("fields", {})
    lines = ["Table: entries", "--------------"]
    core = [
        ("id", "TEXT PRIMARY KEY", "e.g. 20260321T143022-a7f2"),
        ("ts", "TEXT NOT NULL", "ISO 8601 local time"),
        ("writer_id", "TEXT NOT NULL", "e.g. cong, agent-claude-01"),
        ("context", "TEXT NOT NULL", "e.g. maxie/ssl-comparison"),
        ("type", "TEXT NOT NULL", "one of: " + ", ".join(schema["types"])),
        ("content", "TEXT NOT NULL", "free-text notebook prose"),
    ]
    for name, sqltype, desc in core:
        lines.append(f"  {name:<12} {sqltype:<18} -- {desc}")
    for name, spec in fields.items():
        sqltype = TYPE_MAP[spec["type"]]
        fts_note = " (fts)" if spec.get("fts") else ""
        label = "built-in" if name in BUILTIN_FIELDS else "schema field"
        lines.append(f"  {name:<12} {sqltype:<18} -- {label}{fts_note}")
    lines.append(f"  {'extra':<12} {'TEXT':<18} -- JSON blob for --extra fields")
    lines.append("")
    fts_cols = ["content"] + [n for n, s in fields.items() if s.get("fts")]
    lines.append(f"FTS table: entries_fts ({', '.join(fts_cols)})")
    return "\n".join(lines)


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
    fields = schema.get("fields") or {}
    schema["fields"] = fields
    reserved = set(CORE_FIELDS) | {"extra"}
    for name, spec in fields.items():
        if name in reserved:
            print(f"Error: field '{name}' conflicts with a core field.", file=sys.stderr)
            sys.exit(1)
        if not isinstance(spec, dict):
            print(f"Error: field '{name}' must be a mapping (e.g. {{type: text}}), "
                  f"got '{spec}'.", file=sys.stderr)
            sys.exit(1)
        if name in BUILTIN_FIELDS:
            print(f"Error: field '{name}' is built-in and cannot be redeclared in schema.",
                  file=sys.stderr)
            sys.exit(1)
        ftype = spec.get("type")
        if ftype not in VALID_FIELD_TYPES:
            print(f"Error: field '{name}' has invalid type '{ftype}'. "
                  f"Must be one of {VALID_FIELD_TYPES}.", file=sys.stderr)
            sys.exit(1)
    # Merge built-in fields (user redeclaration is rejected above)
    for name, spec in BUILTIN_FIELDS.items():
        if name not in fields:
            fields[name] = spec
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
        col_defs.append(f'"{name}" {TYPE_MAP[spec["type"]]}')
    col_defs.append("extra TEXT")

    fts_cols = ["content"]
    for name, spec in fields.items():
        if spec.get("fts"):
            fts_cols.append(name)

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS entries (\n    "
        + ",\n    ".join(col_defs)
        + "\n);\n"
        + "CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5("
        + ", ".join(f'"{c}"' for c in fts_cols)
        + ");\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_context ON entries(context);\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_ts ON entries(ts);\n"
    )

    # -- UPSERT --
    all_cols = list(CORE_FIELDS) + list(fields.keys()) + ["extra"]
    quoted_cols = ", ".join(f'"{c}"' for c in all_cols)
    placeholders = ", ".join(f":{c}" for c in all_cols)
    upsert_sql = (
        f"INSERT OR REPLACE INTO entries\n"
        f"    ({quoted_cols})\n"
        f"VALUES\n"
        f"    ({placeholders});\n"
    )

    # -- FTS INSERT --
    quoted_fts = ", ".join(f'"{c}"' for c in fts_cols)
    fts_placeholders = ", ".join(f":{c}" for c in fts_cols)
    fts_insert_sql = (
        f"INSERT INTO entries_fts (rowid, {quoted_fts})\n"
        f"VALUES (:rowid, {fts_placeholders});\n"
    )

    return SchemaSQL(create=create_sql, upsert=upsert_sql,
                     fts_insert=fts_insert_sql, fts_cols=fts_cols)


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
    # 1. Check for .lnb.env (walk up from CWD)
    env_file = _find_lnb_env()
    if env_file:
        val = _parse_lnb_env(env_file)
        if val:
            return Path(val)
    # 2. Fall back to environment variable
    d = os.environ.get("LAB_NOTEBOOK_DIR")
    if d:
        return Path(d)
    # 3. Error
    print("Error: LAB_NOTEBOOK_DIR is not set and no .lnb.env found.", file=sys.stderr)
    if hint:
        print(hint, file=sys.stderr)
    else:
        print("Run 'lab-notebook init' to set up a project notebook,\n"
              "or set $LAB_NOTEBOOK_DIR in your shell profile.",
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


def _index_is_stale(notebook_dir: Path, dbp: Path) -> bool:
    """True if any JSONL file or schema.yaml is newer than the index."""
    idx_mtime = dbp.stat().st_mtime
    schema_file = notebook_dir / "schema.yaml"
    if schema_file.exists() and schema_file.stat().st_mtime >= idx_mtime:
        return True
    edir = entries_dir(notebook_dir)
    if edir.exists():
        for f in edir.glob("*.jsonl"):
            if f.stat().st_mtime >= idx_mtime:
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
        count = rebuild_from_jsonl(conn, notebook_dir, schema, sql)
        conn.close()
        os.rename(tmp, str(dbp))
        return count
    except BaseException:
        os.unlink(tmp)
        raise


def ensure_db(notebook_dir: Path, schema: dict | None = None) -> tuple[sqlite3.Connection, dict, SchemaSQL]:
    if schema is None:
        schema = load_schema(notebook_dir)
    sql = build_sql(schema)
    dbp = index_path(notebook_dir)
    if not dbp.exists() or _index_is_stale(notebook_dir, dbp):
        count = _atomic_rebuild(notebook_dir, dbp, schema, sql)
        print(f"Index rebuilt: {count} entries", file=sys.stderr)
    conn = sqlite3.connect(str(dbp))
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
                upsert_entry(conn, flat, sql, commit=False)
                count += 1
    conn.commit()
    return count


def print_table(cursor: sqlite3.Cursor) -> None:
    rows = cursor.fetchall()
    if not rows:
        print("(no results)")
        return
    cols = [desc[0] for desc in cursor.description]
    writer = csv.writer(sys.stdout, delimiter="|", lineterminator="\n")
    writer.writerow(cols)
    for row in rows:
        writer.writerow(str(v) if v is not None else "" for v in row)
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


SCHEMAS_DIR = Path(str(files("lab_notebook").joinpath("schemas")))
DEFAULT_TEMPLATE = "research-notebook"


def list_templates() -> list[tuple[str, str]]:
    """Return [(name, description)] for each .yaml in the bundled schemas dir."""
    templates = []
    for p in sorted(SCHEMAS_DIR.glob("*.yaml")):
        name = p.stem
        desc = ""
        with open(p) as f:
            first = f.readline().strip()
            if first.startswith("#"):
                desc = first.removeprefix("# ").rstrip(".")
        templates.append((name, desc))
    return templates


def get_template_path(name: str) -> Path | None:
    """Return path to a bundled template, or None if not found."""
    p = SCHEMAS_DIR / f"{name}.yaml"
    if not p.resolve().is_relative_to(SCHEMAS_DIR.resolve()):
        return None
    return p if p.exists() else None


def read_template(name: str) -> str:
    """Read a bundled template by name, or exit with error."""
    p = get_template_path(name)
    if p is None:
        names = [t[0] for t in list_templates()]
        print(f"Error: unknown template '{name}'. Available: {', '.join(names)}",
              file=sys.stderr)
        sys.exit(1)
    return p.read_text()


def print_templates() -> None:
    """Print available templates to stdout."""
    if not SCHEMAS_DIR.is_dir():
        print("Error: schemas directory not found. Installation may be corrupt.",
              file=sys.stderr)
        sys.exit(1)
    templates = list_templates()
    if not templates:
        print("No templates found.")
        return
    print("Available templates:")
    for name, desc in templates:
        suffix = f" — {desc}" if desc else ""
        print(f"  {name}{suffix}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_init(args: argparse.Namespace) -> None:
    # --template with no value: list templates and exit
    if hasattr(args, "template") and args.template == "":
        print_templates()
        return

    target = Path(args.path or ".lnb").resolve()
    target.mkdir(parents=True, exist_ok=True)

    template_name = getattr(args, "template", None) or DEFAULT_TEMPLATE

    edir = target / "entries"
    edir.mkdir(exist_ok=True)
    (target / "artifacts").mkdir(exist_ok=True)

    gitignore = target / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("index.sqlite\n")
    elif "index.sqlite" not in gitignore.read_text():
        with open(gitignore, "a") as f:
            f.write("index.sqlite\n")

    sf = target / "schema.yaml"
    template_explicit = getattr(args, "template", None) is not None
    if not sf.exists():
        sf.write_text(read_template(template_name))
        schema_msg = f"from template: {template_name}"
    elif template_explicit:
        sf.write_text(read_template(template_name))
        schema_msg = f"from template: {template_name} (overwritten)"
    else:
        schema_msg = "already exists (kept)"

    writer = os.environ.get("USER", "unknown")

    # Write .lnb.env in CWD
    lnb_env = Path.cwd() / LNB_ENV_FILE
    if lnb_env.exists():
        print(f"Warning: overwriting existing {lnb_env}", file=sys.stderr)
    lnb_env.write_text(
        f"# Project-local lab-notebook configuration\n"
        f"export LAB_NOTEBOOK_DIR={target}\n"
        f"export LAB_NOTEBOOK_WRITER={writer}\n"
    )
    print(f"Initialized lab notebook in {target}")
    print(f"  entries/       per-writer JSONL files")
    print(f"  artifacts/     files referenced via --artifacts")
    print(f"  schema.yaml    {schema_msg}")
    print(f"  .gitignore     ignores index.sqlite")
    print(f"\nCreated {lnb_env.name} in {lnb_env.parent}")
    print(f"  LAB_NOTEBOOK_DIR={target}")
    print(f"\nConsider adding to .gitignore:")
    print(f"  {LNB_ENV_FILE}")
    print(f"  .lnb/")


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
    notebook_dir = get_notebook_dir()
    schema = load_schema(notebook_dir)
    print(format_schema_help(schema))
    print()
    print(EXAMPLE_QUERIES)


def cmd_rebuild(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    schema = load_schema(notebook_dir)
    sql = build_sql(schema)
    dbp = index_path(notebook_dir)
    count = _atomic_rebuild(notebook_dir, dbp, schema, sql)
    print(f"Rebuilt index: {count} entries from {entries_dir(notebook_dir)}")


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


def cmd_template(args: argparse.Namespace) -> None:
    if not args.name:
        print_templates()
        return

    notebook_dir = get_notebook_dir(
        hint="Source the notebook's .env file, or run 'lab-notebook init' first.")
    schema_content = read_template(args.name)
    sf = notebook_dir / "schema.yaml"
    if sf.exists() and not args.force:
        print(f"Error: {sf} already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)
    sf.write_text(schema_content)
    print(f"Applied template '{args.name}' to {sf}")

    has_entries = any((notebook_dir / "entries").glob("*.jsonl")) if (notebook_dir / "entries").exists() else False
    if has_entries:
        print("Run 'lab-notebook rebuild' to re-index existing entries.")


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
                        help="Notebook directory (default: .lnb in current directory)")
    p_init.add_argument("--template", nargs="?", const="", default=None,
                        help="Schema template to use (omit value to list available templates)")
    p_init.set_defaults(func=cmd_init)

    # -- emit --
    p_emit = sub.add_parser("emit", help="Write a notebook entry")
    p_emit.add_argument("--context", required=True, help="Research context (e.g. maxie/ssl-comparison)")
    p_emit.add_argument("--type", required=True, help="Entry type (defined in schema.yaml)")
    p_emit.add_argument("--artifacts", default=None,
                        help="Files referenced by this entry (comma-separated paths)")
    p_emit.add_argument("--extra", action="append", metavar="KEY=VALUE",
                        help="Extra undeclared field (repeatable)")
    p_emit.add_argument("content", help="Entry content (notebook prose)")

    # Dynamically add schema-defined fields if a notebook can be found.
    # Best-effort: don't crash arg parsing if the path is stale or schema is bad.
    _parsed_schema = None
    try:
        env_file = _find_lnb_env()
        notebook_env = (_parse_lnb_env(env_file) if env_file else None) or os.environ.get("LAB_NOTEBOOK_DIR")
        if notebook_env:
            nb_dir = Path(notebook_env)
            sf = nb_dir / "schema.yaml"
            if sf.exists():
                _parsed_schema = load_schema(nb_dir)
                for name, spec in _parsed_schema.get("fields", {}).items():
                    if name in BUILTIN_FIELDS:
                        continue  # already added as a static argument
                    ftype = spec.get("type", "text")
                    help_text = f"Schema field ({ftype})"
                    if ftype == "list":
                        help_text = f"Schema field ({ftype}, comma-separated)"
                    p_emit.add_argument(f"--{name}", default=None, help=help_text)
    except (SystemExit, Exception):
        pass  # schema loading failed — emit will load schema at runtime

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

    # -- template --
    p_template = sub.add_parser("template", help="List or apply schema templates")
    p_template.add_argument("name", nargs="?", default=None,
                            help="Template name to apply (omit to list available)")
    p_template.add_argument("--force", action="store_true",
                            help="Overwrite existing schema.yaml")
    p_template.set_defaults(func=cmd_template)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
