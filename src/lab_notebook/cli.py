"""Lab notebook: append-only JSONL entries + SQLite query index.

Usage:
    lab-notebook init [path] [--template NAME]
    lab-notebook emit --context X --type Y "content"
    lab-notebook retract ID --reason "why"
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
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from .schema import (
    BUILTIN_FIELDS,
    CORE_FIELDS,
    DEFAULT_TEMPLATE,
    RETRACT_TYPE,
    LnbError,
    build_sql,
    format_schema_help,
    load_schema,
    print_templates,
    read_template,
    read_template_from_path,
)
from .store import (
    LNB_ENV_FILE,
    _atomic_rebuild,
    _find_lnb_env,
    _parse_lnb_env,
    ensure_db,
    entries_dir,
    generate_id,
    get_notebook_dir,
    get_writer_id,
    index_path,
)

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


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_init(args: argparse.Namespace) -> None:
    # --template with no value: list templates and exit
    if hasattr(args, "template") and args.template == "":
        print_templates()
        return

    target = (Path(args.path or ".") / ".lnb").resolve()
    target.mkdir(parents=True, exist_ok=True)

    template_path = getattr(args, "template_path", None)
    template_name = getattr(args, "template", None)

    if template_path is not None:
        schema_text = read_template_from_path(template_path)
        schema_source = f"from path: {template_path}"
        explicit = True
    elif template_name is not None:
        schema_text = read_template(template_name)
        schema_source = f"from template: {template_name}"
        explicit = True
    else:
        schema_text = read_template(DEFAULT_TEMPLATE)
        schema_source = f"from template: {DEFAULT_TEMPLATE}"
        explicit = False

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
    if not sf.exists():
        sf.write_text(schema_text)
        schema_msg = schema_source
    elif explicit:
        sf.write_text(schema_text)
        schema_msg = f"{schema_source} (overwritten)"
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
    print(f"  {target.name}/")


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


def cmd_retract(args: argparse.Namespace) -> None:
    notebook_dir = get_notebook_dir()
    writer_id = get_writer_id()

    # Confirm the target exists in the current index. A missing id means it was
    # never written or has already been retracted — either way, nothing to do.
    conn, schema, sql = ensure_db(notebook_dir)
    try:
        found = conn.execute(
            "SELECT id FROM entries WHERE id = ?", (args.id,)
        ).fetchone()
    finally:
        conn.close()
    if not found:
        print(f"Error: entry '{args.id}' not found "
              f"(already retracted or never existed)", file=sys.stderr)
        sys.exit(1)

    tombstone = {
        "id": generate_id(),
        "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "writer_id": writer_id,
        "type": RETRACT_TYPE,
        "retracts": args.id,
        "reason": args.reason,
    }

    # Append the tombstone to the retracting writer's own file. The deletion is
    # applied on the next indexed read, the same way emit indexes lazily.
    edir = entries_dir(notebook_dir)
    edir.mkdir(exist_ok=True)
    writer_file = edir / f"{writer_id}.jsonl"
    with open(writer_file, "a") as f:
        f.write(json.dumps(tombstone, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())

    print(f"[retracted] {args.id}  ({args.reason})")


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
                        help="Notebook root directory (default: .lnb in current directory)")
    tgroup = p_init.add_mutually_exclusive_group()
    tgroup.add_argument("--template", nargs="?", const="", default=None,
                        help="Bundled schema template (omit value to list available templates)")
    tgroup.add_argument("--template-path", default=None, metavar="PATH",
                        help="Load schema from a YAML file on disk")
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
        notebook_env = os.environ.get("LAB_NOTEBOOK_DIR")
        if not notebook_env:
            env_file = _find_lnb_env()
            notebook_env = _parse_lnb_env(env_file) if env_file else None
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

    # -- retract --
    p_retract = sub.add_parser(
        "retract",
        help="Retract an entry by id (appends a tombstone; entry stays in JSONL)")
    p_retract.add_argument("id", help="Id of the entry to retract")
    p_retract.add_argument("--reason", required=True,
                           help="Why this entry is being retracted (recorded in the tombstone)")
    p_retract.set_defaults(func=cmd_retract)

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
    try:
        args.func(args)
    except LnbError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
