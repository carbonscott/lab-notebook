"""Lab notebook: append-only JSONL entries + SQLite query index.

Usage:
    lab-notebook init [path] [--template NAME]
    lab-notebook emit --context X --type Y "content"
    lab-notebook retract ID --reason "why"
    lab-notebook show [ID]
    lab-notebook sql "SELECT ..."
    lab-notebook search "query"
    lab-notebook schema
    lab-notebook rebuild
    lab-notebook contexts
    lab-notebook template [name] [--force]
    lab-notebook completion bash
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from pathlib import Path

from .schema import (
    DEFAULT_TEMPLATE,
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
    Notebook,
    _atomic_rebuild,
    entries_dir,
    get_notebook_dir,
    index_path,
)
from .complete import complete

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

# Bash registration script printed by `lab-notebook completion bash`. Install
# with `source <(lab-notebook completion bash)`. The function shells out to the
# hidden `__complete` subcommand at Tab-time; all completion knowledge lives in
# Python (see complete.py), the shell stays dumb.
#
# Candidates are read one-per-line straight into COMPREPLY and are NEVER routed
# through `compgen -W`. `compgen -W` re-expands every word in its list — command
# substitution, backticks, globbing — so a candidate drawn from untrusted
# notebook data (a context, tag, or field value like `$(rm -rf ~)` or `*`) would
# be executed or glob-expanded the instant a user pressed Tab. Instead, a
# `while read` loop assigns each line literally and does the prefix match in the
# shell with a quoted pattern (`"$cur"*`), so every candidate stays inert text.
# `read` + process substitution also keeps this working on bash 3.2 (no
# `mapfile`).
#
# '=' is in bash's default COMP_WORDBREAKS, so `-f repo=ma` tokenizes to
# `… -f repo = ma`: Python keys off the '=' token to find the field, and the
# in-shell prefix match completes only the post-'=' word (`ma`→`mae`), leaving
# `repo=` intact. The `cur=="="` remap covers the empty-value case
# (`-f repo=<TAB>`). Passing `--` before the words makes argparse treat
# flag-like tokens (`--type`, `-f`) as positionals rather than options.
BASH_COMPLETION_SCRIPT = r"""_lab_notebook_complete() {
    local cur="${COMP_WORDS[COMP_CWORD]}"
    [[ "$cur" == "=" ]] && cur=""          # '=' wordbreak: empty value after key=
    local line
    COMPREPLY=()
    while IFS= read -r line; do
        [[ "$line" == "$cur"* ]] && COMPREPLY+=("$line")
    done < <(lab-notebook __complete "$COMP_CWORD" -- "${COMP_WORDS[@]}" 2>/dev/null)
    # Field-key candidates end with '=' — keep the cursor attached so the value
    # can be typed immediately. Only suppress the trailing space in that case.
    if [[ ${#COMPREPLY[@]} -gt 0 && "${COMPREPLY[0]}" == *= ]]; then
        compopt -o nospace
    fi
}
complete -F _lab_notebook_complete lab-notebook
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

    # Refuse to clobber an existing .lnb.env unless --force (matches the
    # `template` command's convention). Checked up front so we fail before
    # creating any notebook directory.
    lnb_env = Path.cwd() / LNB_ENV_FILE
    if lnb_env.exists() and not getattr(args, "force", False):
        raise LnbError(
            f"Error: {lnb_env} already exists. Use --force to overwrite."
        )

    # Literal path semantics: no arg -> ./.lnb; an explicit path is used as-is
    # (no .lnb appended).
    target = (Path(".") / ".lnb" if args.path is None else Path(args.path)).resolve()
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

    # Write .lnb.env in CWD (existence guarded above; --force overwrites).
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
    nb = Notebook(get_notebook_dir())

    # Schema-field values come from the static --artifacts built-in plus the
    # repeatable -f/--field KEY=VALUE flag. Names are validated (and values
    # coerced) inside Notebook.emit — unknown field -> LnbError suggesting
    # --extra, bad int/real -> LnbError.
    fields: dict = {}
    if args.artifacts is not None:
        fields["artifacts"] = args.artifacts
    for item in (args.field or []):
        key, sep, value = item.partition("=")
        if not key or not sep:
            raise LnbError(f"Error: -f/--field must be KEY=VALUE, got '{item}'")
        fields[key] = value

    # Parse repeatable --extra KEY=VALUE strings into a dict; collision checks
    # against declared fields are enforced by Notebook.emit.
    extra: dict = {}
    if args.extra:
        for item in args.extra:
            key, sep, value = item.partition("=")
            if not key or not sep:
                raise LnbError(f"Error: --extra must be key=value, got '{item}'")
            extra[key] = value

    entry = nb.emit(args.context, args.type, args.content,
                    fields=fields, extra=extra)
    print(f"[{entry['type']}] {entry['id']}  {entry['context']}")


def cmd_retract(args: argparse.Namespace) -> None:
    nb = Notebook(get_notebook_dir())
    try:
        nb.retract(args.id, args.reason)
    finally:
        nb.close()
    print(f"[retracted] {args.id}  ({args.reason})")


def cmd_show(args: argparse.Namespace) -> None:
    # No id: list every live entry as a compact table, newest first. Retracted
    # entries are already absent from the index, so the listing is live-only.
    # The `AS content` alias makes the header read `content` rather than the
    # raw `substr(...)` expression; the `rowid DESC` tiebreak keeps ordering
    # stable (newest last-ingested first) when two entries share a timestamp.
    if args.id is None:
        nb = Notebook(get_notebook_dir())
        try:
            cursor = nb.query(
                "SELECT id, ts, context, type, substr(content, 1, 80) AS content "
                "FROM entries ORDER BY ts DESC, rowid DESC"
            )
            print_table(cursor)
        except sqlite3.OperationalError as e:
            raise LnbError(f"SQL error: {e}")
        finally:
            nb.close()
        return

    nb = Notebook(get_notebook_dir())
    try:
        entry = nb.get(args.id)
    finally:
        nb.close()

    # `extra` is stored as a JSON blob; decode it so its keys print like any
    # other field. Core and schema columns print first (in table order), then
    # the decoded --extra keys. None values render as blank.
    raw_extra = entry.pop("extra", None)
    extra = json.loads(raw_extra) if raw_extra else {}

    width = max((len(k) for k in list(entry) + list(extra)), default=1)
    for key, val in entry.items():
        print(f"{key:<{width}}  {'' if val is None else val}")
    for key, val in extra.items():
        print(f"{key:<{width}}  {val}")


def cmd_sql(args: argparse.Namespace) -> None:
    nb = Notebook(get_notebook_dir())
    try:
        cursor = nb.query(args.query)
        print_table(cursor)
    except sqlite3.OperationalError as e:
        raise LnbError(f"SQL error: {e}")
    finally:
        nb.close()


def cmd_search(args: argparse.Namespace) -> None:
    nb = Notebook(get_notebook_dir())
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
        cursor = nb.query(query_sql, params)
        print_table(cursor)
    except sqlite3.OperationalError as e:
        raise LnbError(f"Search error: {e}")
    finally:
        nb.close()


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
    nb = Notebook(get_notebook_dir())
    try:
        cursor = nb.query("""\
            SELECT context, COUNT(*) AS entries, MIN(ts) AS first, MAX(ts) AS latest
            FROM entries GROUP BY context ORDER BY latest DESC
        """)
        print_table(cursor)
    except sqlite3.OperationalError as e:
        raise LnbError(f"SQL error: {e}")
    finally:
        nb.close()


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


def cmd_completion(args: argparse.Namespace) -> None:
    # args.shell is constrained to "bash" by argparse choices.
    print(BASH_COMPLETION_SCRIPT, end="")


def cmd___complete(args: argparse.Namespace) -> None:
    # Internal RPC target for the shell function. Must never raise on odd input
    # (a Tab press should never spew a traceback), so swallow everything and
    # emit nothing on failure. complete() already returns [] for the expected
    # degradation paths; this also guards genuinely malformed argv.
    try:
        for candidate in complete(args.words, args.cword):
            print(candidate)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
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
    p_init.add_argument("--force", action="store_true",
                        help="Overwrite an existing .lnb.env in the current directory")
    p_init.set_defaults(func=cmd_init)

    # -- emit --
    p_emit = sub.add_parser("emit", help="Write a notebook entry")
    p_emit.add_argument("--context", required=True, help="Research context (e.g. maxie/ssl-comparison)")
    p_emit.add_argument("--type", required=True, help="Entry type (defined in schema.yaml)")
    p_emit.add_argument("--artifacts", default=None,
                        help="Files referenced by this entry (comma-separated paths)")
    p_emit.add_argument("-f", "--field", action="append", metavar="KEY=VALUE",
                        help="Schema field value (repeatable; e.g. -f repo=foo -f tags=a,b). "
                             "Unknown names are rejected — use --extra for undeclared fields.")
    p_emit.add_argument("--extra", action="append", metavar="KEY=VALUE",
                        help="Extra undeclared field (repeatable)")
    p_emit.add_argument("content", help="Entry content (notebook prose)")
    # No schema loading here: the emit parser is static, so --help is identical
    # regardless of the working directory or LAB_NOTEBOOK_DIR. Schema fields are
    # passed with -f/--field and validated at runtime in Notebook.emit.
    p_emit.set_defaults(func=cmd_emit)

    # -- retract --
    p_retract = sub.add_parser(
        "retract",
        help="Retract an entry by id (appends a tombstone; entry stays in JSONL)")
    p_retract.add_argument("id", help="Id of the entry to retract")
    p_retract.add_argument("--reason", required=True,
                           help="Why this entry is being retracted (recorded in the tombstone)")
    p_retract.set_defaults(func=cmd_retract)

    # -- show --
    p_show = sub.add_parser(
        "show", help="Print one entry in full by id, or list all entries if no id is given")
    p_show.add_argument(
        "id", nargs="?", default=None,
        help="Id of the entry to show; omit to list every entry newest-first")
    p_show.set_defaults(func=cmd_show)

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

    # -- completion -- (prints a shell registration script)
    p_completion = sub.add_parser("completion", help="Print a shell completion script")
    p_completion.add_argument("shell", choices=["bash"])
    p_completion.set_defaults(func=cmd_completion)

    # -- __complete -- (internal RPC target for the shell function; no help= so
    # it carries no description in --help. The name still appears in the usage
    # metavar — argparse has no clean way to drop one choice from it — but users
    # are not meant to invoke it directly.)
    p_complete = sub.add_parser("__complete")
    p_complete.add_argument("cword", type=int)
    p_complete.add_argument("words", nargs="*")
    p_complete.set_defaults(func=cmd___complete)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except LnbError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
