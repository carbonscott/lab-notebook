"""Shell tab-completion backend for the `lab-notebook` CLI.

A hidden `__complete` subcommand (see cli.py) prints newline-delimited
candidates that a thin bash script feeds to `compgen` at Tab-time. All the
knowledge lives here; the shell stays dumb.

Direction stays one-way: schema <- store <- complete <- cli. This module
depends only on `schema` (for `load_schema`) and `store` (for
`get_notebook_dir` / `open_readonly`).

Everything resolves LIVE from the target notebook (`schema.yaml` +
`index.sqlite`), never from the bundled templates. All reads are strictly
read-only: `open_readonly` never rebuilds or ingests, so tab-completion has no
side effects and tolerates a stale index. Every resolver swallows `LnbError`
and SQLite errors, so completion emits nothing rather than raising when the
notebook is unresolved, the schema is missing, or the index does not exist yet.
"""
from __future__ import annotations

import json
import sqlite3

from .schema import LnbError, load_schema
from .store import get_notebook_dir, open_readonly

# The subcommands a user can complete: the ten registered names plus
# `completion` (the install helper). `__complete` is deliberately excluded — it
# is an internal RPC target, not something a user types.
SUBCOMMANDS = [
    "init", "emit", "retract", "show", "sql", "search",
    "schema", "rebuild", "contexts", "template", "completion",
]

# Per-subcommand flag names, offered in the flag-name slot (`<subcmd> -<TAB>`).
FLAGS = {
    "init": ["--template", "--template-path", "--force"],
    "emit": ["--context", "--type", "--artifacts", "-f", "--field", "--extra"],
    "retract": ["--reason"],
    "search": ["--context", "--type"],
    "template": ["--force"],
}


# ---------------------------------------------------------------------------
# Live resolvers (all side-effect-free; degrade to []/None on any error)
# ---------------------------------------------------------------------------

def _dir():
    """Resolve the target notebook dir, or None if it cannot be resolved."""
    try:
        return get_notebook_dir()
    except LnbError:
        return None


def _schema():
    """Load the target schema, or None if the dir/schema cannot be resolved."""
    d = _dir()
    if d is None:
        return None
    try:
        return load_schema(d)
    except LnbError:
        return None


def _types():
    schema = _schema()
    if schema is None:
        return []
    return [str(t) for t in schema["types"]]


def _field_keys():
    # Each key ends with "=" so the value can be typed immediately after Tab.
    # `schema["fields"]` already includes the built-in `artifacts`.
    schema = _schema()
    if schema is None:
        return []
    return [f"{k}=" for k in schema["fields"]]


def _contexts():
    d = _dir()
    if d is None:
        return []
    conn = open_readonly(d)
    if conn is None:
        return []
    try:
        rows = conn.execute(
            "SELECT DISTINCT context FROM entries "
            "WHERE context IS NOT NULL ORDER BY 1"
        ).fetchall()
        return [str(r[0]) for r in rows]
    except sqlite3.Error:
        # Any DB-layer error (missing table on a partial index, or a corrupt
        # file raising the OperationalError parent DatabaseError) → offer
        # nothing. A Tab press must never surface a traceback.
        return []
    finally:
        conn.close()


def _field_values(field):
    """Distinct existing values for `field` from the index.

    `field` must be a declared schema field — the check guards the double-quoted
    column interpolation below (the same interpolation build_sql uses to create
    the column, so a name that made it into the table is safe here). For a
    `list` field, values are stored as JSON arrays; we json.loads and flatten in
    Python (no json_each dependency). Any other type reads distinct raw values.
    """
    d = _dir()
    if d is None:
        return []
    try:
        schema = load_schema(d)
    except LnbError:
        return []
    fields = schema["fields"]
    if field not in fields:
        return []
    conn = open_readonly(d)
    if conn is None:
        return []
    try:
        col = f'"{field}"'
        if fields[field]["type"] == "list":
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM entries WHERE {col} IS NOT NULL"
            ).fetchall()
            values: set[str] = set()
            for (raw,) in rows:
                try:
                    parsed = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(parsed, list):
                    values.update(str(v) for v in parsed)
                else:
                    values.add(str(parsed))
            return sorted(values)
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM entries "
            f"WHERE {col} IS NOT NULL ORDER BY 1"
        ).fetchall()
        return [str(r[0]) for r in rows]
    except sqlite3.Error:
        # See _contexts: swallow any DB-layer error (incl. the corrupt-file
        # DatabaseError, which is OperationalError's parent) so Tab stays quiet.
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Slot logic
# ---------------------------------------------------------------------------

def complete(words: list[str], cword: int) -> list[str]:
    """Return candidates for the word at index `cword`.

    `words[0]` is the program name (`lab-notebook`); `cword` indexes the word
    being completed. Never raises: any out-of-range `cword`, unresolvable
    notebook, missing schema, or missing index all yield []. (The real bash
    driver always passes an in-range COMP_CWORD, but this stays total so a
    malformed `__complete` argv degrades to silence rather than a traceback.)
    """
    if cword <= 1:
        return SUBCOMMANDS
    if len(words) < 2:
        return []              # cword past the end with no subcommand typed yet
    sub = words[1]
    sub_flags = FLAGS.get(sub, [])
    # Bounds-guard cur/prev independently — cword may point past the end (a
    # trailing-space completion) or, defensively, anywhere.
    cur = words[cword] if 0 <= cword < len(words) else ""
    prev = words[cword - 1] if 0 <= cword - 1 < len(words) else ""

    # --- value slots first (before flag-name) ---
    # Each value slot is gated on the triggering flag actually belonging to
    # `sub`, matching the subcommand-scoped flag-name slot below — so a
    # flag/subcommand mismatch (e.g. `sql --type <TAB>`) offers nothing.
    #
    # `-f <field> = <cur>`: '=' is a bash COMP_WORDBREAK, so key/=/value are
    # separate words. Two shapes: prev=='=' (value being typed) or cur=='='
    # (empty value right after `key=`).
    has_field = "-f" in sub_flags or "--field" in sub_flags
    if has_field and prev == "=" and cword >= 3 and words[cword - 3] in ("-f", "--field"):
        return _field_values(words[cword - 2])
    if has_field and cur == "=" and cword >= 2 and words[cword - 2] in ("-f", "--field"):
        return _field_values(words[cword - 1])

    if prev == "--type" and "--type" in sub_flags:
        return _types()
    if prev == "--context" and "--context" in sub_flags:
        return _contexts()
    if prev in ("-f", "--field") and has_field:
        return _field_keys()   # each ends with "="

    # --- flag-name slot ---
    if cur.startswith("-"):
        return sub_flags
    return []
