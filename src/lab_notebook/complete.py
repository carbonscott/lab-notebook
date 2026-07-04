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
    except sqlite3.OperationalError:
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
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Slot logic
# ---------------------------------------------------------------------------

def complete(words: list[str], cword: int) -> list[str]:
    """Return candidates for the word at index `cword`.

    `words[0]` is the program name (`lab-notebook`); `cword` indexes the word
    being completed. Never raises: unresolvable notebook / missing schema /
    missing index all yield [].
    """
    if cword <= 1:
        return SUBCOMMANDS
    sub = words[1]
    cur = words[cword] if cword < len(words) else ""
    prev = words[cword - 1] if cword >= 1 else ""

    # --- value slots first (before flag-name) ---
    # `-f <field> = <cur>`: '=' is a bash COMP_WORDBREAK, so key/=/value are
    # separate words. Two shapes: prev=='=' (value being typed) or cur=='='
    # (empty value right after `key=`).
    if prev == "=" and cword >= 3 and words[cword - 3] in ("-f", "--field"):
        return _field_values(words[cword - 2])
    if cur == "=" and cword >= 2 and words[cword - 2] in ("-f", "--field"):
        return _field_values(words[cword - 1])

    if prev == "--type":
        return _types()
    if prev == "--context":
        return _contexts()
    if prev in ("-f", "--field"):
        return _field_keys()   # each ends with "="

    # --- flag-name slot ---
    if cur.startswith("-"):
        return FLAGS.get(sub, [])
    return []
