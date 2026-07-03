"""Schema loading, SQL generation, and bundled template helpers.

This is the lowest module: it imports no sibling module. `store` and `cli`
depend on it, never the other way around.
"""
from __future__ import annotations

from collections import namedtuple
from importlib.resources import files
from pathlib import Path

import yaml


class LnbError(Exception):
    """User-facing error. main() prints str(e) to stderr and exits 1."""


CORE_FIELDS = ("id", "ts", "writer_id", "context", "type", "content")
RETRACT_TYPE = "_retract"  # control-record type: tombstones a target entry, never stored as a row
BUILTIN_FIELDS = {"artifacts": {"type": "list"}}  # always present, nullable; merged into every schema
VALID_FIELD_TYPES = ("text", "integer", "real", "list")
TYPE_MAP = {"text": "TEXT", "integer": "INTEGER", "real": "REAL", "list": "TEXT"}

# Index layout version, stamped into the SQLite index via PRAGMA user_version.
# ensure_db forces a full rebuild when an existing index's version differs, so
# bumping this transparently migrates old indexes (they are disposable).
#   1 = standalone entries_fts, manual sync in upsert_entry/delete_entry
#   2 = external-content entries_fts kept in sync by triggers (US-004)
INDEX_USER_VERSION = 2

SchemaSQL = namedtuple("SchemaSQL", ["create", "upsert", "fts_cols"])


def format_schema_help(schema: dict) -> str:
    fields = schema.get("fields", {})
    lines = ["Table: entries", "--------------"]
    core = [
        ("id", "TEXT PRIMARY KEY", "e.g. 20260321T143022-a7f2c3d1"),
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
        raise LnbError(f"Error: {schema_file} not found. Run 'lab-notebook init' first.")
    with open(schema_file) as f:
        schema = yaml.safe_load(f)
    if not isinstance(schema.get("types"), list) or not schema["types"]:
        raise LnbError("Error: schema.yaml must have a non-empty 'types' list.")
    if RETRACT_TYPE in schema["types"]:
        raise LnbError(f"Error: '{RETRACT_TYPE}' is a reserved control-record type and "
                       f"cannot be declared in schema.yaml.")
    fields = schema.get("fields") or {}
    schema["fields"] = fields
    reserved = set(CORE_FIELDS) | {"extra"}
    for name, spec in fields.items():
        if name in reserved:
            raise LnbError(f"Error: field '{name}' conflicts with a core field.")
        if not isinstance(spec, dict):
            raise LnbError(f"Error: field '{name}' must be a mapping (e.g. {{type: text}}), "
                           f"got '{spec}'.")
        if name in BUILTIN_FIELDS:
            raise LnbError(f"Error: field '{name}' is built-in and cannot be redeclared in schema.")
        ftype = spec.get("type")
        if ftype not in VALID_FIELD_TYPES:
            raise LnbError(f"Error: field '{name}' has invalid type '{ftype}'. "
                           f"Must be one of {VALID_FIELD_TYPES}.")
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

    # entries_fts is an external-content FTS5 table: it stores no copy of the
    # text itself, only the inverted index, and reads column values back from
    # `entries` by rowid. Triggers keep the index in sync — AFTER INSERT adds
    # postings, AFTER DELETE removes them via the fts5 'delete' command (which
    # needs the *old* column values to locate the tokens), and AFTER UPDATE
    # does both. INSERT OR REPLACE in upsert_entry fires delete+insert; a plain
    # DELETE in delete_entry fires the delete trigger. (For REPLACE's implicit
    # delete to fire its trigger, the ingest connection must run with
    # PRAGMA recursive_triggers = ON — see store.py.)
    fts_col_list = ", ".join(f'"{c}"' for c in fts_cols)
    new_vals = ", ".join(f'new."{c}"' for c in fts_cols)
    old_vals = ", ".join(f'old."{c}"' for c in fts_cols)

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS entries (\n    "
        + ",\n    ".join(col_defs)
        + "\n);\n"
        + "CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5("
        + f"{fts_col_list}, content='entries', content_rowid='rowid');\n"
        + "CREATE TRIGGER IF NOT EXISTS entries_ai AFTER INSERT ON entries BEGIN\n"
        + f"    INSERT INTO entries_fts(rowid, {fts_col_list}) "
        + f"VALUES (new.rowid, {new_vals});\n"
        + "END;\n"
        + "CREATE TRIGGER IF NOT EXISTS entries_ad AFTER DELETE ON entries BEGIN\n"
        + f"    INSERT INTO entries_fts(entries_fts, rowid, {fts_col_list}) "
        + f"VALUES ('delete', old.rowid, {old_vals});\n"
        + "END;\n"
        + "CREATE TRIGGER IF NOT EXISTS entries_au AFTER UPDATE ON entries BEGIN\n"
        + f"    INSERT INTO entries_fts(entries_fts, rowid, {fts_col_list}) "
        + f"VALUES ('delete', old.rowid, {old_vals});\n"
        + f"    INSERT INTO entries_fts(rowid, {fts_col_list}) "
        + f"VALUES (new.rowid, {new_vals});\n"
        + "END;\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_context ON entries(context);\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);\n"
        + "CREATE INDEX IF NOT EXISTS idx_entries_ts ON entries(ts);\n"
        + "CREATE TABLE IF NOT EXISTS _ingest_state ("
        + "file TEXT PRIMARY KEY, offset INTEGER NOT NULL);\n"
        + f"PRAGMA user_version = {INDEX_USER_VERSION};\n"
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

    return SchemaSQL(create=create_sql, upsert=upsert_sql, fts_cols=fts_cols)


# ---------------------------------------------------------------------------
# Bundled schema templates
# ---------------------------------------------------------------------------

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
    """Read a bundled template by name, or raise LnbError."""
    p = get_template_path(name)
    if p is None:
        names = [t[0] for t in list_templates()]
        raise LnbError(f"Error: unknown template '{name}'. Available: {', '.join(names)}")
    return p.read_text()


def read_template_from_path(path: str) -> str:
    """Read a schema template from an external file path, or raise LnbError."""
    p = Path(path)
    if not p.is_file():
        raise LnbError(f"Error: template path not found or not a file: {path}")
    try:
        return p.read_text()
    except OSError as e:
        raise LnbError(f"Error: failed to read template at {path}: {e}")


def print_templates() -> None:
    """Print available templates to stdout."""
    if not SCHEMAS_DIR.is_dir():
        raise LnbError("Error: schemas directory not found. Installation may be corrupt.")
    templates = list_templates()
    if not templates:
        print("No templates found.")
        return
    print("Available templates:")
    for name, desc in templates:
        suffix = f" — {desc}" if desc else ""
        print(f"  {name}{suffix}")
