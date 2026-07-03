"""Tests for lab-notebook CLI."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from lab_notebook.schema import (
    INDEX_USER_VERSION,
    LnbError,
    build_sql,
    get_template_path,
    list_templates,
    load_schema,
    print_templates,
    read_template,
    read_template_from_path,
)
from lab_notebook.store import (
    LNB_ENV_FILE,
    Notebook,
    _find_lnb_env,
    _index_user_version,
    _parse_lnb_env,
    ensure_db,
    entries_dir,
    flatten_entry,
    generate_id,
    get_notebook_dir,
    index_path,
)
from lab_notebook.cli import (
    cmd_contexts,
    cmd_emit,
    cmd_init,
    cmd_rebuild,
    cmd_retract,
    cmd_search,
    cmd_schema,
    cmd_show,
    cmd_sql,
    cmd_template,
    main,
)


@pytest.fixture()
def notebook(tmp_path, monkeypatch):
    """Initialize a notebook in a temp directory and set env vars."""
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
    cmd_init(args)
    nb_dir = tmp_path / "nb"
    monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(nb_dir))
    monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "test-writer")
    return nb_dir


@pytest.fixture()
def custom_notebook(tmp_path, monkeypatch):
    """Initialize a notebook with a custom schema."""
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
    cmd_init(args)
    nb_dir = tmp_path / "nb"
    # Overwrite schema.yaml with custom fields
    (nb_dir / "schema.yaml").write_text(
        "types:\n"
        "  - observation\n"
        "  - result\n"
        "  - dead-end\n"
        "\n"
        "fields:\n"
        "  dataset:   {type: text, fts: true}\n"
        "  gpu_hours: {type: real}\n"
        "  num_nodes: {type: integer}\n"
        "  tags:      {type: list}\n"
    )
    monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(nb_dir))
    monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "test-writer")
    return nb_dir


def make_emit_args(**kwargs):
    """Build an emit Namespace mirroring the argparse surface.

    Schema fields are now passed via the repeatable -f/--field flag rather than
    dynamic per-field flags, so any kwarg that is not a core/static emit arg is
    folded into the ``field`` list as ``KEY=VALUE``. Pass ``field=[...]``
    directly to exercise malformed or explicit -f input.
    """
    static = {
        "context": "test/context",
        "type": "observation",
        "artifacts": None,
        "extra": None,
        "field": None,
        "content": "Test content",
    }
    field = list(kwargs.pop("field", None) or [])
    for key in [k for k in kwargs if k not in static]:
        val = kwargs.pop(key)
        if val is not None:
            field.append(f"{key}={val}")
    static.update(kwargs)
    static["field"] = field or None
    return argparse.Namespace(**static)


# Custom-schema notebooks use different field names (dataset, gpu_hours, ...),
# but the -f folding above is field-name agnostic, so the builder is identical.
make_custom_emit_args = make_emit_args


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestInit:
    def test_creates_structure(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)

        nb_dir = tmp_path / "nb"
        assert nb_dir.is_dir()
        assert (nb_dir / "entries").is_dir()
        assert (nb_dir / "artifacts").is_dir()
        assert (nb_dir / ".gitignore").exists()
        assert "index.sqlite" in (nb_dir / ".gitignore").read_text()
        # .lnb.env written in CWD
        lnb_env = tmp_path / LNB_ENV_FILE
        assert lnb_env.exists()
        env_text = lnb_env.read_text()
        assert f"LAB_NOTEBOOK_DIR={nb_dir}" in env_text
        assert "LAB_NOTEBOOK_WRITER=" in env_text

    def test_init_creates_schema_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)

        sf = tmp_path / "nb" / "schema.yaml"
        assert sf.exists()
        import yaml
        schema = yaml.safe_load(sf.read_text())
        assert "types" in schema
        assert "observation" in schema["types"]
        assert "fields" in schema
        assert "repo" in schema["fields"]

    def test_init_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=None, template=None)
        cmd_init(args)

        # Default creates .lnb/ and .lnb.env
        assert (tmp_path / ".lnb" / "entries").is_dir()
        assert (tmp_path / ".lnb" / "schema.yaml").exists()
        assert (tmp_path / LNB_ENV_FILE).exists()

    def test_init_auto_creates_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=str(tmp_path / "proj"), template=None)
        cmd_init(args)
        nb_dir = tmp_path / "proj"
        assert nb_dir.is_dir()
        assert (nb_dir / "entries").is_dir()
        assert (tmp_path / LNB_ENV_FILE).exists()

    def test_init_explicit_path_is_literal(self, tmp_path, monkeypatch):
        # An explicit path is used verbatim — no ".lnb" is appended.
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "notes"
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        assert (target / "schema.yaml").exists()
        assert (target / "entries").is_dir()
        # The old appending behavior would have created target/.lnb — it must not.
        assert not (target / ".lnb").exists()
        # .lnb.env points at the literal path.
        assert f"LAB_NOTEBOOK_DIR={target}" in (tmp_path / LNB_ENV_FILE).read_text()

    def test_init_no_arg_creates_dot_lnb(self, tmp_path, monkeypatch):
        # No path arg -> ./.lnb in the current directory.
        monkeypatch.chdir(tmp_path)
        cmd_init(argparse.Namespace(path=None, template=None))
        assert (tmp_path / ".lnb" / "schema.yaml").exists()

    def test_init_refuses_existing_lnb_env(self, tmp_path, monkeypatch):
        # A second init in the same directory refuses to clobber .lnb.env.
        monkeypatch.chdir(tmp_path)
        cmd_init(argparse.Namespace(path=str(tmp_path / "a"), template=None))
        with pytest.raises(LnbError) as exc:
            cmd_init(argparse.Namespace(path=str(tmp_path / "b"), template=None))
        assert "already exists" in str(exc.value)
        assert LNB_ENV_FILE in str(exc.value)
        # The refused run must not have repointed .lnb.env at "b".
        env_text = (tmp_path / LNB_ENV_FILE).read_text()
        assert str(tmp_path / "a") in env_text
        assert str(tmp_path / "b") not in env_text

    def test_init_force_overwrites_lnb_env(self, tmp_path, monkeypatch):
        # --force lets a second init repoint .lnb.env.
        monkeypatch.chdir(tmp_path)
        cmd_init(argparse.Namespace(path=str(tmp_path / "a"), template=None))
        cmd_init(argparse.Namespace(
            path=str(tmp_path / "b"), template=None, force=True))
        env_text = (tmp_path / LNB_ENV_FILE).read_text()
        assert f"LAB_NOTEBOOK_DIR={tmp_path / 'b'}" in env_text


# ---------------------------------------------------------------------------
# schema loading
# ---------------------------------------------------------------------------


class TestSchema:
    def test_load_schema_from_yaml(self, notebook):
        schema = load_schema(notebook)
        assert "observation" in schema["types"]
        assert "repo" in schema["fields"]
        assert schema["fields"]["tags"]["type"] == "list"

    def test_load_schema_missing_file(self, tmp_path):
        with pytest.raises(LnbError):
            load_schema(tmp_path)

    def test_schema_fields_null(self, notebook):
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n"
        )
        schema = load_schema(notebook)
        # BUILTIN_FIELDS are merged in even when schema declares no fields
        assert "artifacts" in schema["fields"]
        assert schema["fields"]["artifacts"]["type"] == "list"

    def test_builtin_field_type_override_rejected(self, notebook):
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n  artifacts: {type: integer}\n"
        )
        with pytest.raises(LnbError):
            load_schema(notebook)

    def test_builtin_field_same_type_also_rejected(self, notebook):
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n  artifacts: {type: list}\n"
        )
        with pytest.raises(LnbError):
            load_schema(notebook)

    def test_schema_field_spec_not_dict(self, notebook):
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n  repo: text\n"
        )
        with pytest.raises(LnbError):
            load_schema(notebook)

    def test_build_sql_creates_custom_columns(self, custom_notebook):
        schema = load_schema(custom_notebook)
        sql = build_sql(schema)
        assert '"dataset" TEXT' in sql.create
        assert '"gpu_hours" REAL' in sql.create
        assert '"num_nodes" INTEGER' in sql.create
        assert "extra TEXT" in sql.create

    def test_build_sql_fts_includes_custom_field(self, custom_notebook):
        schema = load_schema(custom_notebook)
        sql = build_sql(schema)
        assert "content" in sql.fts_cols
        assert "dataset" in sql.fts_cols
        assert "gpu_hours" not in sql.fts_cols

    def test_schema_output_reflects_custom_fields(self, custom_notebook, capsys):
        cmd_schema(argparse.Namespace())
        out = capsys.readouterr().out
        assert "dataset" in out
        assert "gpu_hours" in out
        assert "(fts)" in out
        assert "observation" in out
        assert "artifacts" in out
        assert "built-in" in out

    def test_custom_types_validation(self, custom_notebook):
        cmd_emit(make_custom_emit_args(type="result", content="A result"))
        # Should work: 'result' is in custom types

        with pytest.raises(LnbError):
            cmd_emit(make_custom_emit_args(type="milestone", content="Not allowed"))
        # 'milestone' is not in the custom schema's types list


class TestErrorHandling:
    """main() turns an LnbError raised anywhere below into exit 1 + stderr."""

    def test_main_catches_lnberror_exits_1(self, tmp_path, monkeypatch, capsys):
        # LAB_NOTEBOOK_DIR points at a dir with no schema.yaml, so load_schema
        # (reached via 'schema') raises LnbError. main()'s single handler must
        # print the message to stderr and exit 1 — same as the old sys.exit path.
        monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(tmp_path))
        monkeypatch.setattr("sys.argv", ["lab-notebook", "schema"])
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1
        err = capsys.readouterr().err
        assert "not found" in err
        assert "lab-notebook init" in err

    def test_main_lnberror_message_goes_to_stderr_not_stdout(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(tmp_path))
        monkeypatch.setattr("sys.argv", ["lab-notebook", "schema"])
        with pytest.raises(SystemExit):
            main()
        captured = capsys.readouterr()
        assert "not found" in captured.err
        assert "not found" not in captured.out


# ---------------------------------------------------------------------------
# emit
# ---------------------------------------------------------------------------


class TestEmit:
    def test_creates_jsonl_and_index(self, notebook):
        cmd_emit(make_emit_args(content="First entry"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        assert writer_file.exists()
        line = json.loads(writer_file.read_text().strip())
        assert line["content"] == "First entry"
        assert line["context"] == "test/context"
        assert line["type"] == "observation"
        assert line["writer_id"] == "test-writer"
        assert "id" in line
        assert "ts" in line
        assert "artifacts" in line  # built-in field always present

        # Index is built lazily on query, not during emit
        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT content FROM entries").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "First entry"

    def test_type_validation(self, notebook):
        with pytest.raises(LnbError):
            cmd_emit(make_emit_args(type="invalid-type"))

    def test_tags_and_artifacts(self, notebook):
        cmd_emit(make_emit_args(
            tags="tag1,tag2, tag3",
            artifacts="repo:file.csv, repo:plot.png",
        ))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["tags"] == ["tag1", "tag2", "tag3"]
        assert line["artifacts"] == ["repo:file.csv", "repo:plot.png"]

    def test_repo_and_branch(self, notebook):
        cmd_emit(make_emit_args(repo="my-repo", branch="feature/x"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["repo"] == "my-repo"
        assert line["branch"] == "feature/x"

    def test_multiple_entries_append(self, notebook):
        cmd_emit(make_emit_args(content="Entry 1"))
        cmd_emit(make_emit_args(content="Entry 2"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        lines = writer_file.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_custom_text_field(self, custom_notebook):
        cmd_emit(make_custom_emit_args(dataset="cifar10", content="Testing dataset"))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["dataset"] == "cifar10"

        conn, _, _ = ensure_db(custom_notebook)
        rows = conn.execute("SELECT dataset FROM entries").fetchall()
        conn.close()
        assert rows[0][0] == "cifar10"

    def test_custom_real_field(self, custom_notebook):
        cmd_emit(make_custom_emit_args(gpu_hours="4.5", content="Testing real"))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["gpu_hours"] == 4.5

        conn, _, _ = ensure_db(custom_notebook)
        rows = conn.execute("SELECT gpu_hours FROM entries").fetchall()
        conn.close()
        assert rows[0][0] == 4.5

    def test_custom_integer_field(self, custom_notebook):
        cmd_emit(make_custom_emit_args(num_nodes="32", content="Testing integer"))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["num_nodes"] == 32

    def test_custom_list_field(self, custom_notebook):
        cmd_emit(make_custom_emit_args(tags="mae,vit,scaling", content="Testing list"))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["tags"] == ["mae", "vit", "scaling"]

    def test_artifacts_builtin_on_custom_schema(self, custom_notebook):
        """artifacts is available even when the schema does not declare it."""
        cmd_emit(make_custom_emit_args(
            artifacts="artifacts/notes.md, artifacts/plot.png",
            content="Custom schema with artifacts",
        ))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["artifacts"] == ["artifacts/notes.md", "artifacts/plot.png"]

        conn, _, _ = ensure_db(custom_notebook)
        rows = conn.execute("SELECT artifacts FROM entries").fetchall()
        conn.close()
        assert json.loads(rows[0][0]) == ["artifacts/notes.md", "artifacts/plot.png"]

    def test_extra_escape_hatch(self, notebook):
        cmd_emit(make_emit_args(extra=["foo=bar", "num=42"], content="With extras"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["foo"] == "bar"
        assert line["num"] == "42"

        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT extra FROM entries").fetchall()
        conn.close()
        extra = json.loads(rows[0][0])
        assert extra["foo"] == "bar"
        assert extra["num"] == "42"

    def test_extra_rejects_schema_field_collision(self, notebook):
        with pytest.raises(LnbError):
            cmd_emit(make_emit_args(extra=["repo=sneaky"], content="Should fail"))

    def test_extra_rejects_core_field_collision(self, notebook):
        with pytest.raises(LnbError):
            cmd_emit(make_emit_args(extra=["context=sneaky"], content="Should fail"))

    def test_extra_rejects_builtin_field_collision(self, notebook):
        with pytest.raises(LnbError):
            cmd_emit(make_emit_args(extra=["artifacts=sneaky"], content="Should fail"))

    def test_extra_rejects_custom_schema_field_collision(self, custom_notebook):
        with pytest.raises(LnbError):
            cmd_emit(make_custom_emit_args(extra=["dataset=sneaky"], content="Should fail"))

    def test_extra_with_equals_in_value(self, notebook):
        cmd_emit(make_emit_args(extra=["expr=x=y+1"], content="Equals in value"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["expr"] == "x=y+1"


class TestEmitFieldFlag:
    """Schema fields are passed with repeatable -f/--field KEY=VALUE."""

    def test_field_flag_round_trips_every_type(self, custom_notebook):
        # text, integer, real, and list (comma-separated) all via -f.
        cmd_emit(make_custom_emit_args(field=[
            "dataset=cifar10",
            "num_nodes=32",
            "gpu_hours=4.5",
            "tags=mae,vit, scaling",
        ], content="All field types via -f"))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["dataset"] == "cifar10"          # text
        assert line["num_nodes"] == 32               # integer (coerced)
        assert line["gpu_hours"] == 4.5              # real (coerced)
        # list value lands as a JSON array in the JSONL, not a bare string.
        assert isinstance(line["tags"], list)
        assert line["tags"] == ["mae", "vit", "scaling"]

        conn, _, _ = ensure_db(custom_notebook)
        row = conn.execute(
            "SELECT dataset, num_nodes, gpu_hours FROM entries"
        ).fetchone()
        conn.close()
        assert row == ("cifar10", 32, 4.5)

    def test_field_flag_repeatable(self, notebook):
        cmd_emit(make_emit_args(field=["repo=my-repo", "branch=feature/x"],
                                content="Repeatable -f"))
        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["repo"] == "my-repo"
        assert line["branch"] == "feature/x"

    def test_field_flag_artifacts_still_static(self, notebook):
        # --artifacts remains a static flag (built-in field), not routed via -f.
        cmd_emit(make_emit_args(artifacts="a.csv, b.png", content="static artifacts"))
        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["artifacts"] == ["a.csv", "b.png"]

    def test_field_flag_unknown_field_raises_suggesting_extra(self, notebook):
        with pytest.raises(LnbError) as exc:
            cmd_emit(make_emit_args(field=["nonesuch=1"], content="bad"))
        msg = str(exc.value)
        assert "nonesuch" in msg
        assert "--extra" in msg

    def test_field_flag_bad_integer_raises(self, custom_notebook):
        with pytest.raises(LnbError) as exc:
            cmd_emit(make_custom_emit_args(field=["num_nodes=lots"], content="bad"))
        assert "integer" in str(exc.value)

    def test_field_flag_bad_real_raises(self, custom_notebook):
        with pytest.raises(LnbError) as exc:
            cmd_emit(make_custom_emit_args(field=["gpu_hours=fast"], content="bad"))
        assert "real" in str(exc.value)

    def test_field_flag_malformed_raises(self, notebook):
        with pytest.raises(LnbError):
            cmd_emit(make_emit_args(field=["justkey"], content="bad"))


class TestEmitHelpStable:
    """emit --help must not depend on the working dir or LAB_NOTEBOOK_DIR:
    the emit parser is static (no schema loading during construction)."""

    def _help_out(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["lab-notebook", "emit", "--help"])
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 0
        return capsys.readouterr().out

    def test_help_identical_with_and_without_notebook(
        self, custom_notebook, tmp_path, monkeypatch, capsys
    ):
        # With a discoverable custom notebook (dataset/gpu_hours/... fields):
        # under the old dynamic-argparse block these would appear as flags.
        out_with = self._help_out(monkeypatch, capsys)
        # Strip every notebook signal: the env var and the discovered .lnb.env.
        monkeypatch.delenv("LAB_NOTEBOOK_DIR", raising=False)
        (tmp_path / LNB_ENV_FILE).unlink(missing_ok=True)
        out_without = self._help_out(monkeypatch, capsys)

        assert out_with == out_without
        assert "--field" in out_with
        assert "-f" in out_with
        # No schema-derived flags leak into the static help.
        assert "--dataset" not in out_with
        assert "--gpu_hours" not in out_with


# ---------------------------------------------------------------------------
# sql
# ---------------------------------------------------------------------------


class TestSql:
    def test_basic_query(self, notebook, capsys):
        cmd_emit(make_emit_args(content="Hello world"))
        cmd_sql(argparse.Namespace(query="SELECT content FROM entries"))

        out = capsys.readouterr().out
        assert "Hello world" in out
        assert "1 row" in out

    def test_no_results(self, notebook, capsys):
        cmd_sql(argparse.Namespace(query="SELECT * FROM entries"))

        out = capsys.readouterr().out
        assert "no results" in out

    def test_invalid_sql(self, notebook):
        with pytest.raises(LnbError):
            cmd_sql(argparse.Namespace(query="NOT VALID SQL"))

    def test_query_custom_field(self, custom_notebook, capsys):
        cmd_emit(make_custom_emit_args(dataset="imagenet", content="Custom query test"))
        cmd_sql(argparse.Namespace(
            query="SELECT dataset, content FROM entries WHERE dataset = 'imagenet'"
        ))

        out = capsys.readouterr().out
        assert "imagenet" in out
        assert "1 row" in out


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


def make_show_args(entry_id):
    return argparse.Namespace(id=entry_id)


class TestShow:
    def test_shows_all_core_and_schema_fields(self, notebook, capsys):
        cmd_emit(make_emit_args(
            context="demo/ctx", type="observation",
            artifacts="a.csv,b.png", content="Full content here",
        ))
        entry_id = _last_id_in_file(notebook, "test-writer.jsonl")
        capsys.readouterr()  # clear emit output

        cmd_show(make_show_args(entry_id))
        out = capsys.readouterr().out
        # core fields
        assert entry_id in out
        assert "writer_id" in out
        assert "test-writer" in out
        assert "demo/ctx" in out
        assert "observation" in out
        assert "Full content here" in out
        # built-in schema field (list stored as a JSON array)
        assert "artifacts" in out
        assert "a.csv" in out and "b.png" in out

    def test_decodes_extra_json(self, notebook, capsys):
        cmd_emit(make_emit_args(
            extra=["foo=bar", "count=3"], content="entry with extras",
        ))
        entry_id = _last_id_in_file(notebook, "test-writer.jsonl")
        capsys.readouterr()

        cmd_show(make_show_args(entry_id))
        out = capsys.readouterr().out
        # extra keys are decoded into their own key/value lines, not dumped as
        # the raw JSON blob.
        assert "foo" in out and "bar" in out
        assert "count" in out
        assert '{"foo"' not in out

    def test_shows_custom_schema_fields(self, custom_notebook, capsys):
        cmd_emit(make_custom_emit_args(
            dataset="imagenet", gpu_hours="4.5", tags="mae,vit",
            content="custom fields entry",
        ))
        entry_id = _last_id_in_file(custom_notebook, "test-writer.jsonl")
        capsys.readouterr()

        cmd_show(make_show_args(entry_id))
        out = capsys.readouterr().out
        assert "dataset" in out and "imagenet" in out
        assert "gpu_hours" in out and "4.5" in out
        assert "tags" in out and "mae" in out

    def test_nonexistent_id_errors(self, notebook):
        with pytest.raises(LnbError) as exc:
            cmd_show(make_show_args("20990101T000000-dead"))
        assert "not found" in str(exc.value)

    def test_notebook_get_api(self, notebook):
        # Exercise the Notebook.get API directly (no argparse / cmd layer).
        cmd_emit(make_emit_args(content="direct api entry"))
        entry_id = _last_id_in_file(notebook, "test-writer.jsonl")

        nb = Notebook(notebook)
        try:
            entry = nb.get(entry_id)
        finally:
            nb.close()
        assert entry["id"] == entry_id
        assert entry["content"] == "direct api entry"
        assert entry["context"] == "test/context"

    def test_notebook_get_missing_raises(self, notebook):
        nb = Notebook(notebook)
        try:
            with pytest.raises(LnbError):
                nb.get("nope-never-existed")
        finally:
            nb.close()


# ---------------------------------------------------------------------------
# entry ids
# ---------------------------------------------------------------------------


class TestEntryId:
    def test_new_id_shape(self):
        # generate_id() -> compact naive timestamp + '-' + 8 hex chars
        # (secrets.token_hex(4)).
        entry_id = generate_id()
        assert re.fullmatch(r"\d{8}T\d{6}-[0-9a-f]{8}", entry_id), entry_id

    def test_old_format_suffix_still_retracts_and_shows(self, notebook, capsys):
        # Ids written by older code carry a 4-char (token_hex(2)) suffix.
        # Nothing parses the suffix, so a hand-written old-format entry must
        # still ingest, show, and retract like any current entry.
        old_id = "20240101T120000-ab12"
        writer_file = entries_dir(notebook) / "legacy-writer.jsonl"
        writer_file.write_text(json.dumps({
            "id": old_id,
            "ts": "2024-01-01T12:00:00",
            "writer_id": "legacy-writer",
            "context": "legacy/ctx",
            "type": "observation",
            "content": "old-format entry body",
        }) + "\n")

        # show renders the full entry
        cmd_show(make_show_args(old_id))
        out = capsys.readouterr().out
        assert old_id in out
        assert "old-format entry body" in out

        # retract removes it from the index
        cmd_retract(make_retract_args(old_id))
        conn, _, _ = ensure_db(notebook)
        try:
            rows = [r[0] for r in conn.execute("SELECT id FROM entries").fetchall()]
        finally:
            conn.close()
        assert old_id not in rows


# ---------------------------------------------------------------------------
# timestamps
# ---------------------------------------------------------------------------


class TestTimestamp:
    def test_emit_ts_is_timezone_aware(self, notebook):
        # New entries carry an offset-bearing ISO 8601 ts, e.g.
        # 2026-06-10T14:30:22-07:00.
        cmd_emit(make_emit_args(content="tz-aware entry"))
        conn, _, _ = ensure_db(notebook)
        try:
            ts = conn.execute(
                "SELECT ts FROM entries WHERE content = ?", ("tz-aware entry",)
            ).fetchone()[0]
        finally:
            conn.close()
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}", ts
        ), ts
        # Round-trips through fromisoformat and is genuinely offset-aware.
        assert datetime.fromisoformat(ts).tzinfo is not None

    def test_order_by_ts_mixes_legacy_naive_and_aware(self, notebook):
        # Legacy entries carry a naive ts (no offset); new ones carry an
        # offset-aware ts. Both share the YYYY-MM-DDThh:mm:ss prefix, so a
        # lexical ORDER BY ts still yields correct day-level ordering when the
        # two formats are interleaved in one JSONL.
        writer_file = entries_dir(notebook) / "mixed-writer.jsonl"
        rows = [
            ("20260315T143022-aaaa1111", "2026-03-15T14:30:22-07:00", "march aware"),
            ("20260101T080000-bbbb2222", "2026-01-01T08:00:00", "january naive"),
            ("20260520T091500-cccc3333", "2026-05-20T09:15:00+02:00", "may aware"),
            ("20260210T235959-dddd4444", "2026-02-10T23:59:59", "february naive"),
        ]
        lines = [
            json.dumps({
                "id": entry_id,
                "ts": ts,
                "writer_id": "mixed-writer",
                "context": "mixed/ctx",
                "type": "observation",
                "content": content,
            })
            for entry_id, ts, content in rows
        ]
        writer_file.write_text("\n".join(lines) + "\n")

        conn, _, _ = ensure_db(notebook)
        try:
            got = [
                r[0]
                for r in conn.execute(
                    "SELECT content FROM entries ORDER BY ts"
                ).fetchall()
            ]
        finally:
            conn.close()
        assert got == [
            "january naive",
            "february naive",
            "march aware",
            "may aware",
        ]


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


class TestSearch:
    def test_fts_match(self, notebook, capsys):
        cmd_emit(make_emit_args(content="The broker migration is complete"))
        cmd_emit(make_emit_args(content="Scaling laws experiment started"))

        cmd_search(argparse.Namespace(query="broker", context=None, type=None))
        out = capsys.readouterr().out
        assert "broker migration" in out
        assert "1 row" in out

    def test_filter_by_context(self, notebook, capsys):
        cmd_emit(make_emit_args(context="alpha", content="Entry in alpha"))
        cmd_emit(make_emit_args(context="beta", content="Entry in beta"))

        cmd_search(argparse.Namespace(query="entry", context="alpha", type=None))
        out = capsys.readouterr().out
        assert "alpha" in out
        assert "1 row" in out

    def test_filter_by_type(self, notebook, capsys):
        cmd_emit(make_emit_args(type="observation", content="I noticed this"))
        cmd_emit(make_emit_args(type="decision", content="We decided this"))

        cmd_search(argparse.Namespace(query="this", context=None, type="decision"))
        out = capsys.readouterr().out
        assert "decided" in out
        assert "1 row" in out

    def test_fts_on_custom_field(self, custom_notebook, capsys):
        cmd_emit(make_custom_emit_args(dataset="imagenet", content="Training run"))
        cmd_emit(make_custom_emit_args(dataset="cifar10", content="Another run"))

        cmd_search(argparse.Namespace(query="imagenet", context=None, type=None))
        out = capsys.readouterr().out
        assert "Training run" in out
        assert "1 row" in out


# ---------------------------------------------------------------------------
# contexts
# ---------------------------------------------------------------------------


class TestContexts:
    def test_groups_by_context(self, notebook, capsys):
        cmd_emit(make_emit_args(context="ctx/a", content="A1"))
        cmd_emit(make_emit_args(context="ctx/a", content="A2"))
        cmd_emit(make_emit_args(context="ctx/b", content="B1"))

        cmd_contexts(argparse.Namespace())
        out = capsys.readouterr().out
        assert "ctx/a" in out
        assert "ctx/b" in out
        assert "2 row" in out


# ---------------------------------------------------------------------------
# rebuild
# ---------------------------------------------------------------------------


class TestRebuild:
    def test_rebuild_from_jsonl(self, notebook, capsys):
        cmd_emit(make_emit_args(content="Persistent entry"))

        idx = index_path(notebook)
        idx.unlink(missing_ok=True)
        assert not idx.exists()

        cmd_rebuild(argparse.Namespace())
        out = capsys.readouterr().out
        assert "1 entries" in out
        assert idx.exists()

        conn = sqlite3.connect(str(idx))
        rows = conn.execute("SELECT content FROM entries").fetchall()
        conn.close()
        assert rows[0][0] == "Persistent entry"

    def test_auto_rebuild_on_sql(self, notebook, capsys):
        cmd_emit(make_emit_args(content="Auto rebuild test"))

        index_path(notebook).unlink(missing_ok=True)

        cmd_sql(argparse.Namespace(query="SELECT content FROM entries"))
        out = capsys.readouterr().out
        assert "Auto rebuild test" in out

    def test_rebuild_with_custom_schema(self, custom_notebook, capsys):
        cmd_emit(make_custom_emit_args(
            dataset="imagenet", gpu_hours="2.5", content="Custom rebuild"
        ))

        idx = index_path(custom_notebook)
        idx.unlink(missing_ok=True)

        cmd_rebuild(argparse.Namespace())
        out = capsys.readouterr().out
        assert "1 entries" in out

        conn = sqlite3.connect(str(idx))
        rows = conn.execute("SELECT dataset, gpu_hours, content FROM entries").fetchall()
        conn.close()
        assert rows[0][0] == "imagenet"
        assert rows[0][1] == 2.5
        assert rows[0][2] == "Custom rebuild"

    def test_emit_after_schema_change_succeeds(self, notebook, capsys):
        cmd_emit(make_emit_args(content="Before schema change"))
        # Add a new field to schema — emit no longer touches SQLite
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n"
            "  repo: {type: text}\n"
            "  branch: {type: text}\n"
            "  tags: {type: list}\n"
            "  new_field: {type: text}\n"
        )
        # Emit succeeds (JSONL-only, no SQLite interaction)
        cmd_emit(make_emit_args(content="After schema change"))
        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        lines = writer_file.read_text().strip().split("\n")
        assert len(lines) == 2

        # Query triggers lazy rebuild with new schema
        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT content FROM entries ORDER BY ts").fetchall()
        conn.close()
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# incremental ingest
# ---------------------------------------------------------------------------


class TestIncrementalIngest:
    def _writer_file(self, notebook):
        return entries_dir(notebook) / "test-writer.jsonl"

    def _read_offset(self, notebook, filename):
        conn = sqlite3.connect(str(index_path(notebook)))
        try:
            row = conn.execute(
                "SELECT offset FROM _ingest_state WHERE file = ?", (filename,)
            ).fetchone()
        finally:
            conn.close()
        return row[0] if row else None

    def test_first_read_records_offset(self, notebook):
        cmd_emit(make_emit_args(content="first entry"))
        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT content FROM entries").fetchall()
        conn.close()
        assert [r[0] for r in rows] == ["first entry"]
        wf = self._writer_file(notebook)
        assert self._read_offset(notebook, wf.name) == wf.stat().st_size

    def test_incremental_fast_path(self, notebook, capsys):
        for i in range(3):
            cmd_emit(make_emit_args(content=f"entry {i}"))
        ensure_db(notebook)[0].close()
        wf = self._writer_file(notebook)
        size_after_3 = wf.stat().st_size
        assert self._read_offset(notebook, wf.name) == size_after_3

        cmd_emit(make_emit_args(content="entry 3"))
        capsys.readouterr()  # drain prior output
        conn, _, _ = ensure_db(notebook)
        err = capsys.readouterr().err
        rows = conn.execute("SELECT content FROM entries ORDER BY ts").fetchall()
        conn.close()
        assert [r[0] for r in rows] == [f"entry {i}" for i in range(4)]
        assert "Index updated: +1 entries" in err
        assert "Index rebuilt" not in err
        assert self._read_offset(notebook, wf.name) == wf.stat().st_size

    def test_partial_line_at_eof_is_not_skipped(self, notebook):
        cmd_emit(make_emit_args(content="complete entry"))
        ensure_db(notebook)[0].close()
        wf = self._writer_file(notebook)
        offset_after_complete = self._read_offset(notebook, wf.name)
        assert offset_after_complete == wf.stat().st_size

        # Append a partial line (no trailing \n).
        partial = '{"id":"x-partial","ts":"2026'
        with open(wf, "a") as f:
            f.write(partial)
        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT id FROM entries").fetchall()
        conn.close()
        assert all(r[0] != "x-partial" for r in rows)
        assert self._read_offset(notebook, wf.name) == offset_after_complete

        # Complete the line; next read picks it up.
        rest = '-01-01T00:00:00","writer_id":"test-writer","context":"c","type":"observation","content":"finished","extra":null}\n'
        with open(wf, "a") as f:
            f.write(rest)
        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT id, content FROM entries WHERE id = 'x-partial'").fetchall()
        conn.close()
        assert rows == [("x-partial", "finished")]
        assert self._read_offset(notebook, wf.name) == wf.stat().st_size

    def test_truncation_falls_back_to_full_rebuild(self, notebook, capsys):
        cmd_emit(make_emit_args(content="line A"))
        cmd_emit(make_emit_args(content="line B"))
        ensure_db(notebook)[0].close()
        wf = self._writer_file(notebook)
        # Truncate so size < recorded offset.
        with open(wf, "r+") as f:
            full = f.read()
            first_line = full.split("\n", 1)[0] + "\n"
            f.seek(0)
            f.truncate()
            f.write(first_line)
        capsys.readouterr()
        conn, _, _ = ensure_db(notebook)
        err = capsys.readouterr().err
        rows = conn.execute("SELECT content FROM entries").fetchall()
        conn.close()
        assert [r[0] for r in rows] == ["line A"]
        assert "fence tripped" in err
        assert "Index rebuilt" in err

    def test_schema_change_falls_back_to_full_rebuild(self, notebook, capsys):
        import time
        cmd_emit(make_emit_args(content="before schema change"))
        ensure_db(notebook)[0].close()
        time.sleep(0.05)
        sf = notebook / "schema.yaml"
        sf.write_text(sf.read_text() + "\n# touched\n")
        capsys.readouterr()
        ensure_db(notebook)[0].close()
        err = capsys.readouterr().err
        assert "Index rebuilt" in err
        assert "Index updated" not in err

    def test_pre_ingest_state_index_migrates_transparently(self, notebook):
        cmd_emit(make_emit_args(content="entry one"))
        cmd_emit(make_emit_args(content="entry two"))
        ensure_db(notebook)[0].close()
        # Simulate an existing pre-_ingest_state index by dropping the table.
        conn = sqlite3.connect(str(index_path(notebook)))
        conn.execute("DROP TABLE _ingest_state")
        conn.commit()
        conn.close()
        # Re-open: incremental_ingest recreates the table and re-ingests via
        # idempotent INSERT OR REPLACE.
        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT content FROM entries ORDER BY ts").fetchall()
        conn.close()
        assert [r[0] for r in rows] == ["entry one", "entry two"]
        wf = self._writer_file(notebook)
        assert self._read_offset(notebook, wf.name) == wf.stat().st_size

    def test_malformed_line_in_middle_advances_past_it(self, notebook, capsys):
        cmd_emit(make_emit_args(content="good before bad"))
        ensure_db(notebook)[0].close()
        wf = self._writer_file(notebook)
        # Append a bad line then a good line.
        with open(wf, "a") as f:
            f.write("{not valid json\n")
            f.write(json.dumps({
                "id": "good-after-bad",
                "ts": "2026-01-01T00:00:00",
                "writer_id": "test-writer",
                "context": "c",
                "type": "observation",
                "content": "good after bad",
                "extra": None,
            }) + "\n")
        capsys.readouterr()
        conn, _, _ = ensure_db(notebook)
        err = capsys.readouterr().err
        rows = conn.execute("SELECT id, content FROM entries ORDER BY ts").fetchall()
        conn.close()
        ids = [r[0] for r in rows]
        assert "good-after-bad" in ids
        assert "skipping malformed line" in err
        assert self._read_offset(notebook, wf.name) == wf.stat().st_size

    def test_only_changed_writer_offset_advances(self, notebook, monkeypatch):
        cmd_emit(make_emit_args(content="from A"))
        monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "writer-b")
        cmd_emit(make_emit_args(content="from B"))
        ensure_db(notebook)[0].close()

        edir = entries_dir(notebook)
        a_file = edir / "test-writer.jsonl"
        b_file = edir / "writer-b.jsonl"
        a_offset_before = self._read_offset(notebook, a_file.name)
        b_offset_before = self._read_offset(notebook, b_file.name)

        # Only writer-b appends.
        cmd_emit(make_emit_args(content="from B again"))
        ensure_db(notebook)[0].close()

        assert self._read_offset(notebook, a_file.name) == a_offset_before
        assert self._read_offset(notebook, b_file.name) > b_offset_before
        assert self._read_offset(notebook, b_file.name) == b_file.stat().st_size


# ---------------------------------------------------------------------------
# index versioning (US-004)
# ---------------------------------------------------------------------------


class TestIndexVersion:
    def test_fresh_index_is_stamped_with_current_version(self, notebook):
        cmd_emit(make_emit_args(content="stamp me"))
        ensure_db(notebook)[0].close()
        assert _index_user_version(index_path(notebook)) == INDEX_USER_VERSION

    def test_old_format_index_forces_rebuild(self, notebook, capsys):
        # Build a current index, then simulate a pre-existing old-format index by
        # rewinding its stamped layout version. Any read via ensure_db must
        # notice the mismatch and rebuild from the JSONL (the source of truth),
        # then re-stamp the current version — after which FTS search still works.
        cmd_emit(make_emit_args(content="rebuildable token"))
        ensure_db(notebook)[0].close()
        dbp = index_path(notebook)
        conn = sqlite3.connect(str(dbp))
        conn.execute("PRAGMA user_version = 1")  # pretend the old layout
        conn.commit()
        conn.close()
        assert _index_user_version(dbp) == 1

        capsys.readouterr()  # clear
        conn, _, _ = ensure_db(notebook)
        err = capsys.readouterr().err
        try:
            assert "Index rebuilt" in err
            assert _index_user_version(dbp) == INDEX_USER_VERSION
            hit = conn.execute(
                "SELECT e.content FROM entries e "
                "JOIN entries_fts f ON f.rowid = e.rowid "
                "WHERE entries_fts MATCH 'rebuildable'"
            ).fetchall()
        finally:
            conn.close()
        assert hit == [("rebuildable token",)]


# ---------------------------------------------------------------------------
# multiple writers
# ---------------------------------------------------------------------------


class TestMultipleWriters:
    def test_separate_jsonl_files(self, notebook, monkeypatch):
        cmd_emit(make_emit_args(content="From writer A"))

        monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "writer-b")
        cmd_emit(make_emit_args(content="From writer B"))

        edir = entries_dir(notebook)
        assert (edir / "test-writer.jsonl").exists()
        assert (edir / "writer-b.jsonl").exists()

        conn, _, _ = ensure_db(notebook)
        rows = conn.execute("SELECT writer_id, content FROM entries ORDER BY writer_id").fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "test-writer"
        assert rows[1][0] == "writer-b"

    def test_rebuild_reads_all_writers(self, notebook, monkeypatch):
        cmd_emit(make_emit_args(content="Writer A entry"))
        monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "writer-b")
        cmd_emit(make_emit_args(content="Writer B entry"))

        index_path(notebook).unlink(missing_ok=True)
        cmd_rebuild(argparse.Namespace())

        conn = sqlite3.connect(str(index_path(notebook)))
        count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        conn.close()
        assert count == 2


# ---------------------------------------------------------------------------
# templates
# ---------------------------------------------------------------------------


class TestTemplateHelpers:
    def test_list_templates(self):
        templates = list_templates()
        names = [t[0] for t in templates]
        assert "research-notebook" in names
        assert "ml-experiment-log" in names
        assert len(templates) >= 2

    def test_get_template_path_valid(self):
        p = get_template_path("research-notebook")
        assert p is not None
        assert p.exists()

    def test_get_template_path_invalid(self):
        assert get_template_path("nonexistent") is None

    def test_get_template_path_traversal(self):
        assert get_template_path("../../etc/passwd") is None

    def test_read_template_valid(self):
        content = read_template("research-notebook")
        assert "types:" in content
        assert "observation" in content

    def test_read_template_invalid(self):
        with pytest.raises(LnbError):
            read_template("nonexistent")

    def test_print_templates(self, capsys):
        print_templates()
        out = capsys.readouterr().out
        assert "research-notebook" in out
        assert "ml-experiment-log" in out

    def test_templates_are_valid_yaml(self):
        import yaml
        for name, _ in list_templates():
            content = read_template(name)
            schema = yaml.safe_load(content)
            assert isinstance(schema["types"], list)
            assert len(schema["types"]) > 0
            assert isinstance(schema.get("fields", {}), dict)

    def test_read_template_from_path_valid(self, tmp_path):
        p = tmp_path / "custom.yaml"
        body = "types:\n  - note\nfields: {}\n"
        p.write_text(body)
        assert read_template_from_path(str(p)) == body

    def test_read_template_from_path_missing(self, tmp_path):
        missing = tmp_path / "nope.yaml"
        with pytest.raises(LnbError) as exc:
            read_template_from_path(str(missing))
        assert "not found" in str(exc.value)
        assert str(missing) in str(exc.value)

    def test_read_template_from_path_directory(self, tmp_path):
        with pytest.raises(LnbError):
            read_template_from_path(str(tmp_path))


class TestCmdTemplate:
    def test_list_no_args(self, notebook, capsys):
        args = argparse.Namespace(name=None, force=False)
        cmd_template(args)
        out = capsys.readouterr().out
        assert "research-notebook" in out
        assert "ml-experiment-log" in out

    def test_apply_to_fresh_notebook(self, notebook, capsys):
        # Remove existing schema.yaml to simulate fresh state
        (notebook / "schema.yaml").unlink()
        args = argparse.Namespace(name="ml-experiment-log", force=False)
        cmd_template(args)
        import yaml
        schema = yaml.safe_load((notebook / "schema.yaml").read_text())
        assert "run-start" in schema["types"]
        assert "method" in schema["fields"]

    def test_apply_existing_requires_force(self, notebook):
        args = argparse.Namespace(name="ml-experiment-log", force=False)
        with pytest.raises(SystemExit):
            cmd_template(args)

    def test_apply_force_overwrites(self, notebook, capsys):
        args = argparse.Namespace(name="ml-experiment-log", force=True)
        cmd_template(args)
        import yaml
        schema = yaml.safe_load((notebook / "schema.yaml").read_text())
        assert "run-start" in schema["types"]

    def test_invalid_name(self, notebook):
        (notebook / "schema.yaml").unlink()
        args = argparse.Namespace(name="nonexistent", force=False)
        with pytest.raises(LnbError):
            cmd_template(args)

    def test_rebuild_hint_with_entries(self, notebook, capsys):
        # Create an entry so entries/ has jsonl files
        cmd_emit(make_emit_args(content="An entry"))
        # Remove schema and apply template with --force
        args = argparse.Namespace(name="ml-experiment-log", force=True)
        cmd_template(args)
        out = capsys.readouterr().out
        assert "rebuild" in out


class TestInitTemplate:
    def test_init_with_template(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=str(tmp_path / "nb"), template="ml-experiment-log")
        cmd_init(args)
        import yaml
        nb_dir = tmp_path / "nb"
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "run-start" in schema["types"]
        assert "method" in schema["fields"]

    def test_init_template_list(self, tmp_path, capsys):
        args = argparse.Namespace(path=str(tmp_path), template="")
        cmd_init(args)
        out = capsys.readouterr().out
        assert "research-notebook" in out
        assert "ml-experiment-log" in out

    def test_init_template_overwrites_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        nb_dir = tmp_path / "nb"
        # First init with default
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "observation" in schema["types"]
        # Re-init with explicit template (--force to overwrite .lnb.env)
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template="ml-experiment-log", force=True)
        cmd_init(args)
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "run-start" in schema["types"]

    def test_init_no_template_keeps_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        nb_dir = tmp_path / "nb"
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)
        # Overwrite schema with custom content
        (nb_dir / "schema.yaml").write_text("types:\n  - custom\nfields:\n")
        # Re-init without --template (--force to overwrite .lnb.env)
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None, force=True)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "custom" in schema["types"]

    def test_init_output_reflects_reality(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        # First init
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)
        out = capsys.readouterr().out
        assert "from template: research-notebook" in out
        # Re-init without template — should say "kept" (--force to overwrite .lnb.env)
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None, force=True)
        cmd_init(args)
        out = capsys.readouterr().out
        assert "already exists (kept)" in out
        # Re-init with explicit template — should say "overwritten" (--force to overwrite .lnb.env)
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template="ml-experiment-log", force=True)
        cmd_init(args)
        out = capsys.readouterr().out
        assert "overwritten" in out

    def test_init_with_template_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        custom = tmp_path / "custom.yaml"
        custom.write_text("types:\n  - note\n  - todo\nfields: {}\n")
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=str(custom))
        cmd_init(args)
        nb_dir = tmp_path / "nb"
        assert (nb_dir / "schema.yaml").read_text() == custom.read_text()

    def test_init_template_path_missing_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        missing = tmp_path / "does-not-exist.yaml"
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=str(missing))
        with pytest.raises(LnbError) as exc:
            cmd_init(args)
        assert "not found" in str(exc.value)

    def test_init_template_path_overwrites_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        nb_dir = tmp_path / "nb"
        # First init with default
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=None)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "observation" in schema["types"]
        # Re-init with --template-path (--force to overwrite .lnb.env)
        custom = tmp_path / "custom.yaml"
        custom.write_text("types:\n  - note\nfields: {}\n")
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=str(custom),
            force=True)
        cmd_init(args)
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert schema["types"] == ["note"]

    def test_init_template_path_output_message(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        custom = tmp_path / "custom.yaml"
        custom.write_text("types:\n  - note\nfields: {}\n")
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=str(custom))
        cmd_init(args)
        out = capsys.readouterr().out
        assert f"from path: {custom}" in out

    def test_init_template_and_template_path_mutex(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        custom = tmp_path / "custom.yaml"
        custom.write_text("types:\n  - note\nfields: {}\n")
        monkeypatch.setattr(
            "sys.argv",
            ["lab-notebook", "init", "--template", "research-notebook",
             "--template-path", str(custom)],
        )
        with pytest.raises(SystemExit):
            main()


# ---------------------------------------------------------------------------
# .lnb.env discovery tests
# ---------------------------------------------------------------------------


class TestLnbEnvDiscovery:
    """Tests for .lnb.env file discovery and parsing."""

    def test_find_lnb_env_in_cwd(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(f"export LAB_NOTEBOOK_DIR={tmp_path}/nb\n")
        result = _find_lnb_env(start=tmp_path)
        assert result == env_file

    def test_find_lnb_env_walk_up(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(f"export LAB_NOTEBOOK_DIR={tmp_path}/nb\n")
        subdir = tmp_path / "a" / "b"
        subdir.mkdir(parents=True)
        result = _find_lnb_env(start=subdir)
        assert result == env_file

    def test_find_lnb_env_not_found(self, tmp_path):
        result = _find_lnb_env(start=tmp_path)
        assert result is None

    def test_parse_lnb_env_basic(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text("export LAB_NOTEBOOK_DIR=/some/path\n")
        assert _parse_lnb_env(env_file) == "/some/path"

    def test_parse_lnb_env_quoted(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text('export LAB_NOTEBOOK_DIR="/some/path"\n')
        assert _parse_lnb_env(env_file) == "/some/path"

    def test_parse_lnb_env_no_export(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text("LAB_NOTEBOOK_DIR=/some/path\n")
        assert _parse_lnb_env(env_file) == "/some/path"

    def test_parse_lnb_env_with_comments(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(
            "# Project notebook config\n"
            "export LAB_NOTEBOOK_DIR=/some/path\n"
            "export LAB_NOTEBOOK_WRITER=alice\n"
        )
        assert _parse_lnb_env(env_file) == "/some/path"

    def test_parse_lnb_env_empty_file(self, tmp_path):
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text("")
        assert _parse_lnb_env(env_file) is None

    def test_get_notebook_dir_from_lnb_env(self, tmp_path, monkeypatch):
        nb_dir = tmp_path / "nb"
        nb_dir.mkdir()
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(f"export LAB_NOTEBOOK_DIR={nb_dir}\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LAB_NOTEBOOK_DIR", raising=False)
        assert get_notebook_dir() == nb_dir

    def test_get_notebook_dir_env_fallback(self, tmp_path, monkeypatch):
        nb_dir = tmp_path / "nb"
        nb_dir.mkdir()
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(nb_dir))
        # No .lnb.env — should fall back to env var
        assert get_notebook_dir() == nb_dir

    def test_get_notebook_dir_env_var_precedence(self, tmp_path, monkeypatch):
        local_nb = tmp_path / "local-nb"
        local_nb.mkdir()
        explicit_nb = tmp_path / "explicit-nb"
        explicit_nb.mkdir()
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(f"export LAB_NOTEBOOK_DIR={local_nb}\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(explicit_nb))
        # $LAB_NOTEBOOK_DIR should win over .lnb.env
        assert get_notebook_dir() == explicit_nb

    def test_get_notebook_dir_empty_env_var_falls_through(self, tmp_path, monkeypatch):
        nb_dir = tmp_path / "nb"
        nb_dir.mkdir()
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(f"export LAB_NOTEBOOK_DIR={nb_dir}\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("LAB_NOTEBOOK_DIR", "")
        # Empty env var should be treated as unset; .lnb.env wins
        assert get_notebook_dir() == nb_dir


class TestInitDefault:
    """Tests for lab-notebook init (always creates .lnb/ + .lnb.env)."""

    def test_init_creates_lnb_env(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=None, template=None)
        cmd_init(args)
        lnb_env = tmp_path / LNB_ENV_FILE
        assert lnb_env.exists()
        content = lnb_env.read_text()
        assert "LAB_NOTEBOOK_DIR=" in content
        assert ".lnb" in content

    def test_init_creates_notebook_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path=None, template=None)
        cmd_init(args)
        nb_dir = tmp_path / ".lnb"
        assert nb_dir.is_dir()
        assert (nb_dir / "schema.yaml").exists()
        assert (nb_dir / "entries").is_dir()
        assert (nb_dir / "artifacts").is_dir()

    def test_init_custom_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = argparse.Namespace(path="my-project", template=None)
        cmd_init(args)
        nb_dir = tmp_path / "my-project"
        assert nb_dir.is_dir()
        lnb_env = tmp_path / LNB_ENV_FILE
        content = lnb_env.read_text()
        assert str(nb_dir) in content


# ---------------------------------------------------------------------------
# retract
# ---------------------------------------------------------------------------


def make_retract_args(target_id, reason="no longer accurate"):
    return argparse.Namespace(id=target_id, reason=reason)


def _entry_count(notebook):
    conn, _, _ = ensure_db(notebook)
    try:
        return conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
    finally:
        conn.close()


def _last_id_in_file(notebook, filename):
    """Return the id of the last record in a writer's JSONL file."""
    path = entries_dir(notebook) / filename
    lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
    return json.loads(lines[-1])["id"]


class TestRetract:
    def test_removes_target_keeps_others(self, notebook):
        cmd_emit(make_emit_args(content="keep me"))
        keep_id = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_emit(make_emit_args(content="drop me"))
        drop_id = _last_id_in_file(notebook, "test-writer.jsonl")

        cmd_retract(make_retract_args(drop_id))

        conn, _, _ = ensure_db(notebook)
        rows = [r[0] for r in conn.execute("SELECT id FROM entries").fetchall()]
        conn.close()
        assert keep_id in rows
        assert drop_id not in rows

    def test_target_gone_from_fts_search(self, notebook, capsys):
        cmd_emit(make_emit_args(content="unique_token_xyz here"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_retract(make_retract_args(target))
        capsys.readouterr()  # clear

        cmd_search(argparse.Namespace(query="unique_token_xyz", context=None, type=None))
        out = capsys.readouterr().out
        assert "unique_token_xyz" not in out

    def test_tombstone_is_not_a_queryable_entry(self, notebook):
        cmd_emit(make_emit_args(content="solo"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_retract(make_retract_args(target))

        conn, _, _ = ensure_db(notebook)
        try:
            total = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
            tombstones = conn.execute(
                "SELECT COUNT(*) FROM entries WHERE type = '_retract'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert total == 0
        assert tombstones == 0

    def test_tombstone_preserved_in_jsonl(self, notebook):
        cmd_emit(make_emit_args(content="audit trail"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_retract(make_retract_args(target, reason="superseded"))

        lines = [ln for ln in (entries_dir(notebook) / "test-writer.jsonl")
                 .read_text().splitlines() if ln.strip()]
        tombstone = json.loads(lines[-1])
        assert tombstone["type"] == "_retract"
        assert tombstone["retracts"] == target
        assert tombstone["reason"] == "superseded"

    def test_reason_is_required(self, notebook, monkeypatch):
        cmd_emit(make_emit_args(content="needs reason"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        monkeypatch.setattr("sys.argv", ["lab-notebook", "retract", target])
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 2

    def test_nonexistent_id_errors(self, notebook):
        with pytest.raises(LnbError) as exc:
            cmd_retract(make_retract_args("20990101T000000-dead"))
        assert "not found" in str(exc.value)

    def test_already_retracted_errors(self, notebook):
        cmd_emit(make_emit_args(content="retract twice"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_retract(make_retract_args(target))
        # First retract is applied on the read inside the second retract.
        with pytest.raises(LnbError):
            cmd_retract(make_retract_args(target))

    def test_cross_writer_retract_survives_rebuild(self, notebook, monkeypatch):
        # 'zoe' authors the entry; 'amy' retracts it. amy.jsonl sorts before
        # zoe.jsonl, so on a full rebuild the tombstone is read before the
        # target is inserted — the deferred two-phase delete must still apply.
        monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "zoe")
        cmd_emit(make_emit_args(content="zoe's entry"))
        target = _last_id_in_file(notebook, "zoe.jsonl")

        monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "amy")
        cmd_retract(make_retract_args(target))

        cmd_rebuild(argparse.Namespace())
        assert _entry_count(notebook) == 0

    def test_incremental_path_advances_offset(self, notebook):
        cmd_emit(make_emit_args(content="incremental"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        # Build the index first so retract exercises the incremental path.
        ensure_db(notebook)[0].close()

        cmd_retract(make_retract_args(target))
        assert _entry_count(notebook) == 0

        wf = entries_dir(notebook) / "test-writer.jsonl"
        conn = sqlite3.connect(str(index_path(notebook)))
        try:
            offset = conn.execute(
                "SELECT offset FROM _ingest_state WHERE file = ?", (wf.name,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert offset == wf.stat().st_size

    def test_incremental_ingest_missing_entries_dir(self, notebook):
        # Index built once, then entries/ disappears (deleted, or
        # LAB_NOTEBOOK_DIR repointed at a dir with only schema.yaml). The
        # incremental path must return cleanly, not crash on tuple-unpack.
        cmd_emit(make_emit_args(content="builds the index"))
        ensure_db(notebook)[0].close()  # build index so next read is incremental

        shutil.rmtree(entries_dir(notebook))  # entries/ gone; index + schema remain

        conn, _, _ = ensure_db(notebook)  # reaches incremental_ingest, edir missing
        conn.close()  # must not raise TypeError

    def test_rebuild_reports_net_active_count(self, notebook, capsys):
        cmd_emit(make_emit_args(content="keep"))
        cmd_emit(make_emit_args(content="drop"))
        drop_id = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_retract(make_retract_args(drop_id))
        capsys.readouterr()  # clear

        cmd_rebuild(argparse.Namespace())
        out = capsys.readouterr().out
        assert "1 entries" in out
        assert "2 entries" not in out

    def test_reserved_retract_type_rejected(self, notebook):
        # A schema may not declare the reserved control-record type; doing so
        # would let emitted rows be silently swallowed as tombstones.
        (notebook / "schema.yaml").write_text(
            "types:\n"
            "  - observation\n"
            "  - _retract\n"
        )
        with pytest.raises(LnbError) as exc:
            load_schema(notebook)
        assert "_retract" in str(exc.value)

    def test_pure_retract_pass_reports_retracted_count(self, notebook, capsys):
        cmd_emit(make_emit_args(content="to retract"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        ensure_db(notebook)[0].close()  # build index so retract uses incremental path
        cmd_retract(make_retract_args(target))  # tombstone applied on next read
        capsys.readouterr()  # clear

        ensure_db(notebook)[0].close()  # incremental pass applies the tombstone
        assert "Index updated: -1 retracted" in capsys.readouterr().err

    def test_mixed_add_and_retract_pass_reports_both(self, notebook, capsys):
        cmd_emit(make_emit_args(content="to retract"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        ensure_db(notebook)[0].close()  # build index; offset at EOF

        # Stage both a pending tombstone and a pending add for one incremental
        # pass: retract appends the tombstone unread, then emit appends content.
        cmd_retract(make_retract_args(target))
        cmd_emit(make_emit_args(content="brand new"))
        capsys.readouterr()  # clear

        ensure_db(notebook)[0].close()  # single pass: +1 add, -1 retract
        assert "Index updated: +1 entries, -1 retracted" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Notebook API (no argparse)
# ---------------------------------------------------------------------------


class TestNotebookApi:
    """Exercise the store.Notebook class directly, bypassing the CLI entirely."""

    def test_emit_query_retract_roundtrip(self, notebook):
        nb = Notebook(notebook)

        # emit returns the entry dict with fields coerced by Notebook.emit
        entry = nb.emit(
            "api/ctx",
            "observation",
            "direct api entry",
            fields={"repo": "lab", "tags": "alpha, beta"},
            extra={"custom": "x"},
        )
        assert entry["content"] == "direct api entry"
        assert entry["context"] == "api/ctx"
        assert entry["repo"] == "lab"
        assert entry["tags"] == ["alpha", "beta"]   # list field split from string
        assert entry["custom"] == "x"               # extra field carried through
        target_id = entry["id"]

        # query sees the freshly-emitted entry (index built on demand)
        cur = nb.query(
            "SELECT content, repo FROM entries WHERE id = ?", (target_id,)
        )
        rows = cur.fetchall()
        assert rows == [("direct api entry", "lab")]

        # retract appends a tombstone targeting the entry
        tomb = nb.retract(target_id, "no longer accurate")
        assert tomb["type"] == "_retract"
        assert tomb["retracts"] == target_id

        # query again: the entry is gone (tombstone applied on the next read)
        cur = nb.query("SELECT id FROM entries WHERE id = ?", (target_id,))
        assert cur.fetchall() == []
        nb.close()

    def test_emit_rejects_unknown_type(self, notebook):
        nb = Notebook(notebook)
        with pytest.raises(LnbError):
            nb.emit("api/ctx", "not-a-type", "content")
        nb.close()

    def test_retract_missing_target_raises(self, notebook):
        nb = Notebook(notebook)
        with pytest.raises(LnbError) as exc:
            nb.retract("20990101T000000-dead", "reason")
        assert "not found" in str(exc.value)
        nb.close()

    def test_init_missing_schema_raises(self, tmp_path):
        with pytest.raises(LnbError):
            Notebook(tmp_path)
