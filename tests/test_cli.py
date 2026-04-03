"""Tests for lab-notebook CLI."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

import pytest

from lab_notebook.cli import (
    LNB_ENV_FILE,
    _find_lnb_env,
    _index_is_stale,
    _parse_lnb_env,
    cmd_contexts,
    cmd_emit,
    cmd_init,
    cmd_rebuild,
    cmd_search,
    cmd_schema,
    cmd_sql,
    cmd_template,
    ensure_db,
    entries_dir,
    get_notebook_dir,
    get_template_path,
    index_path,
    list_templates,
    load_schema,
    build_sql,
    flatten_entry,
    print_templates,
    read_template,
)


@pytest.fixture()
def notebook(tmp_path, monkeypatch):
    """Initialize a notebook in a temp directory and set env vars."""
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "nb"
    args = argparse.Namespace(path=str(target), template=None)
    cmd_init(args)
    monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(target))
    monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "test-writer")
    return target


@pytest.fixture()
def custom_notebook(tmp_path, monkeypatch):
    """Initialize a notebook with a custom schema."""
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "nb"
    args = argparse.Namespace(path=str(target), template=None)
    cmd_init(args)
    # Overwrite schema.yaml with custom fields
    (target / "schema.yaml").write_text(
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
    monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(target))
    monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "test-writer")
    return target


def make_emit_args(**kwargs):
    defaults = {
        "context": "test/context",
        "type": "observation",
        "repo": None,
        "branch": None,
        "tags": None,
        "artifacts": None,
        "extra": None,
        "content": "Test content",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def make_custom_emit_args(**kwargs):
    defaults = {
        "context": "test/context",
        "type": "observation",
        "dataset": None,
        "gpu_hours": None,
        "num_nodes": None,
        "tags": None,
        "artifacts": None,
        "extra": None,
        "content": "Test content",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestInit:
    def test_creates_structure(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "nb"
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)

        assert (target / "entries").is_dir()
        assert (target / "artifacts").is_dir()
        assert (target / ".gitignore").exists()
        assert "index.sqlite" in (target / ".gitignore").read_text()
        # .lnb.env written in CWD
        lnb_env = tmp_path / LNB_ENV_FILE
        assert lnb_env.exists()
        env_text = lnb_env.read_text()
        assert f"LAB_NOTEBOOK_DIR={target}" in env_text
        assert "LAB_NOTEBOOK_WRITER=" in env_text

    def test_init_creates_schema_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "nb"
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)

        sf = target / "schema.yaml"
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
        target = tmp_path / "new-notebook"
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        assert target.is_dir()
        assert (target / "entries").is_dir()
        assert (tmp_path / LNB_ENV_FILE).exists()


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
        with pytest.raises(SystemExit):
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
        with pytest.raises(SystemExit):
            load_schema(notebook)

    def test_builtin_field_same_type_also_rejected(self, notebook):
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n  artifacts: {type: list}\n"
        )
        with pytest.raises(SystemExit):
            load_schema(notebook)

    def test_schema_field_spec_not_dict(self, notebook):
        (notebook / "schema.yaml").write_text(
            "types:\n  - observation\nfields:\n  repo: text\n"
        )
        with pytest.raises(SystemExit):
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

        with pytest.raises(SystemExit):
            cmd_emit(make_custom_emit_args(type="milestone", content="Not allowed"))
        # 'milestone' is not in the custom schema's types list


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
        with pytest.raises(SystemExit):
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
        with pytest.raises(SystemExit):
            cmd_emit(make_emit_args(extra=["repo=sneaky"], content="Should fail"))

    def test_extra_rejects_core_field_collision(self, notebook):
        with pytest.raises(SystemExit):
            cmd_emit(make_emit_args(extra=["context=sneaky"], content="Should fail"))

    def test_extra_rejects_builtin_field_collision(self, notebook):
        with pytest.raises(SystemExit):
            cmd_emit(make_emit_args(extra=["artifacts=sneaky"], content="Should fail"))

    def test_extra_rejects_custom_schema_field_collision(self, custom_notebook):
        with pytest.raises(SystemExit):
            cmd_emit(make_custom_emit_args(extra=["dataset=sneaky"], content="Should fail"))

    def test_extra_with_equals_in_value(self, notebook):
        cmd_emit(make_emit_args(extra=["expr=x=y+1"], content="Equals in value"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["expr"] == "x=y+1"


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
        with pytest.raises(SystemExit):
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
# staleness detection
# ---------------------------------------------------------------------------


class TestStaleness:
    def test_stale_after_new_entry(self, notebook):
        ensure_db(notebook)
        cmd_emit(make_emit_args(content="New entry"))
        assert _index_is_stale(notebook, index_path(notebook))

    def test_fresh_after_rebuild(self, notebook):
        cmd_emit(make_emit_args(content="An entry"))
        ensure_db(notebook)
        # Rebuild just happened — touch the index to ensure it's strictly newer
        import time
        time.sleep(0.05)
        idx = index_path(notebook)
        idx.touch()
        assert not _index_is_stale(notebook, idx)

    def test_stale_after_schema_change(self, notebook):
        ensure_db(notebook)
        import time
        time.sleep(0.05)
        (notebook / "schema.yaml").write_text(
            (notebook / "schema.yaml").read_text() + "\n# modified\n"
        )
        assert _index_is_stale(notebook, index_path(notebook))


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
        with pytest.raises(SystemExit):
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
        with pytest.raises(SystemExit):
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
        target = tmp_path / "nb"
        args = argparse.Namespace(path=str(target), template="ml-experiment-log")
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((target / "schema.yaml").read_text())
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
        target = tmp_path / "nb"
        # First init with default
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((target / "schema.yaml").read_text())
        assert "observation" in schema["types"]
        # Re-init with explicit template
        args = argparse.Namespace(path=str(target), template="ml-experiment-log")
        cmd_init(args)
        schema = yaml.safe_load((target / "schema.yaml").read_text())
        assert "run-start" in schema["types"]

    def test_init_no_template_keeps_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "nb"
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        # Overwrite schema with custom content
        (target / "schema.yaml").write_text("types:\n  - custom\nfields:\n")
        # Re-init without --template
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((target / "schema.yaml").read_text())
        assert "custom" in schema["types"]

    def test_init_output_reflects_reality(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "nb"
        # First init
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        out = capsys.readouterr().out
        assert "from template: research-notebook" in out
        # Re-init without template — should say "kept"
        args = argparse.Namespace(path=str(target), template=None)
        cmd_init(args)
        out = capsys.readouterr().out
        assert "already exists (kept)" in out
        # Re-init with explicit template — should say "overwritten"
        args = argparse.Namespace(path=str(target), template="ml-experiment-log")
        cmd_init(args)
        out = capsys.readouterr().out
        assert "overwritten" in out


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

    def test_get_notebook_dir_lnb_env_precedence(self, tmp_path, monkeypatch):
        local_nb = tmp_path / "local-nb"
        local_nb.mkdir()
        global_nb = tmp_path / "global-nb"
        global_nb.mkdir()
        env_file = tmp_path / LNB_ENV_FILE
        env_file.write_text(f"export LAB_NOTEBOOK_DIR={local_nb}\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(global_nb))
        # .lnb.env should win over $LAB_NOTEBOOK_DIR
        assert get_notebook_dir() == local_nb


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
        args = argparse.Namespace(path="my-notebook", template=None)
        cmd_init(args)
        nb_dir = tmp_path / "my-notebook"
        assert nb_dir.is_dir()
        lnb_env = tmp_path / LNB_ENV_FILE
        content = lnb_env.read_text()
        assert str(nb_dir) in content
