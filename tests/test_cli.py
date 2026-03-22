"""Tests for lab-notebook CLI."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

import pytest

from lab_notebook.cli import (
    cmd_contexts,
    cmd_emit,
    cmd_init,
    cmd_rebuild,
    cmd_search,
    cmd_sql,
    ensure_db,
    entries_dir,
    index_path,
    load_schema,
    build_sql,
    flatten_entry,
    DEFAULT_SCHEMA_YAML,
)


@pytest.fixture()
def notebook(tmp_path, monkeypatch):
    """Initialize a notebook in a temp directory and set env vars."""
    target = tmp_path / "nb"
    target.mkdir()
    args = argparse.Namespace(path=str(target))
    cmd_init(args)
    monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(target))
    monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "test-writer")
    return target


@pytest.fixture()
def custom_notebook(tmp_path, monkeypatch):
    """Initialize a notebook with a custom schema."""
    target = tmp_path / "nb"
    target.mkdir()
    args = argparse.Namespace(path=str(target))
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
        "extra": None,
        "content": "Test content",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestInit:
    def test_creates_structure(self, tmp_path):
        target = tmp_path / "nb"
        target.mkdir()
        args = argparse.Namespace(path=str(target))
        cmd_init(args)

        assert (target / "entries").is_dir()
        assert (target / ".gitignore").exists()
        assert "index.sqlite" in (target / ".gitignore").read_text()
        assert (target / ".env").exists()
        env_text = (target / ".env").read_text()
        assert f"LAB_NOTEBOOK_DIR={target}" in env_text
        assert "LAB_NOTEBOOK_WRITER=" in env_text

    def test_init_creates_schema_yaml(self, tmp_path):
        target = tmp_path / "nb"
        target.mkdir()
        args = argparse.Namespace(path=str(target))
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
        args = argparse.Namespace(path=None)
        cmd_init(args)

        assert (tmp_path / "entries").is_dir()
        assert (tmp_path / ".env").exists()
        assert (tmp_path / "schema.yaml").exists()

    def test_init_nonexistent_dir(self, tmp_path):
        args = argparse.Namespace(path=str(tmp_path / "does-not-exist"))
        with pytest.raises(SystemExit):
            cmd_init(args)


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

    def test_build_sql_creates_custom_columns(self, custom_notebook):
        schema = load_schema(custom_notebook)
        sql = build_sql(schema)
        assert "dataset TEXT" in sql.create
        assert "gpu_hours REAL" in sql.create
        assert "num_nodes INTEGER" in sql.create
        assert "extra TEXT" in sql.create

    def test_build_sql_fts_includes_custom_field(self, custom_notebook):
        schema = load_schema(custom_notebook)
        sql = build_sql(schema)
        assert "content" in sql.fts_cols
        assert "dataset" in sql.fts_cols
        assert "gpu_hours" not in sql.fts_cols

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

        conn = sqlite3.connect(str(index_path(notebook)))
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

        conn = sqlite3.connect(str(index_path(custom_notebook)))
        rows = conn.execute("SELECT dataset FROM entries").fetchall()
        conn.close()
        assert rows[0][0] == "cifar10"

    def test_custom_real_field(self, custom_notebook):
        cmd_emit(make_custom_emit_args(gpu_hours="4.5", content="Testing real"))

        writer_file = entries_dir(custom_notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["gpu_hours"] == 4.5

        conn = sqlite3.connect(str(index_path(custom_notebook)))
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

    def test_extra_escape_hatch(self, notebook):
        cmd_emit(make_emit_args(extra=["foo=bar", "num=42"], content="With extras"))

        writer_file = entries_dir(notebook) / "test-writer.jsonl"
        line = json.loads(writer_file.read_text().strip())
        assert line["foo"] == "bar"
        assert line["num"] == "42"

        conn = sqlite3.connect(str(index_path(notebook)))
        rows = conn.execute("SELECT extra FROM entries").fetchall()
        conn.close()
        extra = json.loads(rows[0][0])
        assert extra["foo"] == "bar"
        assert extra["num"] == "42"

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
        idx.unlink()
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

        index_path(notebook).unlink()

        cmd_sql(argparse.Namespace(query="SELECT content FROM entries"))
        out = capsys.readouterr().out
        assert "Auto rebuild test" in out

    def test_rebuild_with_custom_schema(self, custom_notebook, capsys):
        cmd_emit(make_custom_emit_args(
            dataset="imagenet", gpu_hours="2.5", content="Custom rebuild"
        ))

        idx = index_path(custom_notebook)
        idx.unlink()

        cmd_rebuild(argparse.Namespace())
        out = capsys.readouterr().out
        assert "1 entries" in out

        conn = sqlite3.connect(str(idx))
        rows = conn.execute("SELECT dataset, gpu_hours, content FROM entries").fetchall()
        conn.close()
        assert rows[0][0] == "imagenet"
        assert rows[0][1] == 2.5
        assert rows[0][2] == "Custom rebuild"


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

        conn = sqlite3.connect(str(index_path(notebook)))
        rows = conn.execute("SELECT writer_id, content FROM entries ORDER BY writer_id").fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "test-writer"
        assert rows[1][0] == "writer-b"

    def test_rebuild_reads_all_writers(self, notebook, monkeypatch):
        cmd_emit(make_emit_args(content="Writer A entry"))
        monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "writer-b")
        cmd_emit(make_emit_args(content="Writer B entry"))

        index_path(notebook).unlink()
        cmd_rebuild(argparse.Namespace())

        conn = sqlite3.connect(str(index_path(notebook)))
        count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        conn.close()
        assert count == 2
