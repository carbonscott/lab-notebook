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
    _parse_lnb_env,
    cmd_contexts,
    cmd_emit,
    cmd_init,
    cmd_rebuild,
    cmd_retract,
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
    main,
    print_templates,
    read_template,
    read_template_from_path,
)


@pytest.fixture()
def notebook(tmp_path, monkeypatch):
    """Initialize a notebook in a temp directory and set env vars."""
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
    cmd_init(args)
    nb_dir = tmp_path / "nb" / ".lnb"
    monkeypatch.setenv("LAB_NOTEBOOK_DIR", str(nb_dir))
    monkeypatch.setenv("LAB_NOTEBOOK_WRITER", "test-writer")
    return nb_dir


@pytest.fixture()
def custom_notebook(tmp_path, monkeypatch):
    """Initialize a notebook with a custom schema."""
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
    cmd_init(args)
    nb_dir = tmp_path / "nb" / ".lnb"
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
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)

        nb_dir = tmp_path / "nb" / ".lnb"
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

        sf = tmp_path / "nb" / ".lnb" / "schema.yaml"
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
        nb_dir = tmp_path / "proj" / ".lnb"
        assert nb_dir.is_dir()
        assert (nb_dir / "entries").is_dir()
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

    def test_read_template_from_path_valid(self, tmp_path):
        p = tmp_path / "custom.yaml"
        body = "types:\n  - note\nfields: {}\n"
        p.write_text(body)
        assert read_template_from_path(str(p)) == body

    def test_read_template_from_path_missing(self, tmp_path, capsys):
        missing = tmp_path / "nope.yaml"
        with pytest.raises(SystemExit):
            read_template_from_path(str(missing))
        err = capsys.readouterr().err
        assert "not found" in err
        assert str(missing) in err

    def test_read_template_from_path_directory(self, tmp_path):
        with pytest.raises(SystemExit):
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
        args = argparse.Namespace(path=str(tmp_path / "nb"), template="ml-experiment-log")
        cmd_init(args)
        import yaml
        nb_dir = tmp_path / "nb" / ".lnb"
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
        nb_dir = tmp_path / "nb" / ".lnb"
        # First init with default
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "observation" in schema["types"]
        # Re-init with explicit template
        args = argparse.Namespace(path=str(tmp_path / "nb"), template="ml-experiment-log")
        cmd_init(args)
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "run-start" in schema["types"]

    def test_init_no_template_keeps_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        nb_dir = tmp_path / "nb" / ".lnb"
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)
        # Overwrite schema with custom content
        (nb_dir / "schema.yaml").write_text("types:\n  - custom\nfields:\n")
        # Re-init without --template
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
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
        # Re-init without template — should say "kept"
        args = argparse.Namespace(path=str(tmp_path / "nb"), template=None)
        cmd_init(args)
        out = capsys.readouterr().out
        assert "already exists (kept)" in out
        # Re-init with explicit template — should say "overwritten"
        args = argparse.Namespace(path=str(tmp_path / "nb"), template="ml-experiment-log")
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
        nb_dir = tmp_path / "nb" / ".lnb"
        assert (nb_dir / "schema.yaml").read_text() == custom.read_text()

    def test_init_template_path_missing_file(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        missing = tmp_path / "does-not-exist.yaml"
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=str(missing))
        with pytest.raises(SystemExit):
            cmd_init(args)
        err = capsys.readouterr().err
        assert "not found" in err

    def test_init_template_path_overwrites_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        nb_dir = tmp_path / "nb" / ".lnb"
        # First init with default
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=None)
        cmd_init(args)
        import yaml
        schema = yaml.safe_load((nb_dir / "schema.yaml").read_text())
        assert "observation" in schema["types"]
        # Re-init with --template-path
        custom = tmp_path / "custom.yaml"
        custom.write_text("types:\n  - note\nfields: {}\n")
        args = argparse.Namespace(
            path=str(tmp_path / "nb"), template=None, template_path=str(custom))
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
        nb_dir = tmp_path / "my-project" / ".lnb"
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

    def test_nonexistent_id_errors(self, notebook, capsys):
        with pytest.raises(SystemExit) as exc:
            cmd_retract(make_retract_args("20990101T000000-dead"))
        assert exc.value.code == 1
        assert "not found" in capsys.readouterr().err

    def test_already_retracted_errors(self, notebook):
        cmd_emit(make_emit_args(content="retract twice"))
        target = _last_id_in_file(notebook, "test-writer.jsonl")
        cmd_retract(make_retract_args(target))
        # First retract is applied on the read inside the second retract.
        with pytest.raises(SystemExit) as exc:
            cmd_retract(make_retract_args(target))
        assert exc.value.code == 1

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
