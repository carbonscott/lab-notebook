"""pytest suite for lnb.py -- the minimal lab notebook CLI.

Invokes the CLI as a subprocess against this worktree's lnb.py. Each test
gets an isolated notebook via $LNB_DIR pointing into pytest's tmp_path, and
a fixed $LNB_WRITER="tester" so the per-writer jsonl filename is
deterministic. Args are passed as a list (no shell), so sigils like
"#decision" / "+decision" / "@ctx" reach argv literally.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

WORKTREE = Path(__file__).resolve().parent.parent
LNB_PY = str(WORKTREE / "lnb.py")

NOTED_RE = re.compile(r"^noted (\S+)\s+@(\S*)\s+#(\S*)", re.MULTILINE)


def run_lnb(args, cwd=WORKTREE, env=None):
    return subprocess.run(
        [sys.executable, LNB_PY, *args],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


@pytest.fixture
def nb_env(tmp_path):
    """Base env: isolated notebook dir under tmp_path, fixed writer id.

    cwd for run_lnb() defaults to WORKTREE (a real git repo) so that the
    *default context* logic (git rev-parse --show-toplevel) is exercised
    the same way it would be for a real user, while the notebook itself
    is safely isolated under tmp_path via $LNB_DIR.
    """
    env = os.environ.copy()
    env["LNB_DIR"] = str(tmp_path / "nb")
    env["LNB_WRITER"] = "tester"
    return env


def parse_noted(stdout):
    m = NOTED_RE.search(stdout)
    assert m, f"could not parse 'noted ...' line from: {stdout!r}"
    return {"id": m.group(1), "context": m.group(2), "type": m.group(3)}


def writer_jsonl(env):
    return Path(env["LNB_DIR"]) / f'{env["LNB_WRITER"]}.jsonl'


def read_entries(env):
    path = writer_jsonl(env)
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


# --- note -> find round trip -------------------------------------------------

def test_note_find_roundtrip(nb_env):
    marker = "roundtrip-marker-9f8e"
    r = run_lnb(["note", f"testing the {marker} round trip"], env=nb_env)
    assert r.returncode == 0, r.stderr
    info = parse_noted(r.stdout)

    r2 = run_lnb(["find", marker], env=nb_env)
    assert r2.returncode == 0, r2.stderr
    assert info["id"] in r2.stdout
    assert marker in r2.stdout


# --- default context ----------------------------------------------------------

def test_default_context_is_git_repo_name(nb_env):
    r = run_lnb(["note", "context default check"], env=nb_env)
    assert r.returncode == 0, r.stderr
    info = parse_noted(r.stdout)
    assert info["context"] == "lab-notebook-min"

    entries = read_entries(nb_env)
    assert entries[-1]["context"] == "lab-notebook-min"


# --- context overrides: @sigil and --context flag ----------------------------

def test_context_override_at_sigil(nb_env):
    r = run_lnb(["note", "at-sign context test", "@myctx"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert parse_noted(r.stdout)["context"] == "myctx"


def test_context_override_flag(nb_env):
    r = run_lnb(["note", "flag context test", "--context", "flagctx"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert parse_noted(r.stdout)["context"] == "flagctx"


# --- type overrides: #sigil, +sigil, --type flag -----------------------------

def test_type_override_hash_sigil(nb_env):
    r = run_lnb(["note", "hash sigil type test", "#decision"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert parse_noted(r.stdout)["type"] == "decision"


def test_type_override_plus_sigil(nb_env):
    r = run_lnb(["note", "plus sigil type test", "+deadend"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert parse_noted(r.stdout)["type"] == "deadend"


def test_type_override_flag(nb_env):
    r = run_lnb(["note", "flag type test", "--type", "milestone"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert parse_noted(r.stdout)["type"] == "milestone"


# --- extras persist and surface via find <id> and sql ------------------------

def test_extras_persist_and_appear_in_find_and_sql(nb_env):
    r = run_lnb(["note", "extras persistence test entry", "mae=0.87", "epoch=3"],
                 env=nb_env)
    assert r.returncode == 0, r.stderr
    entry_id = parse_noted(r.stdout)["id"]

    # unique id-fragment lookup -> full key/value dump, including extras
    frag = entry_id.split("-")[-1]
    r2 = run_lnb(["find", frag], env=nb_env)
    assert r2.returncode == 0, r2.stderr
    assert re.search(r"^mae\s+0\.87\s*$", r2.stdout, re.MULTILINE)
    assert re.search(r"^epoch\s+3\s*$", r2.stdout, re.MULTILINE)

    # sql: extra column carries a JSON blob with the extras
    r3 = run_lnb(["sql", f"SELECT extra FROM entries WHERE id = '{entry_id}'"],
                  env=nb_env)
    assert r3.returncode == 0, r3.stderr
    extra = json.loads(r3.stdout.strip())
    assert extra == {"mae": "0.87", "epoch": "3"}


# --- retract: removes from find/sql, append-only survives both lines --------

def test_retract_removes_from_find_and_sql_but_keeps_both_lines(nb_env):
    marker = "retract-target-marker-77"
    r = run_lnb(["note", f"entry to retract {marker}"], env=nb_env)
    assert r.returncode == 0, r.stderr
    entry_id = parse_noted(r.stdout)["id"]

    r_before = run_lnb(["find", marker], env=nb_env)
    assert entry_id in r_before.stdout

    r_retract = run_lnb(["retract", entry_id, "--reason", "no longer valid"],
                         env=nb_env)
    assert r_retract.returncode == 0, r_retract.stderr

    r_after = run_lnb(["find", marker], env=nb_env)
    assert r_after.returncode == 0
    assert entry_id not in r_after.stdout

    r_sql = run_lnb(["sql", "SELECT id FROM entries"], env=nb_env)
    assert r_sql.returncode == 0, r_sql.stderr
    assert entry_id not in r_sql.stdout.split()

    # append-only: BOTH the original line and the tombstone physically
    # remain in the jsonl file.
    entries = read_entries(nb_env)
    assert len(entries) >= 2
    assert any(e.get("id") == entry_id and e.get("type") != "_retract"
               for e in entries)
    assert any(e.get("type") == "_retract" and e.get("retracts") == entry_id
               for e in entries)


def test_retract_requires_reason(nb_env):
    r = run_lnb(["note", "needs a reason to retract"], env=nb_env)
    entry_id = parse_noted(r.stdout)["id"]
    r2 = run_lnb(["retract", entry_id], env=nb_env)
    assert r2.returncode != 0
    assert r2.stderr.strip() != ""


def test_retract_bogus_id_exits_nonzero(nb_env):
    run_lnb(["note", "some unrelated entry"], env=nb_env)
    r = run_lnb(["retract", "totallyBogusIdThatDoesNotExist", "--reason", "x"],
                 env=nb_env)
    assert r.returncode != 0
    assert r.stderr.strip() != ""


def test_retract_ambiguous_prefix_exits_nonzero(nb_env):
    nb_dir = Path(nb_env["LNB_DIR"])
    nb_dir.mkdir(parents=True)
    shared = "20260101T000000-aaaa"
    e1 = {"id": shared + "1111", "ts": "2026-01-01T00:00:00+00:00",
          "writer": "tester", "context": "lab-notebook-min", "type": "note",
          "content": "first ambiguous"}
    e2 = {"id": shared + "2222", "ts": "2026-01-01T00:00:01+00:00",
          "writer": "tester", "context": "lab-notebook-min", "type": "note",
          "content": "second ambiguous"}
    with open(nb_dir / "tester.jsonl", "w") as fh:
        fh.write(json.dumps(e1) + "\n")
        fh.write(json.dumps(e2) + "\n")

    r = run_lnb(["retract", shared, "--reason", "cleanup"], env=nb_env)
    assert r.returncode != 0
    assert "ambiguous" in r.stderr.lower()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "lnb.py defect (cmd_retract): the 'did you mean' near-match suggestion "
        "scans entry CONTENT for the mistyped target string, not entry IDS. "
        "Per the design spec (design/MINIMAL_LNB_DESIGN.md 'Bad id' example: "
        "\"No entry 'a7f3'. Did you mean a7f2c3d1 ...\"), the suggestion should "
        "fire for a near-miss ID typo. It never does, since ids practically "
        "never appear verbatim inside prose content. See test body / final report."
    ),
)
def test_retract_near_match_suggestion_should_be_id_based(nb_env):
    r = run_lnb(["note", "target entry for near-miss retract test"], env=nb_env)
    entry_id = parse_noted(r.stdout)["id"]
    # A near-miss typo of a real id (not a substring of it, so it misses the
    # substring-match branch and falls to the "no entry" + suggestion path).
    bogus = entry_id + "Z"
    r2 = run_lnb(["retract", bogus, "--reason", "typo id test"], env=nb_env)
    assert r2.returncode != 0
    # NB: `bogus` embeds `entry_id` as a literal prefix, and the error message
    # always echoes the (bogus) target verbatim -- so merely checking
    # "entry_id in stderr" would trivially pass without a real suggestion.
    # The actual "did you mean" suggestion phrase is what's being tested here.
    assert "did you mean" in r2.stderr.lower(), (
        f"expected a 'did you mean {entry_id}' suggestion; stderr was: "
        f"{r2.stderr!r}"
    )


# --- unique id-fragment lookup dumps the full entry --------------------------

def test_find_unique_id_fragment_dumps_full_entry(nb_env):
    r = run_lnb(["note", "fragment lookup entry unique48213", "tag=xyz"],
                 env=nb_env)
    assert r.returncode == 0, r.stderr
    entry_id = parse_noted(r.stdout)["id"]
    frag = entry_id.split("-")[-1]  # 8-hex random suffix; id-ish per ID_RE
    assert re.match(r"^[0-9A-Fa-fT-]{3,}$", frag)

    r2 = run_lnb(["find", frag], env=nb_env)
    assert r2.returncode == 0, r2.stderr
    assert re.search(rf"^id\s+{re.escape(entry_id)}\s*$", r2.stdout, re.MULTILINE)
    assert re.search(r"^content\s+fragment lookup entry unique48213\s*$",
                      r2.stdout, re.MULTILINE)
    assert re.search(r"^tag\s+xyz\s*$", r2.stdout, re.MULTILINE)


# --- malformed trailing line: skipped with a warning, doesn't crash ---------

def test_malformed_trailing_line_skipped_with_warning(nb_env):
    nb_dir = Path(nb_env["LNB_DIR"])
    nb_dir.mkdir(parents=True)
    good = {
        "id": "20260101T000000-cafebabe",
        "ts": "2026-01-01T00:00:00+00:00",
        "writer": "handcrafted",
        "context": "lab-notebook-min",
        "type": "note",
        "content": "well formed marker8675309",
    }
    (nb_dir / "handcrafted.jsonl").write_text(
        json.dumps(good) + "\n" + "{bad json this is not valid\n"
    )
    r = run_lnb(["find", "marker8675309"], env=nb_env)
    assert r.returncode == 0
    assert "marker8675309" in r.stdout
    assert "malformed" in r.stderr.lower()
    assert "handcrafted.jsonl:2" in r.stderr


# --- empty-notebook / no-notebook-found / nothing-found diagnostics --------

def test_empty_notebook_diagnostic(nb_env):
    r = run_lnb(["find"], env=nb_env)
    assert r.returncode == 0
    assert "empty" in r.stderr.lower()
    assert "lnb note" in r.stderr


def test_no_notebook_found_diagnostic(tmp_path):
    env = os.environ.copy()
    env.pop("LNB_DIR", None)
    env["LNB_WRITER"] = "tester"
    fresh_dir = tmp_path / "isolated_no_notebook"
    fresh_dir.mkdir()
    r = run_lnb(["find"], cwd=fresh_dir, env=env)
    assert r.returncode == 0
    assert "no notebook found" in r.stderr.lower()


def test_nothing_found_diagnostic(nb_env):
    run_lnb(["note", "alpha entry one nfmarker"], env=nb_env)
    run_lnb(["note", "beta entry two nfmarker"], env=nb_env)
    r = run_lnb(["find", "zzz_no_such_term_zzz"], env=nb_env)
    assert r.returncode == 0
    assert "no matches" in r.stderr.lower()
    assert "2 entries" in r.stderr
    assert "1 contexts" in r.stderr


# --- note creates ./.lnb from scratch on first write -------------------------

def test_note_creates_lnb_dir_when_none_exists(tmp_path):
    env = os.environ.copy()
    env.pop("LNB_DIR", None)
    env["LNB_WRITER"] = "tester"
    workdir = tmp_path / "fresh_project"
    workdir.mkdir()
    r = run_lnb(["note", "first note creates notebook dir"], cwd=workdir, env=env)
    assert r.returncode == 0, r.stderr
    assert (workdir / ".lnb").is_dir()
    assert (workdir / ".lnb" / "tester.jsonl").exists()


# --- find filters: @context / --context and #type/+type / --type -----------

def test_find_context_filter(nb_env):
    run_lnb(["note", "ctxA content markerqq", "@ctxA"], env=nb_env)
    run_lnb(["note", "ctxB content markerqq", "@ctxB"], env=nb_env)

    r = run_lnb(["find", "@ctxA"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert "ctxA content markerqq" in r.stdout
    assert "ctxB content markerqq" not in r.stdout

    r2 = run_lnb(["find", "--context", "ctxB"], env=nb_env)
    assert "ctxB content markerqq" in r2.stdout
    assert "ctxA content markerqq" not in r2.stdout


def test_find_type_filter(nb_env):
    run_lnb(["note", "typeA content markerqq", "#typeA"], env=nb_env)
    run_lnb(["note", "typeB content markerqq", "+typeB"], env=nb_env)

    r = run_lnb(["find", "#typeA"], env=nb_env)
    assert r.returncode == 0, r.stderr
    assert "typeA content markerqq" in r.stdout
    assert "typeB content markerqq" not in r.stdout

    r2 = run_lnb(["find", "--type", "typeB"], env=nb_env)
    assert "typeB content markerqq" in r2.stdout
    assert "typeA content markerqq" not in r2.stdout


# --- default find view: no args -> last 10, oldest-first / newest-last -----

def test_find_no_args_default_view_newest_last(nb_env):
    ids = []
    for i in range(3):
        r = run_lnb(["note", f"seq marker {i} zzqqqorder"], env=nb_env)
        assert r.returncode == 0, r.stderr
        ids.append(parse_noted(r.stdout)["id"])

    r = run_lnb(["find"], env=nb_env)
    assert r.returncode == 0, r.stderr
    positions = [r.stdout.index(i) for i in ids]
    assert positions == sorted(positions), (
        "expected entries printed oldest-first / newest-last"
    )


# --- sql: GROUP BY aggregate --------------------------------------------------

def test_sql_group_by_aggregate(nb_env):
    run_lnb(["note", "type alpha entry one", "#alpha"], env=nb_env)
    run_lnb(["note", "type alpha entry two", "#alpha"], env=nb_env)
    run_lnb(["note", "type beta entry one", "#beta"], env=nb_env)

    r = run_lnb(
        ["sql", "SELECT type, COUNT(*) FROM entries GROUP BY type ORDER BY type"],
        env=nb_env,
    )
    assert r.returncode == 0, r.stderr
    rows = [line.split("\t") for line in r.stdout.strip().splitlines()]
    counts = {t: int(c) for t, c in rows}
    assert counts["alpha"] == 2
    assert counts["beta"] == 1
