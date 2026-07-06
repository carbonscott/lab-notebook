"""pytest suite for lnb.py -- the minimal lab notebook CLI (post-redesign).

Three verbs: `note` appends, `log` emits EVERY record as JSONL (ascending by
ts, incl. `_retract` tombstones, uncapped, verbatim), `retract` appends a
tombstone. jq is the consumer: all selection -- including liveness -- is jq's
job, so reads are driven through `lnb log` and, where liveness matters, the
exact live-view jq recipe lnb ships in `--help`.

Invokes the CLI as a subprocess against this worktree's lnb.py. Each test gets
an isolated notebook via $LNB_DIR under pytest's tmp_path and a fixed
$LNB_WRITER="tester" so the per-writer jsonl filename is deterministic. Args
are passed as a list (no shell), so sigils like "+decision"/"@ctx" reach argv
literally.

NOTE on ts ordering: `log` sorts on the ISO ts STRING and assumes a stable UTC
offset (a single-timezone notebook). Cross-offset instant ordering is a
documented limitation of the design, deliberately NOT tested here.
"""
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

WORKTREE = Path(__file__).resolve().parent.parent
LNB_PY = str(WORKTREE / "lnb.py")

NOTED_RE = re.compile(r"^noted (\S+)\s+@(\S*)\s+\+(\S*)", re.MULTILINE)

# The ONE canonical live-view jq recipe. Kept byte-identical to the copy lnb
# ships in `--help`; test_live_view_jq_idiom_drops_retracted asserts RECIPE is
# IN the help text AND runs JQ_LIVE_FILTER through real jq, so the shipped copy
# can never silently rot.
JQ_LIVE_FILTER = (
    'map(select(.type=="_retract").retracts) as $dead\n'
    '      | .[] | select(.type != "_retract" and (.id | IN($dead[]) | not))'
)
RECIPE = "lnb log | jq -s '" + JQ_LIVE_FILTER + "'"

# The distinctive core of the live-view recipe. Unlike RECIPE (whose exact
# indentation is pinned to the --help copy), these two fragments appear verbatim
# in ALL THREE shipped copies -- lnb.py's docstring/--help, README.md and
# SKILL.md -- so they pin the README/SKILL copies against drift. Pure text
# presence, so this runs everywhere (no jq needed).
RECIPE_CORE = 'select(.type != "_retract" and (.id | IN($dead[]) | not))'
RECIPE_DEAD_BIND = 'map(select(.type=="_retract").retracts) as $dead'

HAVE_JQ = shutil.which("jq") is not None
needs_jq = pytest.mark.skipif(not HAVE_JQ, reason="jq is not installed")


def run_lnb(args, cwd=WORKTREE, env=None):
    return subprocess.run(
        [sys.executable, LNB_PY, *args],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def run_jq(jq_filter, stdin_text, slurp=False):
    # -c: compact, one JSON object per line, so results parse line-by-line.
    # (Only affects formatting; the shipped filter itself is run verbatim.)
    args = ["jq", "-c"] + (["-s"] if slurp else []) + [jq_filter]
    return subprocess.run(args, input=stdin_text, capture_output=True,
                          text=True, timeout=10)


@pytest.fixture
def nb_env(tmp_path):
    """Base env: isolated notebook dir under tmp_path, fixed writer id.

    cwd for run_lnb() defaults to WORKTREE (a real git repo) so that the
    *default context* logic (git rev-parse --show-toplevel) is exercised the
    same way it would be for a real user, while the notebook itself is safely
    isolated under tmp_path via $LNB_DIR.
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


def log_objs(env):
    """`lnb log` -> the emitted records, each stdout line parsed as JSON."""
    r = run_lnb(["log"], env=env)
    assert r.returncode == 0, r.stderr
    return [json.loads(l) for l in r.stdout.splitlines() if l.strip()]


def n_tombstones(env):
    return sum(1 for e in read_entries(env) if e.get("type") == "_retract")


# --- note -> log round trip --------------------------------------------------

def test_note_log_roundtrip(nb_env):
    marker = "roundtrip-marker-9f8e"
    r = run_lnb(["note", f"testing the {marker} round trip"], env=nb_env)
    assert r.returncode == 0, r.stderr
    info = parse_noted(r.stdout)

    hit = next(o for o in log_objs(nb_env) if o["id"] == info["id"])
    assert marker in hit["content"]


# --- default context ----------------------------------------------------------

def test_default_context_is_git_repo_name(nb_env):
    r = run_lnb(["note", "context default check"], env=nb_env)
    assert r.returncode == 0, r.stderr
    info = parse_noted(r.stdout)
    # default context is the enclosing git repo's dir name; run_lnb's cwd is
    # WORKTREE, so it's WORKTREE.name -- derived from the filesystem, an
    # independent cross-check of default_context()'s `git rev-parse` result,
    # and robust to the worktree's directory name.
    assert info["context"] == WORKTREE.name

    entries = read_entries(nb_env)
    assert entries[-1]["context"] == WORKTREE.name


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


# --- extras persist and surface as top-level keys in the emitted record ------

def test_extras_persist(nb_env):
    r = run_lnb(["note", "extras persistence test entry", "mae=0.87", "epoch=3"],
                env=nb_env)
    assert r.returncode == 0, r.stderr
    entry_id = parse_noted(r.stdout)["id"]

    hit = next(o for o in log_objs(nb_env) if o["id"] == entry_id)
    assert hit["mae"] == "0.87"       # extra surfaced as a top-level key
    assert hit["epoch"] == "3"
    # on-disk record matches what `log` emits, verbatim
    stored = next(e for e in read_entries(nb_env) if e["id"] == entry_id)
    assert hit == stored


# --- retract: entry stays visible in log, tombstone appended (contract iii) --

def test_retract_keeps_entry_visible_in_log_and_appends_tombstone(nb_env):
    marker = "retract-target-marker-77"
    r = run_lnb(["note", f"entry to retract {marker}"], env=nb_env)
    assert r.returncode == 0, r.stderr
    entry_id = parse_noted(r.stdout)["id"]

    assert any(o["id"] == entry_id for o in log_objs(nb_env))

    r_retract = run_lnb(["retract", entry_id, "--reason", "no longer valid"],
                        env=nb_env)
    assert r_retract.returncode == 0, r_retract.stderr

    # FLIP: after retract, `log` STILL shows the original entry AND the
    # tombstone (was: excluded). Liveness is the consumer's job now.
    objs = log_objs(nb_env)
    assert any(o["id"] == entry_id and o.get("type") != "_retract" for o in objs)
    assert any(o.get("type") == "_retract" and o.get("retracts") == entry_id
               for o in objs)

    # append-only: BOTH the original line and the tombstone physically remain.
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


def test_retract_near_match_suggestion_is_id_based(nb_env):
    r = run_lnb(["note", "target entry for near-miss retract test"], env=nb_env)
    entry_id = parse_noted(r.stdout)["id"]
    # A near-miss typo of a real id (not a substring of it, so it misses the
    # substring-match branch and falls to the "no entry" + suggestion path).
    bogus = entry_id + "Z"
    r2 = run_lnb(["retract", bogus, "--reason", "typo id test"], env=nb_env)
    assert r2.returncode != 0
    # `bogus` embeds `entry_id`, and the error echoes the target verbatim, so
    # "entry_id in stderr" would trivially pass. The "did you mean" suggestion
    # phrase is what's actually being tested here.
    assert "did you mean" in r2.stderr.lower(), (
        f"expected a 'did you mean {entry_id}' suggestion; stderr was: "
        f"{r2.stderr!r}"
    )


# --- malformed trailing line: skipped with a warning, doesn't crash ----------

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
    r = run_lnb(["log"], env=nb_env)
    assert r.returncode == 0
    assert "marker8675309" in r.stdout               # good line survives
    assert "malformed" in r.stderr.lower()           # bad line diagnosed
    assert "handcrafted.jsonl:2" in r.stderr


@pytest.mark.parametrize("scalar_line", ["42", "null", "true", '"x"', "[1,2]"])
def test_scan_skips_non_object_json_line(nb_env, scalar_line):
    """A line that is VALID json but NOT an object (a scalar/array) must be
    skipped with a 'skipping non-object line' warning -- never reach rec.get()
    and crash with an AttributeError. scan() 'fails closed per line', so both
    reads (`log` here, and `retract`) survive. This test FAILS (traceback,
    exit != 0) if the isinstance(rec, dict) guard is removed from scan()."""
    nb_dir = Path(nb_env["LNB_DIR"])
    nb_dir.mkdir(parents=True)
    good = {
        "id": "20260101T000000-deadbeef",
        "ts": "2026-01-01T00:00:00+00:00",
        "writer": "handcrafted",
        "context": "lab-notebook-min",
        "type": "note",
        "content": "well formed nonobj-marker-5150",
    }
    # good record first, then a valid-JSON-but-non-object line.
    (nb_dir / "handcrafted.jsonl").write_text(
        json.dumps(good) + "\n" + scalar_line + "\n"
    )
    r = run_lnb(["log"], env=nb_env)
    assert r.returncode == 0, r.stderr                # exit 0, no crash
    assert "nonobj-marker-5150" in r.stdout           # good record still emitted
    assert "skipping non-object line" in r.stderr     # bad line diagnosed
    assert "handcrafted.jsonl:2" in r.stderr
    assert "Traceback" not in r.stderr                # NEVER an uncaught traceback
    assert "AttributeError" not in r.stderr
    # the non-object was skipped, not emitted: every stdout line is an object.
    objs = [json.loads(l) for l in r.stdout.splitlines() if l.strip()]
    assert all(isinstance(o, dict) for o in objs)
    assert len(objs) == 1


# --- empty-notebook / no-notebook-found onboarding nudges (exit 0) -----------

def test_empty_notebook_diagnostic(nb_env):
    r = run_lnb(["log"], env=nb_env)
    assert r.returncode == 0
    assert "empty" in r.stderr.lower()
    assert "lnb note" in r.stderr
    assert r.stdout.strip() == ""


def test_no_notebook_found_diagnostic(tmp_path):
    env = os.environ.copy()
    env.pop("LNB_DIR", None)
    env["LNB_WRITER"] = "tester"
    fresh_dir = tmp_path / "isolated_no_notebook"
    fresh_dir.mkdir()
    r = run_lnb(["log"], cwd=fresh_dir, env=env)
    assert r.returncode == 0
    assert "no notebook found" in r.stderr.lower()
    assert "lnb note" in r.stderr


def test_log_empty_and_absent_notebook_still_exit_0(tmp_path):
    # absent notebook
    env = os.environ.copy()
    env.pop("LNB_DIR", None)
    env["LNB_WRITER"] = "tester"
    fresh_dir = tmp_path / "absent"
    fresh_dir.mkdir()
    assert run_lnb(["log"], cwd=fresh_dir, env=env).returncode == 0
    # empty (dir exists via $LNB_DIR, no records)
    env2 = os.environ.copy()
    env2["LNB_DIR"] = str(tmp_path / "empty_nb")
    env2["LNB_WRITER"] = "tester"
    assert run_lnb(["log"], env=env2).returncode == 0


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


# --- log is a literal, filter-free, ascending, uncapped emitter --------------

def test_log_is_filter_free(nb_env):
    """`log` (no args) emits ALL records regardless of type/context; there is
    no argument that narrows it."""
    a = parse_noted(run_lnb(["note", "alpha filterfree", "+decision", "@ctxA"],
                            env=nb_env).stdout)["id"]
    b = parse_noted(run_lnb(["note", "beta filterfree", "+note", "@ctxB"],
                            env=nb_env).stdout)["id"]
    c = parse_noted(run_lnb(["note", "gamma filterfree", "+deadend", "@ctxC"],
                            env=nb_env).stdout)["id"]
    ids = [o["id"] for o in log_objs(nb_env)]
    assert set(ids) == {a, b, c}


@pytest.mark.parametrize("stray", [
    ["decision"], ["@ctx"], ["+type"], ["masking", "ratio"], ["-o", "json"],
])
def test_log_stray_arg_is_usage_error(nb_env, stray):
    """`log` is argument-free and fails CLOSED on any argument (never a silent
    filter). This is also the anti-gaming lever against keeping stale
    `-o json`/id/filter behavior alive."""
    run_lnb(["note", "some entry for stray-arg test"], env=nb_env)
    r = run_lnb(["log", *stray], env=nb_env)
    assert r.returncode != 0, f"log {stray!r} must fail closed, not filter"
    assert r.stdout.strip() == "", "fail-closed: nothing on stdout"
    assert r.stderr.strip() != ""


def test_log_ascending_order(nb_env):
    ids = []
    for i in range(3):
        r = run_lnb(["note", f"seq marker {i} zzqqqorder"], env=nb_env)
        assert r.returncode == 0, r.stderr
        ids.append(parse_noted(r.stdout)["id"])

    r = run_lnb(["log"], env=nb_env)
    assert r.returncode == 0, r.stderr
    positions = [r.stdout.index(i) for i in ids]
    assert positions == sorted(positions), "log emits ascending oldest->newest"


def test_log_emits_all_ascending_across_writers(nb_env):
    """Records from two writer files merge and sort ascending by ts across
    files (`log` re-sorts the whole corpus, so per-file order is irrelevant)."""
    nb_dir = Path(nb_env["LNB_DIR"])
    nb_dir.mkdir(parents=True)
    a = {"id": "20260101T000000-aaaa0001", "ts": "2026-01-01T00:00:00+00:00",
         "writer": "w1", "context": "c", "type": "note", "content": "first w1"}
    c = {"id": "20260101T000030-cccc0003", "ts": "2026-01-01T00:00:30+00:00",
         "writer": "w1", "context": "c", "type": "note", "content": "third w1"}
    b = {"id": "20260101T000015-bbbb0002", "ts": "2026-01-01T00:00:15+00:00",
         "writer": "w2", "context": "c", "type": "note", "content": "second w2"}
    d = {"id": "20260101T000045-dddd0004", "ts": "2026-01-01T00:00:45+00:00",
         "writer": "w2", "context": "c", "type": "note", "content": "fourth w2"}
    # neither file is internally interleaved with the other, so a correct merge
    # requires re-sorting the whole corpus.
    (nb_dir / "w1.jsonl").write_text(json.dumps(a) + "\n" + json.dumps(c) + "\n")
    (nb_dir / "w2.jsonl").write_text(json.dumps(b) + "\n" + json.dumps(d) + "\n")

    objs = log_objs(nb_env)
    ts_list = [o["ts"] for o in objs]
    assert ts_list == sorted(ts_list)
    assert [o["id"] for o in objs] == [a["id"], b["id"], c["id"], d["id"]]


def test_log_is_uncapped(nb_env):
    """>10 entries -> `log` emits ALL of them, with no 'showing N of M' notice
    anywhere (the removed default-view cap)."""
    for i in range(15):
        assert run_lnb(["note", f"uncapped entry number {i}"],
                       env=nb_env).returncode == 0
    r = run_lnb(["log"], env=nb_env)
    assert r.returncode == 0, r.stderr
    objs = [json.loads(l) for l in r.stdout.splitlines() if l.strip()]
    assert len(objs) == 15
    combined = (r.stdout + r.stderr).lower()
    assert "showing" not in combined
    assert "of 15" not in combined


def test_log_is_valid_jsonl_only(nb_env):
    """Every non-empty stdout line independently parses as JSON (valid JSONL);
    stdout carries ONLY json, no human table header/framing."""
    for i in range(3):
        assert run_lnb(["note", f"row {i} jsonlmarker entry"],
                       env=nb_env).returncode == 0
    r = run_lnb(["log"], env=nb_env)
    assert r.returncode == 0, r.stderr
    lines = [l for l in r.stdout.splitlines() if l.strip()]
    objs = [json.loads(l) for l in lines]        # raises if any line isn't JSON
    assert len(objs) == 3
    assert all(isinstance(o, dict) and "id" in o for o in objs)
    assert all(l.startswith("{") and l.endswith("}") for l in lines)


def test_log_no_truncation(nb_env):
    """`log` emits `content` verbatim -- no 80-char display truncation."""
    long_content = "verbatim-marker-vb42 " + "z" * 100
    r = run_lnb(["note", long_content], env=nb_env)
    entry_id = parse_noted(r.stdout)["id"]

    hit = next(o for o in log_objs(nb_env) if o["id"] == entry_id)
    assert hit["content"] == long_content
    assert len(hit["content"]) > 80              # would have been truncated


def test_log_non_ascii_raw(nb_env):
    """ensure_ascii=False is now the only path: a non-ASCII record round-trips
    through `log` as raw UTF-8 glyphs, not \\uXXXX escapes."""
    env = dict(nb_env, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    content = "café ☕ mesure Δμ=0.3 数据"
    r = run_lnb(["note", content], env=env)
    assert r.returncode == 0, r.stderr
    entry_id = parse_noted(r.stdout)["id"]

    r2 = run_lnb(["log"], env=env)
    assert r2.returncode == 0, r2.stderr
    line = next(l for l in r2.stdout.splitlines() if entry_id in l)
    assert "café ☕" in line and "数据" in line   # raw glyphs, present verbatim
    assert "\\u" not in line                       # NOT ascii-escaped
    assert json.loads(line)["content"] == content  # lossless round-trip


# --- contract iii: log includes retracted entries AND tombstones -------------

def test_log_includes_tombstone_and_retracted(nb_env):
    """The highest-value pin (RT3, and the FLIP of the old -o-json exclusion):
    after a retract, `log` emits BOTH the retracted entry AND the
    `type=='_retract'` tombstone -- the exact OPPOSITE of the pre-redesign
    behavior. This is what makes the notebook auditable and lets jq compute
    liveness."""
    marker = "retract-json-marker-rj9"
    a = parse_noted(run_lnb(["note", f"first {marker}"], env=nb_env).stdout)["id"]
    b = parse_noted(run_lnb(["note", f"second {marker}"], env=nb_env).stdout)["id"]
    assert run_lnb(["retract", a, "--reason", "drop it"],
                   env=nb_env).returncode == 0

    objs = log_objs(nb_env)
    ids = [o["id"] for o in objs]
    assert a in ids                        # retracted entry STILL emitted
    assert b in ids                        # live sibling emitted
    assert any(o.get("type") == "_retract" and o.get("retracts") == a
               for o in objs)              # tombstone emitted


def test_retracts_is_scalar_id_string(nb_env):
    """A `_retract` tombstone's `retracts` is a SCALAR id string, not an array
    -- the live-view idiom relies on this (an array would silently stop
    matching in `IN($dead[])` and resurrect retracted rows)."""
    victim = parse_noted(run_lnb(["note", "scalar retracts target"],
                                 env=nb_env).stdout)["id"]
    assert run_lnb(["retract", victim, "--reason", "x"],
                   env=nb_env).returncode == 0
    tomb = next(e for e in read_entries(nb_env) if e.get("type") == "_retract")
    assert isinstance(tomb["retracts"], str)
    assert tomb["retracts"] == victim


def test_tombstone_has_no_context_or_content(nb_env):
    """A `_retract` tombstone carries exactly id/ts/writer/type/retracts/reason
    and NO context/content (documents the @tsv/projection-null cost of contract
    iii -- filter `select(.type!="_retract")` before projecting)."""
    victim = parse_noted(run_lnb(["note", "tombstone shape target"],
                                 env=nb_env).stdout)["id"]
    assert run_lnb(["retract", victim, "--reason", "why-it-died"],
                   env=nb_env).returncode == 0
    tomb = next(e for e in read_entries(nb_env) if e.get("type") == "_retract")
    assert set(tomb) == {"id", "ts", "writer", "type", "retracts", "reason"}
    assert "context" not in tomb
    assert "content" not in tomb
    assert tomb["reason"] == "why-it-died"


# --- the shipped live-view recipe: ships in --help AND works under real jq ----

@needs_jq
def test_live_view_jq_idiom_drops_retracted(nb_env):
    """Pipe `lnb log` through the EXACT recipe shipped in `--help` (via real
    jq): the retracted entry and every `_retract` line are dropped, the
    survivor remains. Asserts BOTH that help contains the recipe verbatim AND
    that the recipe works, so the shipped copy can never silently rot."""
    help_out = run_lnb(["--help"], env=nb_env).stdout
    assert RECIPE in help_out, "the canonical recipe must ship verbatim in --help"

    dead = parse_noted(run_lnb(["note", "to be retracted liveview", "+decision"],
                               env=nb_env).stdout)["id"]
    alive = parse_noted(run_lnb(["note", "survivor liveview", "+note"],
                                env=nb_env).stdout)["id"]
    assert run_lnb(["retract", dead, "--reason", "superseded"],
                   env=nb_env).returncode == 0

    log_out = run_lnb(["log"], env=nb_env)
    assert log_out.returncode == 0, log_out.stderr
    jq = run_jq(JQ_LIVE_FILTER, log_out.stdout, slurp=True)
    assert jq.returncode == 0, jq.stderr
    survivors = [json.loads(l) for l in jq.stdout.splitlines() if l.strip()]
    ids = [o["id"] for o in survivors]
    assert alive in ids
    assert dead not in ids                                   # retracted -> gone
    assert all(o.get("type") != "_retract" for o in survivors)  # tombstones gone


@needs_jq
def test_naive_filter_keeps_retracted_footgun(nb_env):
    """The DOCUMENTED fail-open, pinned as a DECISION (not a regression): a
    naive `log | jq 'select(.type=="decision")'` STILL returns a retracted
    decision (exit 0). Compose the live view first when you mean live."""
    dead = parse_noted(run_lnb(["note", "retracted decision footgun", "+decision"],
                               env=nb_env).stdout)["id"]
    assert run_lnb(["retract", dead, "--reason", "superseded"],
                   env=nb_env).returncode == 0

    log_out = run_lnb(["log"], env=nb_env)
    assert log_out.returncode == 0, log_out.stderr
    jq = run_jq('select(.type=="decision")', log_out.stdout)
    assert jq.returncode == 0, jq.stderr
    ids = [json.loads(l)["id"] for l in jq.stdout.splitlines() if l.strip()]
    assert dead in ids, "naive filter returns the dead decision -- the footgun"


def test_live_view_recipe_pinned_in_readme_and_skill():
    """The canonical live-view jq filter ships in THREE places -- lnb.py's
    docstring/--help (pinned by test_live_view_jq_idiom_drops_retracted),
    README.md and SKILL.md. Pin the two doc copies too, so none can silently
    drift from the recipe lnb actually ships. jq-independent (pure text
    presence), so it runs everywhere."""
    for doc in ("README.md", "SKILL.md"):
        text = (WORKTREE / doc).read_text(encoding="utf-8")
        assert RECIPE_DEAD_BIND in text, (
            f"{doc} is missing the live-view $dead binding "
            f"'{RECIPE_DEAD_BIND}' -- it drifted from the shipped recipe")
        assert RECIPE_CORE in text, (
            f"{doc} is missing the live-view select core '{RECIPE_CORE}' "
            f"-- it drifted from the shipped recipe")


def test_live_view_recipe_core_ships_in_help(nb_env):
    """Sanity: the same core fragments this test pins in README/SKILL are the
    ones lnb ships in --help -- so pinning the docs pins the real recipe, not a
    fabricated string that exists only in the test."""
    help_out = run_lnb(["--help"], env=nb_env).stdout
    assert RECIPE_DEAD_BIND in help_out
    assert RECIPE_CORE in help_out


# --- fail-closed write boundary (a note may not forge system records) --------

def test_note_cannot_forge_tombstone_via_extras(nb_env):
    """`note "x" type=_retract retracts=<victim>` must be rejected, not appended
    -- otherwise it reads back as a tombstone and silently deletes the victim."""
    r = run_lnb(["note", "victim entry forgetest"], env=nb_env)
    victim = parse_noted(r.stdout)["id"]
    attack = run_lnb(
        ["note", "innocent looking", "type=_retract", f"retracts={victim}"],
        env=nb_env)
    assert attack.returncode != 0
    assert "reserved" in attack.stderr.lower()
    # victim survives; no tombstone was written (verified via bare `log`)
    assert any(o["id"] == victim for o in log_objs(nb_env))
    assert not any(e.get("type") == "_retract" for e in read_entries(nb_env))


def test_note_cannot_forge_tombstone_via_type_sigil(nb_env):
    """Reserving the `_` namespace closes the sigil path too:
    `note "x" +_retract retracts=<id>`."""
    r = run_lnb(["note", "victim two forgetest2"], env=nb_env)
    victim = parse_noted(r.stdout)["id"]
    attack = run_lnb(
        ["note", "sneaky", "+_retract", f"retracts={victim}"], env=nb_env)
    assert attack.returncode != 0
    assert any(o["id"] == victim for o in log_objs(nb_env))


def test_note_cannot_overwrite_core_fields(nb_env):
    for bad in ("id=FAKE", "ts=1999", "writer=mallory", "content=hijack"):
        r = run_lnb(["note", "core overwrite attempt", bad], env=nb_env)
        assert r.returncode != 0, f"{bad} should be rejected"
        assert "reserved" in r.stderr.lower()


def test_note_with_only_kv_and_no_content_fails_loudly(nb_env):
    """A stray `note tags=x` (content forgotten) must fail, not log
    content='tags=x'."""
    r = run_lnb(["note", "tags=mae"], env=nb_env)
    assert r.returncode != 0
    assert "nothing to log" in r.stderr.lower()
    assert not writer_jsonl(nb_env).exists() or read_entries(nb_env) == []


def test_note_content_with_equals_and_spaces_is_preserved(nb_env):
    r = run_lnb(["note", "lr=3e-4 gave the best val marker_eq"], env=nb_env)
    assert r.returncode == 0, r.stderr
    entry = read_entries(nb_env)[-1]
    assert entry["content"] == "lr=3e-4 gave the best val marker_eq"
    assert "lr" not in entry  # not misparsed as an extra


def test_note_rejects_output_flag(nb_env):
    """`-o/--output` no longer exists: `note ... -o json` is rejected (now via
    parse()'s generic "unexpected argument"), not a silent no-op."""
    r = run_lnb(["note", "note with output flag", "-o", "json"], env=nb_env)
    assert r.returncode != 0
    assert r.stderr.strip() != ""


# --- retract owns its liveness guard (scan() no longer excludes retracted) ---

def test_retract_already_retracted_reports_clearly(nb_env):
    # NOW MORE LOAD-BEARING: scan() no longer excludes retracted rows, so this
    # exercises retract's own `id not in retracted` liveness guard directly.
    r = run_lnb(["note", "retract twice target"], env=nb_env)
    victim = parse_noted(r.stdout)["id"]
    assert run_lnb(["retract", victim, "--reason", "first"],
                   env=nb_env).returncode == 0
    r2 = run_lnb(["retract", victim, "--reason", "second"], env=nb_env)
    assert r2.returncode != 0
    assert "already retracted" in r2.stderr.lower()


def test_retract_double_is_already_retracted(nb_env):
    """Re-retracting an already-retracted id -> 'already retracted', and NO
    second tombstone is appended (retract's `id not in retracted` guard)."""
    victim = parse_noted(run_lnb(["note", "double retract target"],
                                 env=nb_env).stdout)["id"]
    assert run_lnb(["retract", victim, "--reason", "first"],
                   env=nb_env).returncode == 0
    assert n_tombstones(nb_env) == 1
    r2 = run_lnb(["retract", victim, "--reason", "second"], env=nb_env)
    assert r2.returncode != 0
    assert "already retracted" in r2.stderr.lower()
    assert n_tombstones(nb_env) == 1, "no second tombstone for a dead id"


def test_retract_of_tombstone_id_is_no_entry(nb_env):
    """RT1 -- the single most likely real bug. Retracting a `_retract` row's
    OWN id must be 'no entry', never a second tombstone. This exercises the
    `type != "_retract"` guard DISTINCTLY from the already-retracted path: a
    fresh tombstone's own id is NOT in `retracted`, so a partial guard that
    only checks `id not in retracted` FAILS this test (it would substring-match
    the tombstone and append a second tombstone at exit 0)."""
    victim = parse_noted(run_lnb(["note", "live entry to retract tombstonetest"],
                                 env=nb_env).stdout)["id"]
    assert run_lnb(["retract", victim, "--reason", "first"],
                   env=nb_env).returncode == 0
    assert n_tombstones(nb_env) == 1

    tomb = next(e for e in read_entries(nb_env) if e.get("type") == "_retract")
    tomb_id = tomb["id"]
    assert tomb_id not in ({victim})            # sanity: distinct id

    # attempt to retract the tombstone's own id -- both the full id and its
    # random suffix (the substring-match attack surface).
    for target in (tomb_id, tomb_id.split("-")[-1]):
        r = run_lnb(["retract", target, "--reason", "attack"], env=nb_env)
        assert r.returncode != 0, f"retract {target!r} (a tombstone id) must fail"
        assert "no entry" in r.stderr.lower(), r.stderr

    # NO second tombstone was appended by either attempt.
    assert n_tombstones(nb_env) == 1


def test_suggest_id_never_surfaces_tombstone(nb_env):
    """A near-miss retract suggests a LIVE id, never a `_retract` tombstone id
    nor a retracted (dead) id (retract passes `live` to suggest_id). A surfaced
    tombstone would also print an empty snippet, since tombstones have no
    content."""
    live_id = parse_noted(run_lnb(["note", "the live survivor suggesttest"],
                                  env=nb_env).stdout)["id"]
    victim = parse_noted(run_lnb(["note", "to retract suggesttest"],
                                 env=nb_env).stdout)["id"]
    assert run_lnb(["retract", victim, "--reason", "x"],
                   env=nb_env).returncode == 0
    tomb = next(e for e in read_entries(nb_env) if e.get("type") == "_retract")

    # a near-miss typo that is not a substring of any id -> "no entry" + a
    # "did you mean" suggestion drawn only from live entries.
    bogus = live_id + "Z"
    r = run_lnb(["retract", bogus, "--reason", "typo"], env=nb_env)
    assert r.returncode != 0
    assert "did you mean" in r.stderr.lower(), r.stderr
    assert tomb["id"] not in r.stderr, "suggestion surfaced a tombstone id"
    assert victim not in r.stderr, "suggestion surfaced a retracted (dead) id"


def test_retract_rejects_output_flag_and_does_not_retract(nb_env):
    """`retract <id> -o json` must be rejected LOUDLY (not silently swallowed
    while the retraction still happens) -- the fail-closed boundary. The entry
    must survive; verified via bare `log | grep`, NOT `log ... -o json` (which
    would fail-close on the stray arg and MASK whether the retract happened)."""
    marker = "retract-outflag-marker-rof7"
    r = run_lnb(["note", f"keep me {marker}"], env=nb_env)
    victim = parse_noted(r.stdout)["id"]

    attack = run_lnb(["retract", victim, "-o", "json", "--reason", "sneaky"],
                     env=nb_env)
    assert attack.returncode != 0, "retract must reject -o, not swallow it"
    assert attack.stderr.strip() != ""

    # the entry was NOT retracted: it still surfaces and no tombstone exists.
    r2 = run_lnb(["log"], env=nb_env)
    assert victim in r2.stdout
    assert not any(e.get("type") == "_retract" for e in read_entries(nb_env))


# --- verb set & help: bare lnb is help, sql/find are unknown -----------------

def test_bare_lnb_is_help_not_read(nb_env):
    """Bare `lnb` prints __doc__ help on stdout (incl. the live-view line),
    NOT a read of the notebook: no JSONL, exit 0."""
    run_lnb(["note", "an entry so the notebook is non-empty barehelp"],
            env=nb_env)
    r = run_lnb([], env=nb_env)
    assert r.returncode == 0
    assert "lnb note" in r.stdout and "lnb log" in r.stdout
    assert RECIPE in r.stdout
    assert "barehelp" not in r.stdout                  # not a read of the entry

    def is_record(line):
        line = line.strip()
        try:
            obj = json.loads(line)
        except Exception:
            return False
        return isinstance(obj, dict) and "id" in obj
    assert not any(is_record(l) for l in r.stdout.splitlines())


def test_help_names_three_verbs_and_ships_recipe(nb_env):
    for flag in (["--help"], ["-h"], ["help"]):
        r = run_lnb(flag, env=nb_env)
        assert r.returncode == 0, r.stderr
        assert "lnb note" in r.stdout
        assert "lnb log" in r.stdout
        assert "lnb retract" in r.stdout
        assert RECIPE in r.stdout
        # the retired/renamed verbs are gone from help
        assert "lnb find" not in r.stdout
        assert "lnb sql" not in r.stdout


@pytest.mark.parametrize("cmd", ["sql", "find"])
def test_retired_and_renamed_commands_are_unknown(nb_env, cmd):
    """`sql` (retired) and `find` (renamed to `log`) are unknown commands."""
    run_lnb(["note", "entry for unknown-command test"], env=nb_env)
    r = run_lnb([cmd, "whatever"], env=nb_env)
    assert r.returncode != 0
    assert "unknown command" in r.stderr.lower()
