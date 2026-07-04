# Minimal `lnb` — Design Spec (Iteration 1)

Synthesized from a three-persona `/agent-persona` review (Fable 5) of the current
`lab-notebook` (~1481 LOC Python + ~429-line SKILL.md + ~307-line README).

Reviewers: **Karpathy** (smallest thing that works end-to-end, hold it all in your
head), **Geohot** (hard complexity budget, shrink concept count, push variation into
data, fail-closed boundary), **Conway** (DWIM the common case, table-driven,
diagnostics as first-class).

---

## The reframe

> **A lab notebook is not a database to configure. It is a git-tracked append-only
> log you scan.** Everything that survives serves *capture* or *recall*; everything
> cut served *configuration* or *caching*.

Once you say that, `store.py` (541 lines of index coherence) and `schema.py` (234
lines of config-driven DDL) evaporate.

## Complexity budget (hard cap, tinygrad-style — concepts, not characters)

- **`lnb.py` — one file, stdlib only, target ≤ 250 lines.**
- **3 concepts:** (1) append-only JSONL log, (2) tombstones as ordinary appended
  records, (3) query = scan the log.
- **Deps: `[]`.** No PyYAML, no SQLite in the core path. (`sqlite3` is stdlib and
  used only by the optional `sql` escape hatch, rebuilt from scratch — never a
  persistent index.)

## What gets cut (unanimous across all three lenses)

| Cut | Why |
|-----|-----|
| SQLite+FTS5 **persistent** index | It's a cache for a dataset that fits in RAM (10²–10⁴ entries, ~1KB each). ~500 lines of incremental byte-offset ingest, `_ingest_state`, truncation fence + rebuild fallback, `user_version` migration, external-content FTS, 3 sync triggers, `recursive_triggers` footguns, atomic tmp rebuilds — all cache-coherence code for a cache nobody needs. Scan the file: the coherence problem ceases to exist. |
| `schema.yaml` + validation + field types + `-f`/`--extra` split | JSON is already schemaless. The "schema" is just the keys writers use. Kill the 234-line `schema.py`, `build_sql` DDL codegen, reserved-name checks, and the `yaml` dep. |
| Templates (`template`, bundled schemas) | Die with `schema.yaml`. |
| Bash completion (`complete.py`, 205 lines) | Polish, not product. |
| `init` / `.lnb.env` discovery walk + onboard | Git-style: walk up for `.lnb/`, override with `$LNB_DIR`. First write auto-creates `./.lnb/`. Deletes the sourcing ritual and the "shadowing env var" pathology in SKILL.md. |
| `sql`, `schema`, `contexts`, `rebuild` as distinct commands | `contexts` is a dict-reduce during the scan; `rebuild` is a lever for a mechanism that no longer exists; `sql` survives only as an optional throwaway escape hatch. |
| The `Notebook` class | Free functions + `if cmd == ...` dispatch on `sys.argv[1]`. |

## What survives (earns its place)

- **Per-writer JSONL files** (`.lnb/<writer>.jsonl`) — files-as-IPC, eliminates
  merge conflicts for multi-agent writers. Costs one `glob("*.jsonl")`.
- **Tombstone-retract** — preserves append-only; ~10 lines (readers collect
  retracted ids into a set and skip).

---

## Command surface (4 commands — DWIM)

### `lnb note "content" [#type] [@context] [k=v ...]`   *(the whole write path)*
- **Context** defaults to the enclosing git repo basename (`git rev-parse
  --show-toplevel`) or cwd basename; override with `@context` token or `--context`.
- **Type** defaults to `note`; override with `#type` token or `--type`. (No silent
  keyword inference in the first cut — guessing that *writes* to an append-only log
  is the one ambiguity Conway refuses. Inference can be an explicit, echoed opt-in
  later.)
- Extra `k=v` args go straight into the entry dict, unvalidated.
- **Echoes back the recorded entry** (id, context, type) so a wrong default is
  immediately visible.
- First `note` in a tree with no `.lnb/` **creates** `./.lnb/` — `init` dissolves.

### `lnb find [terms...] [@context] [#type]`   *(the whole read path — absorbs show/search/contexts)*
- **No args** → last 10 entries, oldest→newest (tail-style: the newest line lands
  next to your prompt): `id | ts | context | type | content[:80]`.
- A single term that is a **unique id-prefix** → full key/value dump of that entry
  (`lnb find a7f2`, not a 24-char id).
- Otherwise **case-insensitive substring/regex** over content, filtered by
  `@context` / `#type` if given.

### `lnb retract <id> --reason "why"`
- **Fail-closed:** verify the id (unique-prefix ok) exists in the scan *before*
  appending. If not, error with a "did you mean" suggestion — never mint a dangling
  tombstone.
- Appends `{"type": "_retract", "retracts": id, "reason": ...}`.

### `lnb sql "query"`   *(optional escape hatch — Karpathy's ladder rung 2 = Conway's power hatch)*
- Rebuilds a **throwaway** SQLite (temp file or `:memory:`) from scratch on *every*
  invocation — columns `id, ts, writer, context, type, content` + a JSON blob for
  extras — runs the query, prints, discards. No persistence, no incremental ingest,
  no triggers, no migration. ~35 lines. Kept because it's cheap and agents use SQL
  well for recall; iteration 3 review decides if it truly earns its place.

---

## Single load path

```
load(dir) -> list[dict]:
    glob .lnb/*.jsonl, parse each line (skip malformed trailing lines w/ stderr warn),
    collect retracted ids, drop them, sort by ts.
```
One function feeds `find`, `retract` validation, and `sql`.

## Fail-closed boundary (Geohot — the only place rigor is non-negotiable, ~15 lines)

1. **Append is all-or-nothing:** open `"a"`, write one complete `\n`-terminated
   line, `flush()` + `os.fsync()`.
2. **No code path opens a JSONL for anything but append or read.** Retract appends,
   never edits.
3. **Readers fail closed per-line:** a malformed/partial trailing line is skipped
   with a stderr warning — never crashes a read, never auto-"repaired".
4. **`retract` verifies the target id exists** before appending.

## Diagnostics as first-class (Conway — minimal must NOT cut these)

- **Empty notebook:** `Notebook at <dir> is empty — nothing logged yet. Start with:
  lnb note "..."` — never a bare `LAB_NOTEBOOK_DIR is not set` env lecture.
- **Nothing found:** say what *does* exist + hypothesize: `No matches for "<q>" (312
  entries, 4 contexts). Fewer terms usually helps: try lnb find <one-term>`.
- **Bad id:** prefix-match live ids and suggest: `No entry 'a7f3'. Did you mean
  a7f2c3d1 (3 days ago: "MAE with 75%...")?`

## The agent skill (`SKILL.md`) — target 60–80 lines (from 429)

Most of the current 429 lines compensate for CLI ceremony (env checks, init/onboard
flows, prose inference rules, empty-result recovery). With the DWIM CLI, those move
into the tool. The minimal skill keeps only genuine agent-layer value:
- **two verbs — `log` and `recall`** (retract folded into `log`'s flow);
- confirm-before-write, distill-to-1-3-sentences, cite entries when summarizing.

---

## LOC scorecard (target)

| | Current | Minimal (target) |
|--|--:|--:|
| CLI / core | 1481 (4 modules) | ~305 (`lnb.py`, 1 file) |
| SKILL.md | 429 | ~70 |
| README | 307 | ~75 |
| Deps | `pyyaml` + build tooling | `[]` (stdlib) |

> **Budget reconciliation (post-build, honest).** The ≤250 line estimate was for
> the pure CRUD core. The realized `lnb.py` is ~305 lines: a shared `parse()`
> recovered the ~25 lines of duplicated arg-loops the review flagged, but that
> saving was *reinvested* into the two things the same review said minimal must
> **not** cut — the fail-closed write boundary (~6 lines) and Conway-grade
> diagnostics (~30 lines: nothing-found vocabulary, id-based retract suggestion,
> already-retracted detection). Per geohot's own anti-code-golf rule, the target
> is a low **concept** count (4 commands, one `scan`, one `parse`, one `append`),
> not a low line count — and that is met. Golfing the diagnostics away to hit 250
> would trade the tool's UX for a number.

## The one honest tradeoff (owned, not hidden)

We lose structured/typed querying and schema enforcement: a typo'd field name
silently becomes a new key; every read is O(entire notebook). That's fine at 10⁴
entries and a lie at 10⁷ — the `sql` rung and, if ever needed, an explicit rebuilt
index are the reviewable next rungs. The minimal version **trusts its writers**.

---

## Build plan (feeds Iteration 2)

1. `lnb.py` — dispatch, `load()`, `note`, `find`, `retract`, `sql`, diagnostics,
   fail-closed append. Stdlib only.
2. `pyproject.toml` — one console-script `lnb = lnb:main`, `dependencies = []`.
3. `SKILL.md` — ~70 lines, verbs `log`/`recall`.
4. `README.md` — ~60 lines.
5. `tests/` — end-to-end: note→find round-trip, retract removes from find,
   malformed-line skip, empty-notebook diagnostic, id-prefix match.

---

## Iteration 3 outcome (second `/agent-persona` review of the built code)

Three Fable-5 reviewers (geohot/conway/karpathy lenses) audited `lnb.py`. What
they found and what changed:

- **[SEVERE, fixed] Fail-open write boundary.** `note` did `record.update(extras)`
  after setting core fields, so `note "x" type=_retract retracts=<id>` produced a
  record that read back as a **tombstone** — one note could silently delete another
  entry (and be consumed itself, blanking the visible notebook). Fix: reject any
  extras key in the core/system set and reserve the `_` namespace (type + keys),
  loudly. Precedence-flipping was rejected as insufficient (the `+_retract` sigil
  path). Pinned by 3 regression tests.
- **[HIGH, fixed] Silent-write on forgotten content.** `note tags=mae` logged
  `content="tags=mae"`. Fix: parse `key=value` before the positional, so a stray
  k=v with no content fails loudly; quoted content containing `=` (with spaces) is
  still preserved.
- **[MED, fixed] retract "did you mean" was id-blind** (searched prose content).
  Now `difflib`-suggests the nearest live id and distinguishes *already retracted*
  from *never existed*.
- **[MED, done] Budget via one shared `parse()`** for note/find; `--help` prints
  the module docstring (single-sourced, no duplicate USAGE block).
- **[MED, done] Sigils:** `+type`/`--type` are canonical in docs and echo-back
  (`+note (default)` flags a defaulted type so a shell-eaten `#decision` is
  caught); `#` stays parsed but undocumented ("liberal input, strict docs").
- **[LOW, done] Footgun:** id-fragment lookup now requires a digit, so hex-words
  ("dead", "cafe") stay content searches; ambiguous fragments list matches.
- **Kept (unanimous do-not-change):** single `scan()` read path, fsync single-line
  append, per-writer JSONL, tombstone retract, throwaway `:memory:` `sql`.

Result: 30 passing tests, `deps=[]`, ~305 SLOC / 4 concepts.
