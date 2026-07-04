# lnb — minimal lab notebook

A git-tracked, append-only research notebook. **A notebook is not a database to
configure; it is a directory of JSONL files you scan.** One file, `lnb.py`,
stdlib only, no index, no schema, no config.

> This is the experimental *minimal* line (`dev-min`), distilled from the full
> `lab-notebook` via a three-persona design review. See `design/`.

## Install

```bash
uv tool install .        # or: pipx install .
# or just run it: python3 lnb.py ...
```

## Use

```bash
# Log a thought. Context defaults to the git repo name; type defaults to "note".
lnb note "MAE with 75% masking spends most capacity on background" \
    --type observation @ssl/pretraining tags=mae,masking

# Recall
lnb find                    # 10 most recent
lnb find masking            # case-insensitive / regex over content
lnb find @ssl/pretraining   # filter by context
lnb find --type dead-end    # filter by type
lnb find 73caceb5           # a unique id fragment -> that entry in full
lnb sql "SELECT type, count(*) FROM entries GROUP BY type"

# Retract (logical, append-only): writes a tombstone; the original line stays.
lnb retract 20260704T163806-73caceb5 --reason "superseded"
```

At the shell, `--type X` / `--context Y` are the safe forms. The `+type` /
`@context` sigils are DWIM shorthands (`@` is shell-safe; quote `+`/`#` if your
shell needs it).

## Model

- **Storage:** `.lnb/<writer>.jsonl` — one JSON object per line, the git-tracked
  source of truth. Each writer gets their own file, so there are no merge
  conflicts. Writer = `$LNB_WRITER`, else `$USER`.
- **Discovery:** `$LNB_DIR`, else the nearest `.lnb/` walking up from the cwd,
  else `./.lnb` is created on the first `note`.
- **Query:** every read scans every line (milliseconds for a realistic notebook).
  No index to build, migrate, or invalidate.
- **Retract:** appends `{"type":"_retract","retracts":<id>,"reason":...}`; readers
  drop retracted ids. The original entry and the tombstone both remain as the
  audit trail. No un-retract — restore from a JSONL backup.
- **Append is fail-closed:** one `\n`-terminated write, flushed and `fsync`ed; a
  malformed trailing line is skipped with a warning, never rewritten.

## Entry

```json
{"id":"20260704T163806-73caceb5","ts":"2026-07-04T16:38:06-07:00",
 "writer":"cong","context":"ssl/pretraining","type":"observation",
 "content":"MAE with 75% masking spends most capacity on background",
 "tags":"mae,masking"}
```

Types are free-form; common ones: `observation, decision, dead-end, question,
milestone, note`. Any `key=value` becomes an entry field — unvalidated by design.

## The honest tradeoff

Every read is O(all entries) and nothing validates fields — fine at 10^4 entries,
a lie at 10^7. `lnb sql` (a throwaway SQLite rebuilt per call) is the reviewable
next rung if you outgrow scanning.
