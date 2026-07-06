# lnb — minimal lab notebook

A git-tracked, append-only research notebook. **lnb is a JSONL producer; `jq`
is the consumer.** A notebook is not a database to configure; it is a directory
of JSONL files you scan. One file, `lnb.py`, stdlib only, no index, no schema,
no config, no dependencies.

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

# Read: `log` emits the WHOLE notebook as JSONL (oldest first); jq selects.
lnb log                                       # every record, one JSON per line
lnb log | jq 'select(.type=="decision")'      # filter by type
lnb log | jq 'select(.content|test("masking";"i"))'   # regex over content
lnb log | jq 'select(.context=="ssl/pretraining")'    # filter by context

# The ONE canonical LIVE view -- drop tombstones and the entries they retract
# (also printed by `lnb --help`; naive filters above are fail-open on this):
lnb log | jq -s 'map(select(.type=="_retract").retracts) as $dead
  | .[] | select(.type != "_retract" and (.id | IN($dead[]) | not))'

# Retract (logical, append-only): writes a tombstone; the original line stays.
lnb retract 20260704T163806-73caceb5 --reason "superseded"
```

`lnb log` is uncapped and takes no arguments -- for a short view, pipe to
`| tail` (or `| tac` for most-recent-first).

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
  No index to build, migrate, or invalidate. `log` sorts on the ISO ts *string*
  and assumes a stable UTC offset (a single-timezone notebook); cross-offset
  instant ordering is a documented limitation.
- **Retract:** appends `{"type":"_retract","retracts":<id>,"reason":...}`. `log`
  emits **every** record, including `_retract` tombstones and the entries they
  retract — the consumer (jq) computes liveness with the idiom lnb ships in
  `--help`. The original entry and the tombstone both remain as the audit trail.
  No un-retract — restore from a JSONL backup.
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
Want a closed vocabulary? Wrap `lnb note` in a small validator of your own —
lnb ships none by design.
Values may contain spaces (quote the whole `key=value` token); a quoted arg
beginning `key=` is a field, not content — lead with the content string.

## The honest tradeoff

Every read is O(all entries) and nothing validates fields — fine at 10^4 entries,
a lie at 10^7. The escalation path when a scan outgrows the terminal is not a
second query language; it is `lnb log | jq '…'`, and for the full-power case,
`jq` directly over `.lnb/*.jsonl`.
