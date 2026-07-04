# `lnb find -o json` — design spec (from the iter-1 persona debate)

**Verdict: GO (unanimous 3/3). Decided scope: `find` ONLY.**

`note`, `sql`, `retract` do NOT get `-o json` (see rationale below). `note` was a
contested 2-1 rider (conway for; karpathy/geohot against) — declined for minimality:
the generated id is already printed on the `noted ...` line and the record is already
on disk as JSON.

## Constraints (unanimous across karpathy / geohot / conway)

1. **JSONL** — one compact JSON object per live entry, one per line. NOT a JSON array.
   Mirrors the on-disk `.lnb/*.jsonl` shape: `find -o json` reads as "a `cat` that
   resolves filters + retractions + dedup." Streamable; never buffers the whole scan.
2. **`ensure_ascii=False`** — match `append()`'s on-disk encoding exactly.
3. **Records verbatim** — emit the stored dict as-is. `extras` stay as **top-level
   typed keys**, NOT flattened into a blob (the `sql` TSV failure) and NOT re-nested
   under a wrapper key. This losslessness is the entire justification.
4. **Invariant shape — kill the human DWIM in json mode.** The id-fragment unique-match
   path must NOT emit a special full-dump object; it emits ONE normal JSONL line (one
   row selected). Zero matches emit ZERO lines. Consumers never branch on output shape.
   (The table-vs-full-entry DWIM stays a human-only affordance.)
5. **No 80-char content truncation** in json mode (that truncation is display-only).
6. **Retraction semantics unchanged** — emit only live records; `scan()` already drops
   tombstones, so a `_retract` line never appears.
7. **stdout / stderr discipline** — stdout carries ONLY JSONL. EVERY diagnostic (empty
   notebook, no matches, "N ids match", malformed-line skip, "no notebook found")
   stays on stderr and never interleaves with the JSON.
8. **Empty result is not an error** — zero lines on stdout, exit 0.
9. **Opt-in only** — `-o json` / `--output json`. Default stays human. Do NOT auto-switch
   to json on a non-TTY pipe (a human's `find | less` must stay human).
10. **Reject an unknown `-o` value loudly**, naming the accepted vocabulary (`json`),
    in Conway's diagnose-and-prescribe style. Flag is local to `find` — `note`/`retract`/
    `sql` must not silently accept `-o json` and no-op.
11. **Guardrails** — stdlib `json` only (already imported); no new runtime dependency;
    lnb.py stays ONE file.

## Validity check the goal requires
`lnb find <UNIQUE_ID> -o json | python -m json.tool >/dev/null; echo rc=$?` → rc=0.
A unique id selects exactly one row → one JSONL line → one JSON object → `json.tool` OK.
Multi-row `find -o json` is JSONL (deliberately), validated per-line, not as one document.
