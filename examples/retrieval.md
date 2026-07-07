# Retrieving records without `lnb log`

`lnb log` is the blessed read path: it emits every record as JSONL, globally
sorted by `ts`, and you filter with `jq`. But sometimes you want to read the
notebook's `.lnb/<writer>.jsonl` files **directly** — when `lnb` isn't on `PATH`
(a CI runner, a fresh clone, a container that only has `jq` + coreutils), when
you're already inside a raw-file shell pipeline, or when a notebook has grown big
enough that a full `lnb log` scan on every query is wasteful.

This is a cookbook of those direct-access recipes. Every command was run against a
real lnb notebook and the block under it is the **actual** output. They use only
tools you already have — coreutils `grep`, `jq`, `sqlite3`, and Python's standard
library — and none of them modify the notebook.

> The examples run against a small notebook whose `.lnb/` holds two writer files
> (`cong.jsonl`, `ana.jsonl`), 11 records, one of them retracted. Commands use the
> relative glob `.lnb/*.jsonl`; run them from the directory that contains `.lnb/`.

## Two things `lnb log` does that raw files don't

Reading the files yourself is not quite the same as `lnb log`, and two differences
bite if you ignore them:

1. **The files are not globally time-ordered.** Each `<writer>.jsonl` is
   `ts`-ascending on its own (append-only), but `.lnb/*.jsonl` expands in glob
   (alphabetical) order, so concatenating them interleaves writers by *filename*,
   not by time. `lnb log` does a k-way merge to restore global `ts` order; if you
   need that, add `jq -s 'sort_by(.ts)'` yourself.
2. **Naive filters are fail-open on retractions.** A retraction never deletes
   anything — it appends a `{"type":"_retract","retracts":"<id>",...}` tombstone.
   So `jq 'select(.type=="decision")'` still returns records that were later
   retracted. The one correct "what's true now" view is the live-view filter under
   *Filter to what's current* below; build on it whenever retractions matter.

(`jq`'s default output is pretty-printed across multiple lines and is no longer
valid JSONL. Add `-c` whenever the output feeds another tool, a file, or a diff.)

## Point-lookup by id — `grep`

You have an id (copied from a previous `lnb log` line, or from another record's
`retracts` field) and want that one record back. `grep` finds it without parsing a
single line of JSON it doesn't have to:

```bash
grep -h -F '20260707T211151-a3087b5a' .lnb/*.jsonl
```

```json
{"id": "20260707T211151-a3087b5a", "ts": "2026-07-07T21:11:51+00:00", "writer": "cong", "context": "ssl/pretraining", "type": "decision", "content": "switch to 50% masking for the ablation", "tags": "mae,masking"}
```

Both flags are load-bearing: `-F` matches the id as a **literal** string (an id or a
content phrase can contain regex metacharacters like `.` that would otherwise
mismatch), and `-h` suppresses the `filename:` prefix `grep` adds when the glob
expands to more than one file — that prefix corrupts the JSON on the line. Pipe the
result to `jq .` to pretty-print it; swap `-h` for `-l` if instead you just want to
know *which* writer file holds the record.

When you only remember the short hex **tail** of an id (what people actually
memorize), match that — but anchor the pattern to the `id` field so a tail that also
appears inside some record's free-text `content` or `tags` can't give a false hit:

```bash
grep -h -E '"id": "[^"]*a3087b5a"' .lnb/*.jsonl
```

```json
{"id": "20260707T211151-a3087b5a", "ts": "2026-07-07T21:11:51+00:00", "writer": "cong", "context": "ssl/pretraining", "type": "decision", "content": "switch to 50% masking for the ablation", "tags": "mae,masking"}
```

## Make `jq` fast at scale: `grep` as a pre-filter

`jq` has to build a parse tree for every line it reads. On a large notebook, most of
that work is wasted on lines that can't possibly match. `grep -F` is a pure literal
substring scan — no JSON parsing — so use it to throw away the lines that can't match
*before* `jq` does the field-aware work on the survivors:

```bash
grep -h -F 'AUROC' .lnb/*.jsonl | jq -c 'select(.type=="observation")'
```

```json
{"id":"20260707T211152-9ceb94eb","ts":"2026-07-07T21:11:52+00:00","writer":"cong","context":"ssl/eval","type":"observation","content":"baseline AUROC 0.91 on holdout","metric":"auroc","value":"0.91"}
```

The speedup is real: on a 1,000,016-record / 191 MB notebook, this same shape —
`grep -F '<phrase>' | jq 'select(.type==…)'` for a phrase occurring in a single
record — ran in **0.06 s** versus **~2.5 s** for the equivalent bare
`jq 'select(.content|contains("<phrase>"))'` full scan: about **40× faster**,
because `grep` discarded all but the one matching line before `jq` parsed anything.
(`grep` alone can only match raw bytes; the moment you need field logic — a specific
`.type`, a comparison — `jq` still does that part. For a literal id, `grep` alone
from *Point-lookup by id* is enough.) If you are doing *many* lookups rather than
one, build the index under *Read-heavy bursts*.

## Project fields to a table — `jq`

The most common "just let me look at this" query: pick a few fields and print them
as columns. Add `-s | sort_by(.ts)` for one globally chronological table (see caveat
1); drop it and the rows come out in glob-then-file order instead:

```bash
jq -s -r 'sort_by(.ts) | .[] | [.id, .type, .content] | @tsv' .lnb/*.jsonl
```

```
20260707T211151-a3e05202	observation	MAE with 75% masking spends most capacity on background
20260707T211151-a3087b5a	decision	switch to 50% masking for the ablation
20260707T211152-20477314	observation	detector gain drift measured at 0.3%/hr
20260707T211152-f83902e9	decision	adopt rolling gain recalibration every 30 min
20260707T211152-ac879345	milestone	first end-to-end run passed acceptance
20260707T211152-65e0c285	dead-end	temperature logging is off by one sample
20260707T211152-8b03433b	note	reviewed the masking sweep
20260707T211152-f53d2780	question	why does val loss plateau at epoch 40?
20260707T211152-9ceb94eb	observation	baseline AUROC 0.91 on holdout
20260707T211213-7b0d9416	dead-end	learning rate 3e-4 diverges after 2 epochs
20260707T211213-a278c01f	_retract	
```

This ordering matches `lnb log` exactly. Without `-s | sort_by(.ts)`, ana's
`21:11:52` rows print before cong's `21:11:51` row — backwards in time.

## Filter to what's current — `jq`

This is the one recipe you *must* reproduce yourself once you leave `lnb log`,
because a naive `jq 'select(.type=="dead-end")'` is fail-open — it returns records
that were later retracted. Here that filter returns **two** dead-ends:

```bash
jq -c 'select(.type=="dead-end")' .lnb/*.jsonl
```

```
{"id":"20260707T211152-65e0c285","ts":"2026-07-07T21:11:52+00:00","writer":"ana","context":"calib/detector","type":"dead-end","content":"temperature logging is off by one sample","tags":"logging"}
{"id":"20260707T211213-7b0d9416","ts":"2026-07-07T21:12:13+00:00","writer":"cong","context":"ssl/pretraining","type":"dead-end","content":"learning rate 3e-4 diverges after 2 epochs","tags":"lr,stability"}
```

The second one (`…7b0d9416`, "learning rate 3e-4 diverges…") was retracted. The
canonical **live view** — the same idiom `lnb --help` prints, but applied to the
raw files — reads every `_retract.retracts` id first, then drops both the tombstones
and the records they retract. Layer your real filter on top of it:

```bash
jq -s '
  (map(select(.type=="_retract").retracts)) as $dead
  | sort_by(.ts)
  | .[]
  | select(.type != "_retract" and (.id | IN($dead[]) | not))
  | select(.type=="dead-end")
' .lnb/*.jsonl
```

```json
{
  "id": "20260707T211152-65e0c285",
  "ts": "2026-07-07T21:11:52+00:00",
  "writer": "ana",
  "context": "calib/detector",
  "type": "dead-end",
  "content": "temperature logging is off by one sample",
  "tags": "logging"
}
```

Only the live dead-end survives; the retracted one is correctly gone. Copy this as
the base of any "give me the current X" query. (Naive filters on `.context` or any
other field are fail-open for the same reason.)

## Most-recent-N and time-range — `jq`

`ts` is ISO 8601 with a fixed offset, so lexicographic string comparison **is**
chronological comparison — no date parsing needed. That single fact powers two
shapes. Most recent 3 records globally (slurp, sort, slice):

```bash
jq -sc 'sort_by(.ts) | .[-3:] | .[]' .lnb/*.jsonl
```

```
{"id":"20260707T211152-9ceb94eb","ts":"2026-07-07T21:11:52+00:00","writer":"cong","context":"ssl/eval","type":"observation","content":"baseline AUROC 0.91 on holdout","metric":"auroc","value":"0.91"}
{"id":"20260707T211213-7b0d9416","ts":"2026-07-07T21:12:13+00:00","writer":"cong","context":"ssl/pretraining","type":"dead-end","content":"learning rate 3e-4 diverges after 2 epochs","tags":"lr,stability"}
{"id":"20260707T211213-a278c01f","ts":"2026-07-07T21:12:13+00:00","writer":"cong","type":"_retract","retracts":"20260707T211213-7b0d9416","reason":"re-ran with warmup; lr=3e-4 is fine"}
```

Everything from a time onward is a stateless per-line predicate (no `-s` needed):

```bash
jq -c 'select(.ts >= "2026-07-07T21:12:00+00:00")' .lnb/*.jsonl
```

```
{"id":"20260707T211213-7b0d9416","ts":"2026-07-07T21:12:13+00:00","writer":"cong","context":"ssl/pretraining","type":"dead-end","content":"learning rate 3e-4 diverges after 2 epochs","tags":"lr,stability"}
{"id":"20260707T211213-a278c01f","ts":"2026-07-07T21:12:13+00:00","writer":"cong","type":"_retract","retracts":"20260707T211213-7b0d9416","reason":"re-ran with warmup; lr=3e-4 is fine"}
```

## Read-heavy bursts: a throwaway SQLite index

Everything above re-scans the JSONL on every query — the right trade-off for a
one-off question. But when you are about to fire **many** lookups against the same
snapshot of a large notebook, build a disposable SQLite index once and let its
B-tree do the seeking.

This database is **not a second source of truth.** The `.lnb/*.jsonl` files stay
authoritative. The DB is derived (built entirely from them), rebuildable (delete it
and re-run), git-ignored (add `*.db` to `.gitignore`), and safe to `rm` at any
time — `lnb` never reads it, writes it, or knows it exists. Put it *outside* `.lnb/`.

Load each JSONL line into a `jsonb` column (binary JSON — smaller and faster to
decode than text `json`) and index only the fields you'll seek on. A tiny
stdlib-only loader is the most robust way to get raw lines in:

> **Prerequisite:** `jsonb()` needs `sqlite3` ≥ 3.45.0 (Jan 2024) — check with
> `sqlite3 --version`. On an older `sqlite3`, swap `JSONB`/`jsonb(?)` for
> `JSON`/`json(?)` (text storage): every recipe below works unchanged, just
> without the binary-format speed and size win.

```bash
python3 - .lnb notebook.db <<'PY'
import glob, json, sqlite3, sys
src, db = sys.argv[1], sys.argv[2]
con = sqlite3.connect(db)
con.execute("DROP TABLE IF EXISTS rec")
con.execute("CREATE TABLE rec(data JSONB)")
for path in sorted(glob.glob(f"{src}/*.jsonl")):
    with open(path, encoding="utf-8") as fh:
        lines = (ln.strip() for ln in fh)
        con.executemany("INSERT INTO rec(data) VALUES(jsonb(?))",
                        ([ln] for ln in lines if ln))
con.execute("CREATE INDEX idx_id   ON rec(json_extract(data,'$.id'))")
con.execute("CREATE INDEX idx_ts   ON rec(json_extract(data,'$.ts'))")
con.execute("CREATE INDEX idx_type ON rec(json_extract(data,'$.type'))")
con.commit()
print(f"loaded {con.execute('SELECT count(*) FROM rec').fetchone()[0]} records")
PY
```

```
loaded 11 records
```

Now a point-lookup seeks the `idx_id` B-tree instead of scanning:

```bash
sqlite3 notebook.db "SELECT json_extract(data,'\$.type'), json_extract(data,'\$.content')
  FROM rec WHERE json_extract(data,'\$.id')='20260707T211151-a3087b5a';"
```

```
decision|switch to 50% masking for the ablation
```

`EXPLAIN QUERY PLAN` confirms the index is used (a `SEARCH … USING INDEX`, not a
`SCAN`):

```bash
sqlite3 notebook.db "EXPLAIN QUERY PLAN SELECT json_extract(data,'\$.content')
  FROM rec WHERE json_extract(data,'\$.id')='20260707T211151-a3087b5a';"
```

```
QUERY PLAN
`--SEARCH rec USING INDEX idx_id (<expr>=?)
```

Retracted records and tombstones are ordinary rows — liveness is still the reader's
job, exactly as with `lnb log`, so apply the live-view logic in SQL if you need it.
When the burst is done, throw the index away:

```bash
rm notebook.db
```

**Why it earns its place.** On the 1,000,016-record / 191 MB notebook, building the
index (load + three indexes) took **5.4 s**, and each id lookup then took **~1.5 ms**
— versus **~2.2 s** for a `jq 'select(.id==…)'` full scan and **~5.7 s** for
`lnb log | jq 'select(.id==…)'` (which sorts all million records first). The build
costs about the same as a *single* `lnb log | jq` call, so it pays for itself in
**under one lookup** against that path — or about **three** against a bare `jq` full
scan (5.4 s build vs 3 × 2.2 s). Past break-even, each lookup is ~1,500–3,800×
cheaper. For just a handful of lookups, don't bother — use `grep` or `jq` above.
