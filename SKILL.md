---
name: lnb
description: "Record and recall research notebook entries. Use when the user asks to record an observation, decision, dead-end, question, or milestone, or asks what we've tried / decided / hit. Trigger the WRITE (Note) action on: 'note this', 'log this', 'record'. Trigger the READ (Recall) action on: 'recall', 'what have we tried', 'what did we decide', 'search the notebook'."
user-invocable: true
argument-hint: "<note|recall> [text...]"
---

# Lab Notebook (`/lnb`)

A git-tracked, append-only research notebook. Two skill verbs: **note** (write)
and **recall** (read). NB: the *CLI* read verb is now `lnb log` (it emits the
whole notebook as JSONL) — so at this skill layer "log" is never a write; use
**note**. The CLI (`lnb`) auto-discovers the notebook (nearest `.lnb/` walking
up, or `$LNB_DIR`) and creates one on the first `note`. No setup step.

Parse the intent of `$ARGUMENTS`. A request to record something — first word
`note`/`log`/`record`, a phrasing like "log this"/"note this", or clearly a new
observation/decision/dead-end → **Note** (the WRITE action; runs `lnb note …`).
A request to recall — first word `recall`, "what did we / have we …", or a
question about what was tried / decided → **Recall** (the READ action; runs
`lnb log | jq …`). If ambiguous, show the two usages below. Note the split:
the user saying "log this" means WRITE an entry (→ **Note**), whereas the *CLI*
verb `lnb log` READS the whole notebook (see above) — at this skill layer "log"
is never the write command, `lnb note` is.

## Note

Append one entry. **Distill first, then confirm, then write** — the log is
append-only, so a wrong entry stays as history.

1. **Distill** the content to 1–3 sentences with specifics (numbers, file names,
   commit hashes). If the user's text is longer, propose the distilled version.
2. **Infer type and context**, and state them so a wrong guess is visible:
   - type — `dead-end` (tried/failed/broke), `decision` (chose/going with),
     `milestone` (done/shipped/merged), `question` (open/should we), else
     `observation`. Pass it with `--type` (or `+type`).
   - context — a `topic/subtopic` slug; defaults to the git repo name. Pass with
     `@context` or `--context`.
3. **Confirm** the exact command, then run it:

   ```bash
   lnb note "distilled content" --type decision @ssl/pretraining
   ```

   Extra fields are `key=value` args (e.g. `gpu_hours=12`). Values may contain
   spaces — quote the whole token (`"cause=two words"`), and note a quoted arg
   beginning `key=` is a field, not content. Prefer `--type`/
   `--context` flags in scripted calls — the `+type`/`@context` sigils are for
   humans typing at the shell. `key=value` may not reuse the fields lnb owns
   (`id`, `ts`, `writer`, `context`, `type`, `content`) or start with `_`; lnb
   rejects those, so just use `--context`/`--type` for those.

   A common use is attaching **references** — local files or a URL — as a
   comma-joined value; lnb stores the string verbatim and jq unpacks it on read
   (see Recall). `artifacts` is not special, just an ordinary field:

   ```bash
   lnb note "reviewed the masking sweep" \
       artifacts="/cwd/sweep.md,https://docs.google.com/spreadsheets/d/<id>"
   ```

   After logging, **surface the echoed entry id** to the user (the `noted <id>`
   line) — it's what a later `retract` needs.

To **retract** a wrong entry, confirm the id and a reason, then:

```bash
lnb retract <id> --reason "superseded by a corrected measurement"
```

## Recall

Reads are non-destructive — no confirmation needed. `lnb log` emits **all**
entries as JSONL (oldest first, uncapped, including `_retract` tombstones); `jq`
does every selection.

```bash
lnb log                                              # all entries, oldest first
lnb log | jq 'select(.type=="dead-end")'             # filter by type
lnb log | jq 'select(.content|test("masking";"i"))'  # regex over content
lnb log | jq 'select(.context=="ssl/pretraining")'  # filter by context
lnb log | jq 'select(.id|test("a7f2c3d1"))'          # fetch one entry by id
lnb log | jq -r '[.ts,.type,.content]|@tsv'          # project to a table
lnb log | jq -r 'select(.artifacts).artifacts|split(",")[]'  # unpack an artifacts list
lnb log | jq -s 'group_by(.type)|map({(.[0].type):length})'  # aggregate counts

# LIVE view — drop tombstones and the entries they retract (also in `lnb --help`);
# the plain filters above are FAIL-OPEN and return retracted entries too:
lnb log | jq -s 'map(select(.type=="_retract").retracts) as $dead
  | .[] | select(.type != "_retract" and (.id | IN($dead[]) | not))'
```

`| tail` trims to the most recent; `| tac` flips to newest-first. Present a
concise answer **citing specific entries** (id, context, type) — don't dump raw
output. When "live" matters (excluding retracted entries), compose the live-view
idiom first; a naive type/context filter still returns retracted rows.

$ARGUMENTS
