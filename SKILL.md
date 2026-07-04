---
name: lnb
description: "Log and recall research notebook entries. Use when the user asks to record an observation, decision, dead-end, question, or milestone, or asks what we've tried / decided / hit. Trigger on: 'log this', 'note this', 'record', 'what have we tried', 'what did we decide', 'search the notebook'."
user-invocable: true
argument-hint: "<log|recall> [text...]"
---

# Lab Notebook (`/lnb`)

A git-tracked, append-only research notebook. Two verbs: **log** and **recall**.
The CLI (`lnb`) auto-discovers the notebook (nearest `.lnb/` walking up, or
`$LNB_DIR`) and creates one on first `log`. No setup step.

Parse the first word of `$ARGUMENTS`: `log` → **Log**, `recall` → **Recall**,
anything else → show the two usages below.

## Log

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

   Extra fields are `key=value` args (e.g. `gpu_hours=12`). Prefer `--type`/
   `--context` flags in scripted calls — the `+type`/`@context` sigils are for
   humans typing at the shell. `key=value` may not reuse the fields lnb owns
   (`id`, `ts`, `writer`, `context`, `type`, `content`) or start with `_`; lnb
   rejects those, so just use `--context`/`--type` for those.

   After logging, **surface the echoed entry id** to the user (the `noted <id>`
   line) — it's what a later `retract` needs.

To **retract** a wrong entry, confirm the id and a reason, then:

```bash
lnb retract <id> --reason "superseded by a corrected measurement"
```

## Recall

Reads are non-destructive — no confirmation needed.

```bash
lnb find                      # 10 most recent, newest last
lnb find masking ratio        # case-insensitive / regex over content
lnb find @ssl/pretraining     # filter by context
lnb find --type dead-end      # (or +dead-end) filter by type
lnb find a7f2c3d1             # a unique id fragment prints that entry in full
lnb sql "SELECT type, count(*) FROM entries GROUP BY type"   # aggregates
```

Present a concise answer **citing specific entries** (id, context, type). Don't
dump raw output. If nothing matches, the CLI already suggests how to broaden —
relay that.

$ARGUMENTS
