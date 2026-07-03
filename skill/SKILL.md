---
name: lnb
description: "Log and query research notebook entries. Use when the user asks to record observations, decisions, dead-ends, questions, or milestones. Also use when they ask what we've tried, what decisions were made, or want to search the notebook. Trigger on: 'log this', 'note this', 'record', 'what have we tried', 'what did we decide', 'search the notebook', 'document progress', 'what dead-ends', 'what's open'."
user-invocable: true
argument-hint: "<log|recall|retract|init|onboard> [args...]"
---

# Lab Notebook (`/lnb`)

A structured, append-only research notebook.

## Command Dispatch

Parse the first word of `$ARGUMENTS`:

| Command | Action |
|---------|--------|
| `log` | Go to **Log** |
| `recall` | Go to **Recall** |
| `retract` | Go to **Retract** |
| `init` | Go to **Init** |
| `onboard` | Go to **Onboard** |
| *(empty or unrecognized)* | Show usage: `/lnb log <what to log>`, `/lnb recall <question>`, `/lnb retract <id>`, `/lnb init`, `/lnb onboard` |

Before executing any command (except `init` and `onboard` themselves), run the **Environment Check**. If it fails, offer the user a choice between **Init** (project-local) and **Onboard** (global), then return to the original command.

---

## Environment Check

The CLI handles notebook discovery automatically (`$LAB_NOTEBOOK_DIR` → nearest `.lnb.env` walking up from CWD → error). Verify it can find a notebook:

```bash
lab-notebook schema >/dev/null 2>&1 && echo "OK" || echo "NO_NOTEBOOK"
```

- If `OK` → notebook found. Proceed.
- If `NO_NOTEBOOK` → tell the user:

> No notebook found. You can:
> - `/lnb init` — set up a project-local notebook in this directory
> - `/lnb onboard` — set up a global notebook

Wait for the user's choice. After setup completes, proceed directly to the original command.

### Index messages are normal

The index is rebuilt incrementally on read. The first command after new entries (or a retraction) may print one of these to **stderr** — they are progress messages, not errors:

- `Index updated: +N entries` / `-N retracted` — incremental ingest applied
- `Index rebuilt: N entries` — full rebuild (first use, or after a schema change)

`stdout` still carries clean query results, so commands that pipe or capture stdout are unaffected. You can ignore these lines or relay them to the user as-is.

---

## Init

Project-local notebook setup. The CLI creates a `.lnb.env` file in the current directory and a notebook directory (`./.lnb` by default, or exactly the path you give). All `lab-notebook` commands then auto-discover it.

### Step 1: Pick a notebook path

Ask:

> Where should this project's notebook live? (default: `./.lnb` in the current directory)

### Step 2: Run init

```bash
lab-notebook init [<path>]
```

Omit `<path>` to use the default `./.lnb`. An explicit `<path>` is used verbatim — nothing is appended. The CLI will:
- Create the notebook directory (`./.lnb`, or exactly `<path>`) with `entries/`, `artifacts/`, `schema.yaml`, `.gitignore`
- Write `.lnb.env` in the current directory
- Refuse if `.lnb.env` already exists — pass `--force` to overwrite it

If the command fails, show the error and stop.

### Step 3: Suggest .gitignore entries

The CLI already suggests `.gitignore` additions in its output. If the user wants to add them, offer to append:

```
.lnb.env
.lnb/
```

Wait for the user's preference before modifying `.gitignore`.

### Step 4: Check for a shadowing env var

`$LAB_NOTEBOOK_DIR` takes precedence over `.lnb.env`. If it's already exported (e.g. from a shell profile or a prior `/lnb onboard`), the freshly-written project `.lnb.env` will be silently ignored.

```bash
[ -n "$LAB_NOTEBOOK_DIR" ] && echo "SHADOWED: $LAB_NOTEBOOK_DIR" || echo "OK"
```

- If `OK` → proceed to Step 5.
- If `SHADOWED: <path>` → tell the user:

> `$LAB_NOTEBOOK_DIR` is exported and points at `<path>`. The new project notebook won't be used until you remove the export from your shell profile and **restart Claude Code** — each `bash` tool call inherits env vars from Claude's own process, so unsetting it in your terminal (or inside a single tool call) won't propagate to the verify step below.

Wait for them to restart and re-run (or confirm they want the global notebook to keep winning) before proceeding.

### Step 5: Verify

```bash
lab-notebook schema
```

This prints the full schema (unlike the quiet env-check above); that verbosity
is intentional here — it confirms the notebook is live and shows what fields are
available. If it succeeds, tell them:

> Project notebook ready. Any `lab-notebook` command in this directory (or subdirectories) will use this notebook.

---

## Onboard

One-time global setup. Go step by step and confirm before writing anything.

### Step 1: Pick a notebook path

Ask:

> Where should your notebook live? (e.g. `~/lab-notebook`, `/proj/myproject/notebook`)

If the user declines, is unsure, or doesn't answer:

> No problem — I can use the global default at `~/lab-notebook`. Want me to use that?

- If they agree: use `LAB_NOTEBOOK_DIR="$HOME/lab-notebook"` and continue.
- If they decline again: tell them the skill needs `$LAB_NOTEBOOK_DIR` to work, and offer to run `/lnb onboard` whenever they're ready. Stop here.

Also ask (optional):

> What writer ID should be used for your entries? (defaults to `$USER` = your current username)

Set `LAB_NOTEBOOK_WRITER` only if they provide a value different from `$USER`.

### Step 2: Initialize the notebook

First check if the notebook already exists. The CLI uses the given path verbatim, so the notebook lives directly at `$LAB_NOTEBOOK_DIR`:

```bash
test -f "$LAB_NOTEBOOK_DIR/schema.yaml" && echo "EXISTS" || echo "NEW"
```

- If `EXISTS`: tell the user "Notebook already initialized — skipping init." Proceed to Step 3.
- If `NEW`: run (`--force` lets init overwrite any `.lnb.env` left in the current directory, which global setup discards anyway):

```bash
lab-notebook init "$LAB_NOTEBOOK_DIR" --force
```

This creates the notebook at `$LAB_NOTEBOOK_DIR` and writes `.lnb.env` in the current directory. Since this is global setup, the `.lnb.env` file is not needed — clean it up:

```bash
rm -f .lnb.env
```

If the init command fails (non-zero exit), tell the user:

> Init failed. Check that the path is writable: `ls -ld "<path>"`

Do not proceed past this step on failure.

### Step 3: Set and persist the environment

The notebook lives at exactly `<chosen path>` (the same value set in Step 1), so the env var already points there. Confirm it's exported in the current session:

```bash
export LAB_NOTEBOOK_DIR="<chosen path>"
```

Then tell the user to add to their shell profile (or a project `.env`) for future sessions:

```bash
export LAB_NOTEBOOK_DIR="<chosen path>"
export LAB_NOTEBOOK_WRITER="<username>"  # optional, defaults to $USER
```

### Step 4: Confirm

```bash
lab-notebook schema
```

This prints the full schema (unlike the quiet env-check in the Environment
Check); that verbosity is intentional here. Show the output to the user. If this succeeds, tell them they're ready. If it fails, tell them init may not have completed successfully and suggest re-running `/lnb onboard`.

---

## Log

Emit an entry to the notebook. Content comes from `$ARGUMENTS` after the `log` keyword.

If no content was provided after `log`, ask:

> What would you like to log?

### Step 1: Infer the entry type

From the content itself, infer the most likely type before running `lab-notebook schema`:

- Mentions trying/failing/didn't work/broke → `dead-end`
- Mentions deciding/going with/choosing/we'll use → `decision`
- Mentions done/merged/shipped/working/complete → `milestone`
- Mentions wondering/should we/open question/what if → `question`
- Anything else (a measurement, finding, behavior) → `observation`

Propose the inferred type to the user:

> Looks like a **decision**. Correct, or different type?

Only run `lab-notebook schema` if they want to see available types.

### Step 2: Pick the context

Use `topic/subtopic` slugs, e.g. `ssl/pretraining`, `data/loading`. Check existing contexts:

```bash
lab-notebook contexts
```

Infer from conversation context when possible. Ask only if unclear.

### Step 3: Check content length

Before drafting, assess whether the content is too long to fit in a single entry (longer than ~3 sentences, or contains raw output, data dumps, or multiple distinct ideas).

If it is, present the user with options **before** drafting the emit command:

> This seems like a lot for one entry. How would you like to handle it?
>
> **A — Reference a file**: Keep the entry concise and attach the details via `--artifacts <path>[,<path>...]` (one or more comma-separated paths). Best when the bulk is data, code, or output that lives in a file.
>
> **B — Break it up**: Split into separate `/lnb log` calls — one per distinct insight or decision. Best when there are multiple ideas bundled together.
>
> **C — Distill it**: Summarize to 1-3 sentences capturing the key insight. Best when it's one dense thought that can be compressed.

Wait for the user's choice before proceeding.

If user picks **A**, ask: "What file should I attach?" Then use the path as `--artifacts <path>` in Step 4.

### Step 4: Draft and confirm

```bash
lab-notebook emit \
    --context <context> --type <type> \
    [--artifacts <path>] \
    [-f <field>=<value>] \
    "content"
```

Schema fields (those declared in `schema.yaml`, e.g. `repo`, `tags`) are passed
with the repeatable `-f KEY=VALUE` flag; `list` fields take a comma-separated
value (`-f tags=a,b`). Use `--extra key=value` only for one-off fields not in
the schema.

**Content**: should be 1-3 sentences with specific numbers, file names, or commit hashes. If you chose option C above, the content is already distilled — use it as-is.

Present for confirmation, showing the actual notebook path:

> **Notebook**: `<actual value of $LAB_NOTEBOOK_DIR>`
> **Type**: decision | **Context**: ssl/pretraining
>
> ```bash
> lab-notebook emit --context ssl/pretraining --type decision [--artifacts <path>] "..."
> ```
>
> OK to emit, or adjust?

Only execute after the user confirms.

---

## Recall

Search the notebook to answer a question. Query comes from `$ARGUMENTS` after the `recall` keyword.

No confirmation needed — reads are non-destructive.

**If no query was provided**, show recent entries as a default:

```bash
lab-notebook sql \
  "SELECT ts, type, context, substr(content,1,100) FROM entries ORDER BY ts DESC LIMIT 10"
```

**Otherwise**, pick the approach based on the question:

- Open-ended or keyword question → keyword search
- "What have we tried / decided / hit?" → SQL filtered by type
- "What contexts / topics exist?" → list contexts
- Counts, date ranges, comparisons → SQL

### Keyword search

```bash
lab-notebook search "<keywords>"
lab-notebook search "<keywords>" --type dead-end
lab-notebook search "<keywords>" --context ssl/pretraining
```

### SQL (for structured questions)

```bash
lab-notebook sql \
  "SELECT ts, type, substr(content,1,80) FROM entries ORDER BY ts DESC LIMIT 10"

lab-notebook sql \
  "SELECT context, ts, substr(content,1,80) FROM entries WHERE type='dead-end' ORDER BY ts DESC"
```

### List contexts

```bash
lab-notebook contexts
```

### Summarize

Present a concise answer citing specific entries (timestamp, context, type). Do not dump raw output.

SQL queries use `substr(content,1,80)` by default for scanning. If the truncated preview isn't enough to answer the question, re-run without `substr()` to see the full content:

```bash
lab-notebook sql \
  "SELECT ts, type, context, content FROM entries WHERE type='dead-end' ORDER BY ts DESC"
```

**If results are empty**, suggest recovery:

> Nothing found. Try:
> - Broader keywords (fewer or shorter terms)
> - Drop `--type` or `--context` filters
> - `/lnb recall` with no args to see recent entries
> - `lab-notebook contexts` to see what topics exist

---

## Retract

Forget an entry that is wrong or out of date. The id to retract comes from `$ARGUMENTS` after the `retract` keyword.

Retract is **append-only and logical**, not a delete. It writes a tombstone record to the notebook — it does **not** edit or remove the original JSONL line. On the next indexed read the target row is removed from `index.sqlite` (and from full-text search); the original entry and the tombstone (with its reason) both stay in `entries/*.jsonl` as the audit trail of what was forgotten and why. The deletion survives `rebuild`. There is no un-retract command — recovery means restoring the JSONL from a backup and rebuilding. Any writer may retract any entry by id.

Because this changes what the notebook returns, **always confirm before executing** (like Log).

### Step 1: Identify the entry

If an id was provided after `retract`, use it.

If no id was provided (or the user describes the entry instead of giving an id), help them find it using the **Recall** approaches above, then ask which one to retract:

```bash
lab-notebook search "<keywords>"
# or, to see ids directly:
lab-notebook sql "SELECT id, ts, type, context, substr(content,1,80) FROM entries ORDER BY ts DESC LIMIT 10"
```

Confirm the exact id with the user before continuing.

### Step 2: Get a reason

A reason is **required**. If the user didn't give one, ask:

> Why are you retracting this? (recorded in the tombstone, e.g. "superseded by a corrected measurement")

### Step 3: Confirm and execute

Present for confirmation, showing the entry being retracted:

> **Notebook**: `<actual value of $LAB_NOTEBOOK_DIR>`
> Retracting `<id>` — *<one-line preview of the entry>*
>
> ```bash
> lab-notebook retract <id> --reason "<why>"
> ```
>
> This is a logical delete (the original line stays as an audit trail). OK to retract?

Only execute after the user confirms:

```bash
lab-notebook retract <id> --reason "<why>"
```

### Step 4: Handle the result

- Success prints `[retracted] <id>  (<reason>)`. Tell the user it's done and that the entry will no longer appear in searches or SQL queries.
- If the CLI prints `Error: entry '<id>' not found (already retracted or never existed)`, **surface that** rather than treating it as success. The id was mistyped, already retracted, or never written — offer to look it up via **Recall**.

---

## Templates

```bash
# List available templates
lab-notebook template

# Initialize with a bundled template
lab-notebook init "$LAB_NOTEBOOK_DIR" --template ml-experiment-log

# Initialize with a schema file shipped by the current project
lab-notebook init --template-path ./my-schema.yaml
```

Run `lab-notebook template` (shown above) to see the bundled templates; the list is generated from the schemas the installed package ships, so it's always current.

Use `--template-path` when your project ships its own schema file rather than relying on a bundled template. Mutually exclusive with `--template`.

$ARGUMENTS
