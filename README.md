# lab-notebook

Append-only lab notebook for tracking research across multiple repos.
JSONL files are the git-tracked source of truth. SQLite + FTS5 is a
disposable query index rebuilt on demand. Schema is configurable via
`schema.yaml`.

## Installation

```bash
# From GitHub
uv tool install git+https://github.com/carbonscott/lab-notebook

# From a local clone
uv tool install /path/to/lab-notebook

# Or with pip
pip install git+https://github.com/carbonscott/lab-notebook
```

To update after code changes:

```bash
uv tool install --force git+https://github.com/carbonscott/lab-notebook
```

## Claude Code skill

This repo ships the Claude Code skill that powers the `/lnb` slash command at
[`skill/SKILL.md`](skill/SKILL.md). It wraps the CLI with guided flows for
logging, recalling, and retracting entries. The skill is the agent-facing
surface of this CLI and is kept in sync with it here (rather than in a separate
repo), so a change to the CLI's command surface should update `skill/SKILL.md`
in the same PR.

To use it locally, symlink it into your skills directory:

```bash
ln -sfn "$(pwd)/skill" ~/.claude/skills/lab-notebook-skill
```

The install directory name (`lab-notebook-skill`) is independent of the skill's
internal `name: lnb` in `skill/SKILL.md` — that `name` is what powers the `/lnb`
command, so the two differing is expected.

The skill calls the `lab-notebook` CLI, so install that first (see above).

## Quick Start

```bash
# 1. Initialize a project-local notebook (creates .lnb/ and .lnb.env)
cd /path/to/my-project
lab-notebook init

# 2. Source .lnb.env (sets LAB_NOTEBOOK_DIR and LAB_NOTEBOOK_WRITER)
source .lnb.env

# 3. Write an entry (CLI args come from schema.yaml)
lab-notebook emit --context maxie/ssl-comparison --type observation \
    --tags mae,masking \
    "MAE with 75% masking spends most capacity on background."

# 4. Query
lab-notebook sql "SELECT ts, type, substr(content,1,80) FROM entries ORDER BY ts DESC LIMIT 10"

# 5. Full-text search
lab-notebook search "masking"
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LAB_NOTEBOOK_DIR` | Yes | — | Path to notebook data directory |
| `LAB_NOTEBOOK_WRITER` | No | `$USER` | Writer ID for entries |

### Notebook discovery precedence

When the CLI resolves which notebook to use, it checks sources in this order
(first match wins):

1. `$LAB_NOTEBOOK_DIR` environment variable (if set and non-empty)
2. Nearest `.lnb.env` walking up from the current directory (closest wins,
   stops at `$HOME` or `/`)

Explicit `LAB_NOTEBOOK_DIR=... lab-notebook ...` always wins, so one-shot
overrides work as expected. For normal project use, `source .lnb.env` after
`cd`ing into a project so the env var is set for your shell.

## Schema Configuration

Each notebook has a `schema.yaml` that defines entry types and custom fields.
`lab-notebook init` generates a default one. Run `lab-notebook template` to see
bundled templates, or use `lab-notebook init --template <name>` to start with a
specific one. To start from a schema file shipped by your own project, use
`lab-notebook init --template-path ./my-schema.yaml`.

The default template (`research-notebook`) includes basic fields. You can add
more to your `schema.yaml`:

```yaml
types:
  - observation
  - decision
  - dead-end
  - question
  - milestone

fields:
  repo:       {type: text}
  branch:     {type: text}
  tags:       {type: list}
  dataset:    {type: text, fts: true}   # additional field
  gpu_hours:  {type: real}              # additional field
  num_nodes:  {type: integer}           # additional field
```

**Field types:** `text`, `integer`, `real`, `list` (list is comma-separated on
the CLI, stored as a JSON array in JSONL).

**Full-text search:** Add `fts: true` to include a field in the FTS5 index.
`content` is always indexed.

**Built-in fields:** `artifacts` is always available on `emit` (comma-separated paths, stored as a JSON array). It does not need to be declared in `schema.yaml` and cannot be redeclared.

**`--extra`:** For one-off fields not in the schema, use `--extra key=value`
(repeatable). These are stored in the JSONL and in a JSON blob column in SQLite.
Note: extra values are always stored as strings — use schema fields for typed data.

Run `lab-notebook rebuild` after changing `schema.yaml`.

## Commands

### `init [path] [--template NAME | --template-path PATH] [--force]`

Initialize a project-local notebook. With no argument, creates `./.lnb/` in the
current directory; given a `path`, creates the notebook at exactly that path
(the path is used verbatim — nothing is appended). Either way the directory is
populated with `entries/`, `artifacts/`, `schema.yaml`, and `.gitignore`. Also
writes `.lnb.env` in the current directory for automatic notebook discovery; if
`.lnb.env` already exists, `init` refuses unless `--force` is given. Use
`--template` to pick a bundled schema template (default: `research-notebook`).
Pass `--template` with no value to list available templates. Use
`--template-path PATH` to load a schema from an arbitrary YAML file on disk
(mutually exclusive with `--template`).

### `emit --context X --type Y [--artifacts ...] [--field ...] [--extra K=V] "content"`

Write a notebook entry. Required: `--context`, `--type`, content.
Custom field flags (e.g. `--repo`, `--tags`) are generated from `schema.yaml`.

### `retract ID --reason "why"`

Forget an entry that is wrong or out of date. Both `ID` and `--reason` are
required. Retract appends a tombstone record to the notebook — it does **not**
edit or remove the original line. On the next indexed read the target row is
deleted from `index.sqlite` (and from full-text search); the tombstone itself
is never returned as an entry.

Because the index is rebuilt from the JSONL, the deletion survives `rebuild`.
The retraction is logical, not physical: the original entry and the tombstone
(with its reason) both remain in `entries/*.jsonl` as the audit trail of what
was forgotten and why. To recover a retracted entry, restore it from a JSONL
backup and `rebuild` — there is no un-retract command. Any writer may retract
any entry by id.

### `sql "query"`

Run raw SQL against the index. Auto-rebuilds if `index.sqlite` is missing.

### `search "query" [--context X] [--type Y]`

Full-text search with optional filters.

### `schema`

Print table structure and example queries.

### `rebuild`

Regenerate `index.sqlite` from `schema.yaml` and all `entries/*.jsonl` files.

### `contexts`

List active research contexts with entry counts and date ranges.

### `template [name] [--force]`

List or apply bundled schema templates. With no argument, lists available
templates. With a name, copies that template to the current notebook's
`schema.yaml` (requires `--force` if `schema.yaml` already exists).
Run `lab-notebook rebuild` afterward if entries exist.

## Data Layout

```
my-project/
├── .lnb/
│   ├── entries/
│   │   ├── cong.jsonl
│   │   ├── intern-alice.jsonl
│   │   └── agent-claude-01.jsonl
│   ├── artifacts/        # tracked; store files referenced via --artifacts
│   ├── schema.yaml       # field definitions and entry types
│   ├── index.sqlite      # gitignored, rebuilt on demand
│   └── .gitignore
└── .lnb.env              # points to .lnb/, source this
```

Each writer gets their own JSONL file. No merge conflicts.

## JSONL Format

```json
{
  "id": "20260321T143022-a7f2",
  "ts": "2026-03-21T14:30:22",
  "writer_id": "cong",
  "context": "maxie/ssl-comparison",
  "type": "observation",
  "repo": "research-lrn091",
  "branch": "phase0/data-loading",
  "tags": ["mae", "masking"],
  "artifacts": ["research-lrn091:results/S01.csv"],
  "content": "MAE with 75% masking spends most capacity on background."
}
```

A `retract` appends a tombstone record instead of an entry. Its `type` is
`_retract`, `retracts` is the id being forgotten, and `reason` is why. It is a
control record — it deletes its target from the index and is never indexed as
an entry itself:

```json
{
  "id": "20260322T091500-c3d1",
  "ts": "2026-03-22T09:15:00",
  "writer_id": "cong",
  "type": "_retract",
  "retracts": "20260321T143022-a7f2",
  "reason": "superseded by a corrected measurement"
}
```
