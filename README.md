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

## Schema Configuration

Each notebook has a `schema.yaml` that defines entry types and custom fields.
`lab-notebook init` generates a default one. Run `lab-notebook template` to see
bundled templates, or use `lab-notebook init --template <name>` to start with a
specific one.

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

### `init [path] [--template NAME]`

Initialize a project-local notebook. Creates `.lnb/` in the current directory
(or at `path` if given) with `entries/`, `artifacts/`, `schema.yaml`, and
`.gitignore`. Also writes `.lnb.env` in the current directory for automatic
notebook discovery. Use `--template` to pick a schema template (default:
`research-notebook`). Pass `--template` with no value to list available
templates.

### `emit --context X --type Y [--artifacts ...] [--field ...] [--extra K=V] "content"`

Write a notebook entry. Required: `--context`, `--type`, content.
Custom field flags (e.g. `--repo`, `--tags`) are generated from `schema.yaml`.

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
