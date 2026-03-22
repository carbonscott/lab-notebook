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
# 1. Create a notebook directory and initialize it
mkdir /path/to/my-notebook
lab-notebook init /path/to/my-notebook

# 2. Source the .env (sets LAB_NOTEBOOK_DIR and LAB_NOTEBOOK_WRITER)
source /path/to/my-notebook/.env

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
`lab-notebook init` generates a default one. See `schema.example.yaml` in the
repo for the full reference.

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
  artifacts:  {type: list}
  dataset:    {type: text, fts: true}
  gpu_hours:  {type: real}
  num_nodes:  {type: integer}
```

**Field types:** `text`, `integer`, `real`, `list` (list is comma-separated on
the CLI, stored as a JSON array in JSONL).

**Full-text search:** Add `fts: true` to include a field in the FTS5 index.
`content` is always indexed.

**`--extra`:** For one-off fields not in the schema, use `--extra key=value`
(repeatable). These are stored in the JSONL and in a JSON blob column in SQLite.

Run `lab-notebook rebuild` after changing `schema.yaml`.

## Commands

### `init [path]`

Initialize a notebook in an existing directory (default: cwd). Creates
`entries/`, `schema.yaml`, `.gitignore`, and `.env`.

### `emit --context X --type Y [--field ...] [--extra K=V] "content"`

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

## Data Layout

```
my-notebook/
├── entries/
│   ├── cong.jsonl
│   ├── intern-alice.jsonl
│   └── agent-claude-01.jsonl
├── schema.yaml       # field definitions and entry types
├── index.sqlite      # gitignored, rebuilt on demand
├── .gitignore
└── .env
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
