# lab-notebook

Append-only lab notebook for tracking research across multiple repos.
JSONL files are the git-tracked source of truth. SQLite + FTS5 is a
disposable query index rebuilt on demand.

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

# 3. Write an entry
lab-notebook emit --context maxie/ssl-comparison --type observation \
    "MAE with 75% masking spends most capacity on background."

# 4. Query
lab-notebook sql "SELECT ts, type, substr(content,1,80) FROM entries ORDER BY ts DESC LIMIT 10"
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LAB_NOTEBOOK_DIR` | Yes | — | Path to notebook data directory |
| `LAB_NOTEBOOK_WRITER` | No | `$USER` | Writer ID for entries |

## Commands

### `init [path]`

Initialize a notebook in an existing directory (default: cwd). Creates
`entries/`, `.gitignore`, and `.env`.

### `emit --context X --type Y [options] "content"`

Write a notebook entry. Required: `--context`, `--type`, content.

Options: `--repo`, `--branch`, `--tags` (comma-separated), `--artifacts` (comma-separated).

### `sql "query"`

Run raw SQL against the index. Auto-rebuilds if `index.sqlite` is missing.

### `search "query" [--context X] [--type Y]`

Full-text search with optional filters.

### `schema`

Print table structure and example queries.

### `rebuild`

Regenerate `index.sqlite` from all `entries/*.jsonl` files.

### `contexts`

List active research contexts with entry counts and date ranges.

## Entry Types

| Type | Use for |
|------|---------|
| `observation` | "I saw/measured/noticed this" |
| `decision` | "We chose X because Y" |
| `dead-end` | "X failed because Y, don't retry" |
| `question` | "We don't know X yet" |
| `milestone` | "X is done/working/merged" |

## Data Layout

```
my-notebook/
├── entries/
│   ├── cong.jsonl
│   ├── intern-alice.jsonl
│   └── agent-claude-01.jsonl
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
