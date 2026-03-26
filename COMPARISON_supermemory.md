# lab-notebook vs supermemory — Comparison

## TL;DR

These projects solve **fundamentally different problems** despite both dealing with
"memory" for AI workflows:

| | **lab-notebook** | **supermemory** |
|---|---|---|
| **Purpose** | Append-only research log for experiments | Memory/context engine for AI assistants |
| **Target user** | Researchers running ML experiments | AI product builders & end-users |
| **Scope** | Single CLI tool (~670 LOC) | Full-stack SaaS platform (~80k LOC) |
| **License** | (see repo) | MIT |
| **Language** | Python | TypeScript + Python SDKs |
| **Data model** | JSONL files + SQLite FTS5 index | Cloud-hosted memory graph + vector DB |
| **Deployment** | Local, Git-tracked | Cloudflare Workers, hosted API |

---

## 1. What Each Project Does

### lab-notebook

A **minimal, Git-friendly CLI** for logging research entries (observations, decisions,
dead-ends, milestones). Each writer appends to their own JSONL file, eliminating
merge conflicts. A disposable SQLite+FTS5 index is rebuilt on demand for querying.

Key commands: `init`, `emit`, `search`, `sql`, `contexts`, `schema`, `rebuild`.

### supermemory

A **full-stack AI memory platform** (self-described as "#1 on LongMemEval, LoCoMo, and
ConvoMem benchmarks"). It automatically extracts facts from AI conversations, builds
user profiles, handles contradictions/temporal changes, and delivers context back to
AI assistants. Ships with a web app, browser extension, MCP server, Raycast extension,
and SDKs for multiple frameworks.

---

## 2. Architecture

### lab-notebook — Single-module CLI

```
src/lab_notebook/
  cli.py              # Everything: schema, indexing, CLI (670 lines)
  schemas/             # 3 bundled YAML templates
tests/
  test_cli.py          # 742 lines of tests
```

- **Zero external services** — runs entirely on the local filesystem.
- **JSONL as source of truth** — Git-trackable, human-readable.
- **SQLite FTS5** — ephemeral query index, rebuilt atomically.
- **Schema-driven** — YAML schema defines fields, types, FTS columns; CLI args auto-generated.

### supermemory — Monorepo SaaS platform

```
apps/
  web/                 # Next.js consumer app (app.supermemory.ai)
  mcp/                 # MCP server (Cloudflare Workers + Durable Objects)
  browser-extension/   # Chrome extension
  docs/                # Documentation site (Fumadocs)
  raycast-extension/   # Raycast integration
  memory-graph-playground/  # Interactive graph visualizer
packages/
  ai-sdk/              # Vercel AI SDK middleware
  agent-framework-python/  # Python agent framework SDK
  openai-sdk-python/   # OpenAI SDK middleware
  pipecat-sdk-python/  # Pipecat voice AI SDK
  memory-graph/        # D3-based graph visualization
  ui/                  # Shared UI components
  tools/               # CLI tooling
  hooks/               # Shared hooks
  lib/                 # Shared library code
  validation/          # Shared Zod schemas
```

- **Cloud-native** — Cloudflare Workers, Durable Objects, PostgreSQL (Drizzle ORM).
- **Multi-AI provider** — OpenAI, Anthropic, Google, Cerebras integrations.
- **Auth & billing** — better-auth, Stripe, Resend email.
- **Monitoring** — Sentry, PostHog analytics.

---

## 3. Data Model

### lab-notebook

```
Entry = {
  id, ts, writer_id, context, type, content,
  ...schema-defined fields (repo, branch, tags, etc.)
}
```

- Flat JSONL lines, one file per writer.
- Schema customizable via YAML (3 bundled templates: research-notebook, ml-experiment-log, research-logbook).
- Extra fields stored in a JSON blob column.

### supermemory

- **Memory graph** with entities, relationships, and temporal metadata.
- Automatic fact extraction, contradiction resolution, and forgetting.
- User profiles auto-maintained from conversation history.
- Hybrid search combining RAG (vector) + memory (graph).
- Connectors for Google Drive, Gmail, Notion, OneDrive, GitHub.

---

## 4. Key Differences

| Dimension | lab-notebook | supermemory |
|---|---|---|
| **Philosophy** | Minimal, do-one-thing-well | Full platform, batteries-included |
| **Data ownership** | 100% local, Git-tracked | Cloud-hosted API |
| **Complexity** | ~670 LOC, 1 dependency (pyyaml) | ~80k LOC, massive dependency tree |
| **Query** | SQL + FTS5 full-text search | Hybrid vector + graph search |
| **AI integration** | None (logs *about* AI work) | Core product *is* AI memory |
| **Multi-user** | Per-writer JSONL (Git merge-safe) | Full auth, teams, billing |
| **Connectors** | None (manual `emit`) | Google Drive, Gmail, Notion, etc. |
| **Schema** | User-defined YAML | Fixed ontology with auto-extraction |
| **Offline** | Fully offline | Requires internet/API |
| **CI/CD** | GitHub Actions (pytest) | Turborepo + Wrangler deploys |

---

## 5. Where They Overlap

- Both deal with **structured storage of knowledge** for AI-adjacent workflows.
- Both support **search** over stored entries (FTS5 vs hybrid vector+graph).
- Both are **open source** on GitHub.
- Both value **developer experience** — lab-notebook via clean CLI, supermemory via SDKs.

---

## 6. Where They're Complementary

These tools could work well **together**:

- **lab-notebook** logs the *researcher's* decisions, observations, and experiment
  metadata in a Git-tracked, reproducible format.
- **supermemory** gives the *AI assistant* persistent memory across conversations,
  so it can recall past context when helping with research.

A researcher could use lab-notebook to track their experiments while using supermemory
to give their AI coding assistant memory of past discussions about those experiments.

---

## 7. Summary

**lab-notebook** is a focused, minimal tool for researchers who want Git-tracked
experiment logs with SQL queryability. It's the "Unix philosophy" approach — small,
sharp, composable.

**supermemory** is an ambitious full-stack platform solving AI memory at scale —
fact extraction, contradiction handling, user profiles, connectors, and multi-provider
support. It's the "platform" approach — comprehensive, cloud-native, production-grade.

They serve different audiences solving different problems, and could complement each
other in a research workflow.
