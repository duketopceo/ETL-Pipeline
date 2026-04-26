# ETL-Pipeline — Agent Instructions

## Gospel Rules

**READ FIRST**: All agents and contributors must follow [luke-agents](https://github.com/duketopceo/luke-agents) — Karpathy principles, code standards, testing, security, guardrails, integrations, and deployment patterns. That repo is the source of truth.

**Precedence**:
1. `luke-agents` (gospel)
2. This `CLAUDE.md` (repo-specific overrides)
3. Runtime agent instructions

---

## Repo Context
Automated ETL pipeline — CSV data processing, schema validation, BigQuery integration.

- Python + pandas + Jupyter
- BigQuery: use parameterized queries, never string concat
- Test transformations on sample data before production runs
