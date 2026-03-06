# dataxlr8-truthseeker-mcp

Ground truth validation MCP for DataXLR8. Cross-checks claims, data, repos, and status against real state before anything gets merged or reported.

## Tools

| Tool | Description |
|------|-------------|
| validate_repo | Validate a single MCP repo: binary exists, compiles, tools/list works, GitHub status, README, branch protection |
| audit_all_repos | Scan ALL dataxlr8-*-mcp repos: binary, compile, tool count, README, GitHub, branch protection. Returns full verified inventory |
| validate_data | Validate data in a PostgreSQL schema: row counts, check for test/dummy data, orphaned rows, constraint violations |
| clean_test_data | Identify and remove test/dummy data from a schema. Use dry_run=true to preview without deleting |
| register_fact | Store a verified fact in the ground truth registry |
| check_fact | Look up a fact from the ground truth registry. Returns value plus staleness |
| pre_merge_gate | Run all validation checks before merging a PR. Checks: compiles, tools work, no test data, pattern compliance |
| validate_claim | Cross-check a textual claim against real data. E.g. '23 MCPs compile' verifies by counting and compiling each |
| validation_history | Query past validation runs with filters |
| validate_status | Validate the current project status against real state: repo count, tool count, compile status, data quality |

## Setup

```bash
DATABASE_URL=postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8 cargo run
```

## Schema

Creates `truthseeker.*` schema in PostgreSQL:

- `truthseeker.validations` — validation run results (type, target, pass/fail/warn, details)
- `truthseeker.facts` — ground truth registry (category, key, value, source, expiry)
- `truthseeker.gates` — merge gate results (repo, branch, checks, passed)

## Part of

[DataXLR8](https://github.com/pdaxt) - AI-powered recruitment platform
