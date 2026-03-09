# :shield: dataxlr8-truthseeker-mcp

Ground truth validation for AI agents — cross-check claims, validate repos, audit data quality, and gate merges.

[![Rust](https://img.shields.io/badge/Rust-2024_edition-orange?logo=rust)](https://www.rust-lang.org/)
[![MCP](https://img.shields.io/badge/MCP-rmcp_0.17-blue)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## What It Does

Prevents hallucinated claims from reaching production. Validates MCP repos (compiles, tools respond, README exists, branch protection set), audits PostgreSQL schemas for test data and orphaned rows, maintains a ground truth fact registry with staleness tracking, and runs pre-merge gates that block PRs until all checks pass. The integrity layer for the entire DataXLR8 platform.

## Architecture

```
                    ┌───────────────────────────────┐
AI Agent ──stdio──▶ │  dataxlr8-truthseeker-mcp     │
                    │  (rmcp 0.17 server)            │
                    └─────┬───────────┬─────────────┘
                          │ sqlx 0.8  │ shell/gh/cargo
                          ▼           ▼
                    ┌──────────┐ ┌──────────────────┐
                    │ PostgreSQL│ │ Git repos        │
                    │ schema:  │ │ (compile, test,  │
                    │ truth-   │ │  validate)       │
                    │ seeker   │ └──────────────────┘
                    └──────────┘
```

## Tools

| Tool | Description |
|------|-------------|
| `validate_repo` | Validate an MCP repo: compiles, tools/list works, README, branch protection |
| `audit_all_repos` | Scan all dataxlr8-*-mcp repos with full verified inventory |
| `validate_data` | Validate a PostgreSQL schema: row counts, test data, orphans, constraints |
| `clean_test_data` | Identify and remove test/dummy data (supports dry_run) |
| `register_fact` | Store a verified fact in the ground truth registry |
| `check_fact` | Look up a fact with staleness tracking |
| `pre_merge_gate` | Run all validation checks before merging a PR |
| `validate_claim` | Cross-check a textual claim against real data |
| `validation_history` | Query past validation runs with filters |
| `validate_status` | Validate current project status against real state |

## Quick Start

```bash
git clone https://github.com/pdaxt/dataxlr8-truthseeker-mcp
cd dataxlr8-truthseeker-mcp
cargo build --release

export DATABASE_URL=postgres://user:pass@localhost:5432/dataxlr8
./target/release/dataxlr8-truthseeker-mcp
```

The server auto-creates the `truthseeker` schema and all tables on first run.

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `LOG_LEVEL` | No | Tracing level (default: `info`) |

## Claude Desktop Integration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dataxlr8-truthseeker": {
      "command": "./target/release/dataxlr8-truthseeker-mcp",
      "env": {
        "DATABASE_URL": "postgres://user:pass@localhost:5432/dataxlr8"
      }
    }
  }
}
```

## Part of DataXLR8

One of 14 Rust MCP servers that form the [DataXLR8](https://github.com/pdaxt) platform — a modular, AI-native business operations suite. Each server owns a single domain, shares a PostgreSQL instance, and communicates over the Model Context Protocol.

## License

MIT
