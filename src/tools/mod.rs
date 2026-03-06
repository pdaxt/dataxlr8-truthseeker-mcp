use dataxlr8_mcp_core::mcp::{empty_schema, error_result, get_i64, get_str, json_result, make_schema};
use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// ============================================================================
// Constants
// ============================================================================

const MCP_BASE_DIR: &str = "/Users/pran/Projects";
const MCP_BINARY_DIR: &str = "/Users/pran/Projects/.cargo-target/release";
const GITHUB_ORG: &str = "pdaxt";
const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

const VALID_VALIDATION_TYPES: &[&str] = &["claim", "data", "repo", "status", "schema", "gate"];
const VALID_RESULTS: &[&str] = &["pass", "fail", "warn"];
const VALID_CATEGORIES: &[&str] = &["repo", "mcp", "deal", "metric", "schema", "tool", "config"];

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Validation {
    pub id: uuid::Uuid,
    pub validation_type: String,
    pub target: String,
    pub result: String,
    pub details: serde_json::Value,
    pub agent_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Fact {
    pub id: uuid::Uuid,
    pub category: String,
    pub key: String,
    pub value: serde_json::Value,
    pub source: String,
    pub verified_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Gate {
    pub id: uuid::Uuid,
    pub repo: String,
    pub branch: String,
    pub gate_type: String,
    pub passed: bool,
    pub checks: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepoCheck {
    pub repo: String,
    pub binary_exists: bool,
    pub compiles: bool,
    pub compile_error: Option<String>,
    pub tool_count: Option<usize>,
    pub tools: Option<Vec<String>>,
    pub has_readme: bool,
    pub on_github: bool,
    pub branch_protection: bool,
    pub last_commit: Option<String>,
    pub schema_exists: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataCheck {
    pub schema: String,
    pub tables: Vec<TableCheck>,
    pub issues: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableCheck {
    pub table: String,
    pub row_count: i64,
    pub null_required_fields: Vec<String>,
    pub issues: Vec<String>,
}

// ============================================================================
// Input validation helpers
// ============================================================================

fn require_trimmed(args: &serde_json::Value, key: &str) -> Result<String, String> {
    match get_str(args, key) {
        Some(v) => {
            let trimmed = v.trim().to_string();
            if trimmed.is_empty() {
                Err(format!("Parameter '{key}' must not be empty"))
            } else {
                Ok(trimmed)
            }
        }
        None => Err(format!("Missing required parameter: {key}")),
    }
}

fn clamp_limit(args: &serde_json::Value) -> i64 {
    get_i64(args, "limit").unwrap_or(DEFAULT_LIMIT).max(1).min(MAX_LIMIT)
}

fn clamp_offset(args: &serde_json::Value) -> i64 {
    get_i64(args, "offset").unwrap_or(0).max(0)
}

// ============================================================================
// MCP probe — start a binary, do handshake, get tools
// ============================================================================

/// Start an MCP binary, send initialize + tools/list, return the tool names.
async fn probe_mcp(binary_path: &str, db_url: &str) -> Result<Vec<String>, String> {
    let mut child = tokio::process::Command::new("env")
        .arg(format!("DATABASE_URL={db_url}"))
        .arg("RUST_LOG=error")
        .arg(binary_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("Failed to start {binary_path}: {e}"))?;

    let mut stdin = child.stdin.take().ok_or("No stdin")?;
    let stdout = child.stdout.take().ok_or("No stdout")?;
    let mut reader = BufReader::new(stdout);

    // Send initialize
    let init_msg = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"truthseeker","version":"1.0"}}}"#;
    stdin.write_all(init_msg.as_bytes()).await.map_err(|e| format!("Write error: {e}"))?;
    stdin.write_all(b"\n").await.map_err(|e| format!("Write error: {e}"))?;
    stdin.flush().await.map_err(|e| format!("Flush error: {e}"))?;

    // Read initialize response
    let mut line = String::new();
    let read_result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.read_line(&mut line),
    )
    .await;

    match read_result {
        Ok(Ok(0)) | Err(_) => {
            let _ = child.kill().await;
            return Err("Timeout or EOF reading initialize response".into());
        }
        Ok(Err(e)) => {
            let _ = child.kill().await;
            return Err(format!("Read error: {e}"));
        }
        Ok(Ok(_)) => {}
    }

    // Parse init response to confirm it worked
    let init_resp: serde_json::Value = serde_json::from_str(line.trim())
        .map_err(|e| format!("Invalid JSON in init response: {e}"))?;
    if init_resp.get("error").is_some() {
        let _ = child.kill().await;
        return Err(format!("Init error: {}", init_resp["error"]));
    }

    // Send initialized notification
    let notif = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    stdin.write_all(notif.as_bytes()).await.map_err(|e| format!("Write error: {e}"))?;
    stdin.write_all(b"\n").await.map_err(|e| format!("Write error: {e}"))?;
    stdin.flush().await.map_err(|e| format!("Flush error: {e}"))?;

    // Small delay for notification processing
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Send tools/list
    let tools_msg = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#;
    stdin.write_all(tools_msg.as_bytes()).await.map_err(|e| format!("Write error: {e}"))?;
    stdin.write_all(b"\n").await.map_err(|e| format!("Write error: {e}"))?;
    stdin.flush().await.map_err(|e| format!("Flush error: {e}"))?;

    // Read tools/list response
    let mut tools_line = String::new();
    let read_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        reader.read_line(&mut tools_line),
    )
    .await;

    // Clean up
    drop(stdin);
    let _ = child.kill().await;

    match read_result {
        Ok(Ok(0)) | Err(_) => {
            return Err("Timeout or EOF reading tools/list response".into());
        }
        Ok(Err(e)) => {
            return Err(format!("Read error: {e}"));
        }
        Ok(Ok(_)) => {}
    }

    let tools_resp: serde_json::Value = serde_json::from_str(tools_line.trim())
        .map_err(|e| format!("Invalid JSON in tools response: {e}"))?;

    let tools = tools_resp["result"]["tools"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["name"].as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(tools)
}

/// Check if a command succeeds (for cargo check etc.)
async fn run_cmd(cmd: &str, args: &[&str]) -> (bool, String, String) {
    match tokio::process::Command::new(cmd)
        .args(args)
        .output()
        .await
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            (output.status.success(), stdout, stderr)
        }
        Err(e) => (false, String::new(), format!("Failed to run {cmd}: {e}")),
    }
}

/// Check GitHub repo info via gh CLI
async fn check_github_repo(repo: &str) -> (bool, bool, bool) {
    // exists + default branch?
    let (exists, repo_info, _) = run_cmd("gh", &["repo", "view", &format!("{GITHUB_ORG}/{repo}"), "--json", "name,defaultBranchRef", "--jq", ".defaultBranchRef.name"]).await;
    let default_branch = if repo_info.trim().is_empty() { "master".to_string() } else { repo_info.trim().to_string() };

    // README?
    let (has_readme, _, _) = run_cmd("gh", &["api", &format!("repos/{GITHUB_ORG}/{repo}/contents/README.md"), "--jq", ".size"]).await;

    // Branch protection? Check if protection exists at all (not just PR reviews)
    let (bp_exists, bp_out, _) = run_cmd("gh", &["api", &format!("repos/{GITHUB_ORG}/{repo}/branches/{default_branch}/protection"), "--jq", ".url"]).await;
    let has_protection = bp_exists && !bp_out.trim().is_empty();

    (exists, has_readme, has_protection)
}

// ============================================================================
// Tool definitions
// ============================================================================

fn build_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "validate_repo".into(),
            title: None,
            description: Some("Validate a single MCP repo: binary exists, compiles, tools/list works, GitHub status, README, branch protection".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "repo": { "type": "string", "description": "Repo name e.g. dataxlr8-crm-mcp" },
                }),
                vec!["repo"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "audit_all_repos".into(),
            title: None,
            description: Some("Scan ALL dataxlr8-*-mcp repos: binary, compile, tool count, README, GitHub, branch protection. Returns full verified inventory.".into()),
            input_schema: empty_schema(),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "validate_data".into(),
            title: None,
            description: Some("Validate data in a PostgreSQL schema: row counts, check for test/dummy data, orphaned rows, constraint violations".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "schema": { "type": "string", "description": "Schema name e.g. 'crm', 'enrichment'" },
                }),
                vec!["schema"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "clean_test_data".into(),
            title: None,
            description: Some("Identify and remove test/dummy data from a schema. Use dry_run=true to preview without deleting.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "schema": { "type": "string", "description": "Schema name" },
                    "dry_run": { "type": "boolean", "description": "If true, show what would be deleted without actually deleting. Default: true" },
                }),
                vec!["schema"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "register_fact".into(),
            title: None,
            description: Some("Store a verified fact in the ground truth registry".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "category": { "type": "string", "description": "One of: repo, mcp, deal, metric, schema, tool, config" },
                    "key": { "type": "string", "description": "Unique key e.g. 'repo:dataxlr8-crm-mcp:tool_count'" },
                    "value": { "type": "object", "description": "The fact value as JSON" },
                    "source": { "type": "string", "description": "How this fact was verified: cargo_check, gh_api, db_query, mcp_probe, manual" },
                    "expires_hours": { "type": "integer", "description": "Hours until this fact expires. Default: 24" },
                }),
                vec!["category", "key", "value", "source"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "check_fact".into(),
            title: None,
            description: Some("Look up a fact from the ground truth registry. Returns value plus staleness.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "key": { "type": "string", "description": "Fact key to look up" },
                }),
                vec!["key"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "pre_merge_gate".into(),
            title: None,
            description: Some("Run all validation checks before merging a PR. Checks: compiles, tools work, no test data, pattern compliance.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "repo": { "type": "string", "description": "Repo name" },
                    "branch": { "type": "string", "description": "Branch to validate. Default: master" },
                }),
                vec!["repo"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "validate_claim".into(),
            title: None,
            description: Some("Cross-check a textual claim against real data. E.g. '23 MCPs compile' → verifies by counting and compiling each.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "claim": { "type": "string", "description": "The claim to validate e.g. '23 MCPs compile', '212 tools total'" },
                }),
                vec!["claim"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "validation_history".into(),
            title: None,
            description: Some("Query past validation runs with filters".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "validation_type": { "type": "string", "description": "Filter by type: claim, data, repo, status, schema, gate" },
                    "result": { "type": "string", "description": "Filter by result: pass, fail, warn" },
                    "limit": { "type": "integer", "description": "Max results. Default: 50" },
                    "offset": { "type": "integer", "description": "Offset. Default: 0" },
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "validate_status".into(),
            title: None,
            description: Some("Validate the current AGENT-STATUS.md against real state: repo count, tool count, compile status, QA status.".into()),
            input_schema: empty_schema(),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
    ]
}

// ============================================================================
// Server
// ============================================================================

pub struct TruthSeekerServer {
    db: Database,
    tools: Vec<Tool>,
    db_url: String,
}

impl TruthSeekerServer {
    pub fn new(db: Database) -> Self {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8".to_string()
        });
        Self {
            db,
            tools: build_tools(),
            db_url,
        }
    }

    /// Log a validation result to the database.
    async fn log_validation(
        &self,
        validation_type: &str,
        target: &str,
        result: &str,
        details: &serde_json::Value,
        agent_id: Option<&str>,
    ) -> Option<Validation> {
        match sqlx::query_as::<_, Validation>(
            r#"INSERT INTO truthseeker.validations (validation_type, target, result, details, agent_id)
               VALUES ($1, $2, $3, $4, $5) RETURNING *"#,
        )
        .bind(validation_type)
        .bind(target)
        .bind(result)
        .bind(details)
        .bind(agent_id)
        .fetch_one(self.db.pool())
        .await
        {
            Ok(v) => Some(v),
            Err(e) => {
                error!(error = %e, "Failed to log validation");
                None
            }
        }
    }

    /// Get list of all MCP repo directories on disk.
    fn discover_mcp_repos(&self) -> Vec<String> {
        let mut repos = Vec::new();
        if let Ok(entries) = std::fs::read_dir(MCP_BASE_DIR) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("dataxlr8-") && name.ends_with("-mcp") {
                    if entry.path().join("Cargo.toml").exists() {
                        repos.push(name);
                    }
                }
            }
        }
        repos.sort();
        repos
    }

    /// Validate a single repo thoroughly.
    async fn check_repo(&self, repo: &str) -> RepoCheck {
        let binary_path = format!("{MCP_BINARY_DIR}/{repo}");
        let project_path = format!("{MCP_BASE_DIR}/{repo}");
        let binary_exists = std::path::Path::new(&binary_path).exists();

        // Compile check
        let (compiles, _, compile_err) = if std::path::Path::new(&format!("{project_path}/Cargo.toml")).exists() {
            run_cmd(
                "cargo",
                &["check", "--manifest-path", &format!("{project_path}/Cargo.toml")],
            )
            .await
        } else {
            (false, String::new(), "Cargo.toml not found".into())
        };

        // MCP probe — get tools
        let (tool_count, tools) = if binary_exists {
            match probe_mcp(&binary_path, &self.db_url).await {
                Ok(tool_names) => (Some(tool_names.len()), Some(tool_names)),
                Err(e) => {
                    warn!(repo = repo, error = %e, "MCP probe failed");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        // GitHub checks
        let (on_github, has_readme, branch_protection) = check_github_repo(repo).await;

        // Last commit
        let (_, last_commit_out, _) = run_cmd(
            "git",
            &["-C", &project_path, "log", "-1", "--format=%h %s", "--"],
        )
        .await;
        let last_commit = if last_commit_out.trim().is_empty() {
            None
        } else {
            Some(last_commit_out.trim().to_string())
        };

        // Schema check — infer schema name from repo name
        let schema_name = repo
            .strip_prefix("dataxlr8-")
            .and_then(|s| s.strip_suffix("-mcp"))
            .unwrap_or(repo);
        let schema_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
        )
        .bind(schema_name)
        .fetch_one(self.db.pool())
        .await
        .unwrap_or(false);

        RepoCheck {
            repo: repo.to_string(),
            binary_exists,
            compiles,
            compile_error: if compiles { None } else { Some(compile_err) },
            tool_count,
            tools,
            has_readme,
            on_github,
            branch_protection,
            last_commit,
            schema_exists,
        }
    }

    // ========================================================================
    // Tool handlers
    // ========================================================================

    async fn handle_validate_repo(&self, args: &serde_json::Value) -> CallToolResult {
        let repo = match require_trimmed(args, "repo") {
            Ok(r) => r,
            Err(e) => return error_result(&e),
        };

        info!(repo = %repo, "Validating repo");
        let check = self.check_repo(&repo).await;

        let all_pass = check.binary_exists
            && check.compiles
            && check.tool_count.unwrap_or(0) > 0
            && check.on_github
            && check.has_readme
            && check.schema_exists;

        let result_str = if all_pass { "pass" } else { "fail" };
        let details = serde_json::to_value(&check).unwrap_or_default();
        self.log_validation("repo", &repo, result_str, &details, None).await;

        json_result(&serde_json::json!({
            "result": result_str,
            "check": check,
        }))
    }

    async fn handle_audit_all_repos(&self) -> CallToolResult {
        let repos = self.discover_mcp_repos();
        info!(count = repos.len(), "Auditing all MCP repos");

        let mut results = Vec::new();
        let mut total_tools = 0usize;
        let mut pass_count = 0usize;
        let mut fail_count = 0usize;

        for repo in &repos {
            let check = self.check_repo(repo).await;
            total_tools += check.tool_count.unwrap_or(0);
            if check.binary_exists && check.compiles && check.tool_count.unwrap_or(0) > 0 {
                pass_count += 1;
            } else {
                fail_count += 1;
            }
            results.push(check);
        }

        // Register facts
        let summary = serde_json::json!({
            "total_repos": repos.len(),
            "pass": pass_count,
            "fail": fail_count,
            "total_tools": total_tools,
            "verified_at": chrono::Utc::now().to_rfc3339(),
        });

        let _ = sqlx::query(
            r#"INSERT INTO truthseeker.facts (category, key, value, source, verified_at, expires_at)
               VALUES ('metric', 'audit:all_repos', $1, 'audit_all_repos', now(), now() + interval '24 hours')
               ON CONFLICT (key) DO UPDATE SET value = $1, source = 'audit_all_repos', verified_at = now(), expires_at = now() + interval '24 hours'"#,
        )
        .bind(&summary)
        .execute(self.db.pool())
        .await;

        self.log_validation(
            "repo",
            "all",
            if fail_count == 0 { "pass" } else { "warn" },
            &summary,
            None,
        )
        .await;

        json_result(&serde_json::json!({
            "summary": summary,
            "repos": results,
        }))
    }

    async fn handle_validate_data(&self, args: &serde_json::Value) -> CallToolResult {
        let schema = match require_trimmed(args, "schema") {
            Ok(s) => s,
            Err(e) => return error_result(&e),
        };

        // Verify schema exists
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
        )
        .bind(&schema)
        .fetch_one(self.db.pool())
        .await
        .unwrap_or(false);

        if !exists {
            return error_result(&format!("Schema '{schema}' does not exist"));
        }

        // Get all tables in schema
        let tables: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name",
        )
        .bind(&schema)
        .fetch_all(self.db.pool())
        .await
        .unwrap_or_default();

        let mut table_checks = Vec::new();
        let mut issues = Vec::new();

        for (table_name,) in &tables {
            let count_query = format!("SELECT COUNT(*) FROM {schema}.{table_name}");
            let row_count: i64 = sqlx::query_scalar(&count_query)
                .fetch_one(self.db.pool())
                .await
                .unwrap_or(0);

            let mut table_issues = Vec::new();

            // Check for test-looking data patterns
            if schema == "crm" && table_name == "contacts" {
                let test_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM crm.contacts WHERE email LIKE '%@test%' OR email LIKE '%@example%' OR first_name = 'Test' OR last_name = 'Test'"
                )
                .fetch_one(self.db.pool())
                .await
                .unwrap_or(0);

                if test_count > 0 {
                    table_issues.push(format!("{test_count} potential test rows detected"));
                    issues.push(format!("{schema}.{table_name}: {test_count} test rows"));
                }
            }

            if schema == "crm" && table_name == "deals" {
                let test_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM crm.deals WHERE title LIKE '%Test%' OR title LIKE '%DataXLR8%' OR notes LIKE '%test%'"
                )
                .fetch_one(self.db.pool())
                .await
                .unwrap_or(0);

                if test_count > 0 {
                    table_issues.push(format!("{test_count} potential test deals detected"));
                    issues.push(format!("{schema}.{table_name}: {test_count} test deals"));
                }
            }

            table_checks.push(TableCheck {
                table: table_name.clone(),
                row_count,
                null_required_fields: Vec::new(),
                issues: table_issues,
            });
        }

        let result_str = if issues.is_empty() { "pass" } else { "warn" };
        let data_check = DataCheck {
            schema: schema.clone(),
            tables: table_checks,
            issues: issues.clone(),
        };

        let details = serde_json::to_value(&data_check).unwrap_or_default();
        self.log_validation("data", &schema, result_str, &details, None).await;

        json_result(&serde_json::json!({
            "result": result_str,
            "check": data_check,
        }))
    }

    async fn handle_clean_test_data(&self, args: &serde_json::Value) -> CallToolResult {
        let schema = match require_trimmed(args, "schema") {
            Ok(s) => s,
            Err(e) => return error_result(&e),
        };
        let dry_run = get_str(args, "dry_run")
            .map(|s| s != "false")
            .unwrap_or(true);

        let mut cleaned = Vec::new();

        if schema == "crm" {
            // Preview test contacts
            let test_contacts: Vec<(String, String)> = sqlx::query_as(
                "SELECT COALESCE(email, 'no-email'), COALESCE(first_name || ' ' || last_name, 'unknown') FROM crm.contacts WHERE email LIKE '%@test%' OR email LIKE '%@example%' OR email LIKE '%@hireright%' OR email LIKE '%@recruitpro%' OR first_name = 'Test'"
            )
            .fetch_all(self.db.pool())
            .await
            .unwrap_or_default();

            if !test_contacts.is_empty() {
                cleaned.push(serde_json::json!({
                    "table": "crm.contacts",
                    "count": test_contacts.len(),
                    "rows": test_contacts.iter().map(|(e, n)| format!("{n} <{e}>")).collect::<Vec<_>>(),
                    "deleted": !dry_run,
                }));

                if !dry_run {
                    let _ = sqlx::query(
                        "DELETE FROM crm.contacts WHERE email LIKE '%@test%' OR email LIKE '%@example%' OR email LIKE '%@hireright%' OR email LIKE '%@recruitpro%' OR first_name = 'Test'"
                    )
                    .execute(self.db.pool())
                    .await;
                }
            }

            // Preview test deals
            let test_deals: Vec<(String, String)> = sqlx::query_as(
                "SELECT title, stage FROM crm.deals WHERE title LIKE '%DataXLR8%' OR title LIKE '%Test%'"
            )
            .fetch_all(self.db.pool())
            .await
            .unwrap_or_default();

            if !test_deals.is_empty() {
                cleaned.push(serde_json::json!({
                    "table": "crm.deals",
                    "count": test_deals.len(),
                    "rows": test_deals.iter().map(|(t, s)| format!("{t} ({s})")).collect::<Vec<_>>(),
                    "deleted": !dry_run,
                }));

                if !dry_run {
                    let _ = sqlx::query(
                        "DELETE FROM crm.deals WHERE title LIKE '%DataXLR8%' OR title LIKE '%Test%'"
                    )
                    .execute(self.db.pool())
                    .await;
                }
            }
        }

        // For enrichment schema
        if schema == "enrichment" {
            let test_lookups: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM enrichment.lookups WHERE query::text LIKE '%test%' OR query::text LIKE '%example%'"
            )
            .fetch_one(self.db.pool())
            .await
            .unwrap_or(0);

            if test_lookups > 0 {
                cleaned.push(serde_json::json!({
                    "table": "enrichment.lookups",
                    "count": test_lookups,
                    "deleted": !dry_run,
                }));

                if !dry_run {
                    let _ = sqlx::query(
                        "DELETE FROM enrichment.lookups WHERE query::text LIKE '%test%' OR query::text LIKE '%example%'"
                    )
                    .execute(self.db.pool())
                    .await;
                }
            }
        }

        json_result(&serde_json::json!({
            "schema": schema,
            "dry_run": dry_run,
            "cleaned": cleaned,
            "note": if dry_run { "Set dry_run=false to actually delete" } else { "Data deleted" },
        }))
    }

    async fn handle_register_fact(&self, args: &serde_json::Value) -> CallToolResult {
        let category = match require_trimmed(args, "category") {
            Ok(c) => c,
            Err(e) => return error_result(&e),
        };
        if !VALID_CATEGORIES.contains(&category.as_str()) {
            return error_result(&format!(
                "Invalid category '{category}'. Must be one of: {}",
                VALID_CATEGORIES.join(", ")
            ));
        }

        let key = match require_trimmed(args, "key") {
            Ok(k) => k,
            Err(e) => return error_result(&e),
        };
        let value = match args.get("value") {
            Some(v) => v.clone(),
            None => return error_result("Missing required parameter: value"),
        };
        let source = match require_trimmed(args, "source") {
            Ok(s) => s,
            Err(e) => return error_result(&e),
        };
        let expires_hours = get_i64(args, "expires_hours").unwrap_or(24);

        match sqlx::query_as::<_, Fact>(
            r#"INSERT INTO truthseeker.facts (category, key, value, source, verified_at, expires_at)
               VALUES ($1, $2, $3, $4, now(), now() + make_interval(hours => $5))
               ON CONFLICT (key) DO UPDATE SET
                   category = $1, value = $3, source = $4, verified_at = now(),
                   expires_at = now() + make_interval(hours => $5)
               RETURNING *"#,
        )
        .bind(&category)
        .bind(&key)
        .bind(&value)
        .bind(&source)
        .bind(expires_hours as i32)
        .fetch_one(self.db.pool())
        .await
        {
            Ok(fact) => json_result(&fact),
            Err(e) => {
                error!(error = %e, "Failed to register fact");
                error_result(&format!("Failed to register fact: {e}"))
            }
        }
    }

    async fn handle_check_fact(&self, args: &serde_json::Value) -> CallToolResult {
        let key = match require_trimmed(args, "key") {
            Ok(k) => k,
            Err(e) => return error_result(&e),
        };

        match sqlx::query_as::<_, Fact>(
            "SELECT * FROM truthseeker.facts WHERE key = $1",
        )
        .bind(&key)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(Some(fact)) => {
                let is_stale = fact
                    .expires_at
                    .map(|e| e < chrono::Utc::now())
                    .unwrap_or(false);
                json_result(&serde_json::json!({
                    "found": true,
                    "stale": is_stale,
                    "fact": fact,
                }))
            }
            Ok(None) => json_result(&serde_json::json!({
                "found": false,
                "stale": true,
                "message": format!("No fact registered for key '{key}'"),
            })),
            Err(e) => {
                error!(error = %e, "Failed to check fact");
                error_result(&format!("Database error: {e}"))
            }
        }
    }

    async fn handle_pre_merge_gate(&self, args: &serde_json::Value) -> CallToolResult {
        let repo = match require_trimmed(args, "repo") {
            Ok(r) => r,
            Err(e) => return error_result(&e),
        };
        let branch = get_str(args, "branch").unwrap_or_else(|| "master".into());

        info!(repo = %repo, branch = %branch, "Running pre-merge gate");

        let mut checks = Vec::new();
        let mut all_pass = true;

        // 1. Repo validation
        let repo_check = self.check_repo(&repo).await;

        let compile_pass = repo_check.compiles;
        if !compile_pass {
            all_pass = false;
        }
        checks.push(serde_json::json!({
            "check": "compiles",
            "pass": compile_pass,
            "detail": repo_check.compile_error,
        }));

        let tools_pass = repo_check.tool_count.unwrap_or(0) > 0;
        if !tools_pass {
            all_pass = false;
        }
        checks.push(serde_json::json!({
            "check": "tools_respond",
            "pass": tools_pass,
            "tool_count": repo_check.tool_count,
        }));

        let readme_pass = repo_check.has_readme;
        if !readme_pass {
            all_pass = false;
        }
        checks.push(serde_json::json!({
            "check": "has_readme",
            "pass": readme_pass,
        }));

        let github_pass = repo_check.on_github;
        if !github_pass {
            all_pass = false;
        }
        checks.push(serde_json::json!({
            "check": "on_github",
            "pass": github_pass,
        }));

        // 2. Schema data validation
        let schema_name = repo
            .strip_prefix("dataxlr8-")
            .and_then(|s| s.strip_suffix("-mcp"))
            .unwrap_or(&repo);
        if repo_check.schema_exists {
            checks.push(serde_json::json!({
                "check": "schema_exists",
                "pass": true,
                "schema": schema_name,
            }));
        } else {
            checks.push(serde_json::json!({
                "check": "schema_exists",
                "pass": false,
                "schema": schema_name,
                "detail": "Schema not found — MCP may not have been started yet",
            }));
        }

        // Store gate result
        let _ = sqlx::query_as::<_, Gate>(
            r#"INSERT INTO truthseeker.gates (repo, branch, gate_type, passed, checks)
               VALUES ($1, $2, 'pre_merge', $3, $4) RETURNING *"#,
        )
        .bind(&repo)
        .bind(&branch)
        .bind(all_pass)
        .bind(serde_json::json!(checks))
        .fetch_one(self.db.pool())
        .await;

        self.log_validation(
            "gate",
            &format!("{repo}:{branch}"),
            if all_pass { "pass" } else { "fail" },
            &serde_json::json!({"checks": checks}),
            None,
        )
        .await;

        json_result(&serde_json::json!({
            "repo": repo,
            "branch": branch,
            "gate_passed": all_pass,
            "checks": checks,
        }))
    }

    async fn handle_validate_claim(&self, args: &serde_json::Value) -> CallToolResult {
        let claim = match require_trimmed(args, "claim") {
            Ok(c) => c,
            Err(e) => return error_result(&e),
        };

        info!(claim = %claim, "Validating claim");
        let claim_lower = claim.to_lowercase();
        let mut findings = Vec::new();
        let mut overall_pass = true;

        // Extract numbers from claim
        let number_re = regex::Regex::new(r"(\d+)").unwrap();

        // Check for MCP count claims
        if claim_lower.contains("mcp") && (claim_lower.contains("compile") || claim_lower.contains("build")) {
            let repos = self.discover_mcp_repos();
            let claimed_count: Option<usize> = number_re
                .find(&claim)
                .and_then(|m| m.as_str().parse().ok());

            let actual_count = repos.len();
            let count_matches = claimed_count.map(|c| c == actual_count).unwrap_or(true);

            if !count_matches {
                overall_pass = false;
            }

            findings.push(serde_json::json!({
                "aspect": "mcp_count",
                "claimed": claimed_count,
                "actual": actual_count,
                "pass": count_matches,
            }));

            // Spot-check compilation on a sample (first 3 + last 2)
            let sample: Vec<&String> = repos.iter().take(3).chain(repos.iter().rev().take(2)).collect();
            let mut compile_failures = Vec::new();
            for repo in &sample {
                let path = format!("{MCP_BASE_DIR}/{repo}/Cargo.toml");
                let (ok, _, err) = run_cmd("cargo", &["check", "--manifest-path", &path]).await;
                if !ok {
                    compile_failures.push(format!("{repo}: {}", err.lines().last().unwrap_or("unknown error")));
                }
            }

            if !compile_failures.is_empty() {
                overall_pass = false;
            }
            findings.push(serde_json::json!({
                "aspect": "compile_sample",
                "sampled": sample.len(),
                "failures": compile_failures,
                "pass": compile_failures.is_empty(),
            }));
        }

        // Check for tool count claims
        if claim_lower.contains("tool") {
            let claimed_tools: Option<usize> = number_re
                .find(&claim)
                .and_then(|m| m.as_str().parse().ok());

            if let Some(claimed) = claimed_tools {
                // Quick count via probing a few MCPs
                let repos = self.discover_mcp_repos();
                let mut total = 0usize;
                for repo in &repos {
                    let binary = format!("{MCP_BINARY_DIR}/{repo}");
                    if let Ok(tools) = probe_mcp(&binary, &self.db_url).await {
                        total += tools.len();
                    }
                }

                let pass = claimed == total;
                if !pass {
                    overall_pass = false;
                }
                findings.push(serde_json::json!({
                    "aspect": "tool_count",
                    "claimed": claimed,
                    "actual": total,
                    "pass": pass,
                }));
            }
        }

        if findings.is_empty() {
            findings.push(serde_json::json!({
                "aspect": "unrecognized_claim",
                "message": "Could not parse specific verifiable facts from claim. Manual verification needed.",
                "pass": false,
            }));
            overall_pass = false;
        }

        let result_str = if overall_pass { "pass" } else { "fail" };
        let details = serde_json::json!({
            "claim": claim,
            "findings": findings,
        });
        self.log_validation("claim", &claim, result_str, &details, None).await;

        json_result(&serde_json::json!({
            "claim": claim,
            "result": result_str,
            "findings": findings,
        }))
    }

    async fn handle_validation_history(&self, args: &serde_json::Value) -> CallToolResult {
        let validation_type = get_str(args, "validation_type");
        let result_filter = get_str(args, "result");
        let limit = clamp_limit(args);
        let offset = clamp_offset(args);

        if let Some(ref vt) = validation_type {
            if !VALID_VALIDATION_TYPES.contains(&vt.as_str()) {
                return error_result(&format!(
                    "Invalid validation_type '{vt}'. Must be one of: {}",
                    VALID_VALIDATION_TYPES.join(", ")
                ));
            }
        }
        if let Some(ref r) = result_filter {
            if !VALID_RESULTS.contains(&r.as_str()) {
                return error_result(&format!(
                    "Invalid result '{r}'. Must be one of: {}",
                    VALID_RESULTS.join(", ")
                ));
            }
        }

        let rows: Vec<Validation> = match (validation_type, result_filter) {
            (Some(vt), Some(r)) => {
                sqlx::query_as(
                    "SELECT * FROM truthseeker.validations WHERE validation_type = $1 AND result = $2 ORDER BY created_at DESC LIMIT $3 OFFSET $4",
                )
                .bind(&vt)
                .bind(&r)
                .bind(limit)
                .bind(offset)
                .fetch_all(self.db.pool())
                .await
                .unwrap_or_default()
            }
            (Some(vt), None) => {
                sqlx::query_as(
                    "SELECT * FROM truthseeker.validations WHERE validation_type = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                )
                .bind(&vt)
                .bind(limit)
                .bind(offset)
                .fetch_all(self.db.pool())
                .await
                .unwrap_or_default()
            }
            (None, Some(r)) => {
                sqlx::query_as(
                    "SELECT * FROM truthseeker.validations WHERE result = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                )
                .bind(&r)
                .bind(limit)
                .bind(offset)
                .fetch_all(self.db.pool())
                .await
                .unwrap_or_default()
            }
            (None, None) => {
                sqlx::query_as(
                    "SELECT * FROM truthseeker.validations ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(self.db.pool())
                .await
                .unwrap_or_default()
            }
        };

        json_result(&serde_json::json!({
            "count": rows.len(),
            "limit": limit,
            "offset": offset,
            "validations": rows,
        }))
    }

    async fn handle_validate_status(&self) -> CallToolResult {
        info!("Validating project status against ground truth");

        let repos = self.discover_mcp_repos();
        let mut findings = Vec::new();
        let mut issues = Vec::new();

        // 1. Repo count
        findings.push(serde_json::json!({
            "check": "repo_count",
            "value": repos.len(),
            "repos": repos,
        }));

        // 2. Total tool count via MCP probing
        let mut total_tools = 0usize;
        let mut repo_tools = Vec::new();
        for repo in &repos {
            let binary = format!("{MCP_BINARY_DIR}/{repo}");
            match probe_mcp(&binary, &self.db_url).await {
                Ok(tools) => {
                    repo_tools.push(serde_json::json!({
                        "repo": repo,
                        "tool_count": tools.len(),
                        "tools": tools,
                    }));
                    total_tools += tools.len();
                }
                Err(e) => {
                    issues.push(format!("{repo}: probe failed — {e}"));
                    repo_tools.push(serde_json::json!({
                        "repo": repo,
                        "tool_count": null,
                        "error": e,
                    }));
                }
            }
        }

        findings.push(serde_json::json!({
            "check": "total_tools",
            "value": total_tools,
            "per_repo": repo_tools,
        }));

        // 3. Schema count
        let schemas: Vec<(String,)> = sqlx::query_as(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public') ORDER BY schema_name",
        )
        .fetch_all(self.db.pool())
        .await
        .unwrap_or_default();

        findings.push(serde_json::json!({
            "check": "db_schemas",
            "count": schemas.len(),
            "schemas": schemas.iter().map(|(s,)| s.as_str()).collect::<Vec<_>>(),
        }));

        // 4. Data quality — check CRM for test data
        let test_contacts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM crm.contacts WHERE email LIKE '%@test%' OR email LIKE '%@example%' OR email LIKE '%@hireright%' OR email LIKE '%@recruitpro%'"
        )
        .fetch_one(self.db.pool())
        .await
        .unwrap_or(0);

        let test_deals: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM crm.deals WHERE title LIKE '%DataXLR8%' OR title LIKE '%Test%'"
        )
        .fetch_one(self.db.pool())
        .await
        .unwrap_or(0);

        if test_contacts > 0 || test_deals > 0 {
            issues.push(format!("Test data found: {test_contacts} contacts, {test_deals} deals"));
        }

        findings.push(serde_json::json!({
            "check": "data_quality",
            "test_contacts": test_contacts,
            "test_deals": test_deals,
            "clean": test_contacts == 0 && test_deals == 0,
        }));

        let result_str = if issues.is_empty() { "pass" } else { "warn" };
        let details = serde_json::json!({ "findings": findings, "issues": issues });
        self.log_validation("status", "project", result_str, &details, None).await;

        json_result(&serde_json::json!({
            "result": result_str,
            "issues": issues,
            "findings": findings,
        }))
    }
}

// ============================================================================
// ServerHandler implementation
// ============================================================================

impl ServerHandler for TruthSeekerServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "DataXLR8 TruthSeeker MCP — validates claims, data, repos, and status against ground truth. \
                 Call validate_repo to check a single MCP, audit_all_repos for full inventory, \
                 validate_data to check DB integrity, pre_merge_gate before any PR merge."
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        async {
            Ok(ListToolsResult {
                tools: self.tools.clone(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let args = serde_json::to_value(&request.arguments).unwrap_or(serde_json::Value::Null);

            let name_str: &str = request.name.as_ref();
            let result = match name_str {
                "validate_repo" => self.handle_validate_repo(&args).await,
                "audit_all_repos" => self.handle_audit_all_repos().await,
                "validate_data" => self.handle_validate_data(&args).await,
                "clean_test_data" => self.handle_clean_test_data(&args).await,
                "register_fact" => self.handle_register_fact(&args).await,
                "check_fact" => self.handle_check_fact(&args).await,
                "pre_merge_gate" => self.handle_pre_merge_gate(&args).await,
                "validate_claim" => self.handle_validate_claim(&args).await,
                "validation_history" => self.handle_validation_history(&args).await,
                "validate_status" => self.handle_validate_status().await,
                _ => error_result(&format!("Unknown tool: {}", name_str)),
            };

            Ok(result)
        }
    }
}
