use anyhow::Result;
use sqlx::PgPool;

/// Create the truthseeker schema in PostgreSQL if it doesn't exist.
pub async fn setup_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA IF NOT EXISTS truthseeker;

        CREATE TABLE IF NOT EXISTS truthseeker.validations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            validation_type TEXT NOT NULL
                CHECK (validation_type IN ('claim', 'data', 'repo', 'status', 'schema', 'gate')),
            target TEXT NOT NULL,
            result TEXT NOT NULL
                CHECK (result IN ('pass', 'fail', 'warn')),
            details JSONB NOT NULL DEFAULT '{}',
            agent_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS truthseeker.facts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            category TEXT NOT NULL
                CHECK (category IN ('repo', 'mcp', 'deal', 'metric', 'schema', 'tool', 'config')),
            key TEXT NOT NULL UNIQUE,
            value JSONB NOT NULL,
            source TEXT NOT NULL,
            verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS truthseeker.gates (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            repo TEXT NOT NULL,
            branch TEXT NOT NULL,
            gate_type TEXT NOT NULL
                CHECK (gate_type IN ('pre_merge', 'pre_push', 'post_deploy')),
            passed BOOLEAN NOT NULL,
            checks JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_validations_type ON truthseeker.validations(validation_type);
        CREATE INDEX IF NOT EXISTS idx_validations_result ON truthseeker.validations(result);
        CREATE INDEX IF NOT EXISTS idx_validations_created ON truthseeker.validations(created_at);
        CREATE INDEX IF NOT EXISTS idx_facts_category ON truthseeker.facts(category);
        CREATE INDEX IF NOT EXISTS idx_facts_key ON truthseeker.facts(key);
        CREATE INDEX IF NOT EXISTS idx_gates_repo ON truthseeker.gates(repo);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
