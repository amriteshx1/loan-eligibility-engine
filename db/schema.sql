CREATE TABLE IF NOT EXISTS ingestion_batches (
    id UUID PRIMARY KEY,
    object_key TEXT NOT NULL UNIQUE,
    original_filename TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    error_message TEXT,
    workflow_triggered_at TIMESTAMPTZ,
    workflow_trigger_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    monthly_income NUMERIC(14, 2) NOT NULL CHECK (monthly_income >= 0),
    credit_score INTEGER NOT NULL CHECK (credit_score >= 0 AND credit_score <= 1000),
    employment_status TEXT NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 0 AND age <= 130),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (batch_id, user_id)
);

-- Existing databases created before name was required. New rows must carry a
-- non-empty name. NOT VALID keeps a historical row that has no name from
-- blocking this migration; ingestion rejects a blank name before insert.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS name TEXT;

ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_name_required;

ALTER TABLE users
    ADD CONSTRAINT users_name_required
    CHECK (name IS NOT NULL AND length(btrim(name)) > 0) NOT VALID;

CREATE TABLE IF NOT EXISTS loan_products (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    product_name TEXT NOT NULL,
    interest_rate_min NUMERIC(7, 3),
    interest_rate_max NUMERIC(7, 3),
    minimum_monthly_income NUMERIC(14, 2),
    income_currency CHAR(3) NOT NULL DEFAULT 'USD'
        CONSTRAINT loan_products_income_currency_valid
        CHECK (income_currency ~ '^[A-Z]{3}$'),
    credit_score_min INTEGER,
    credit_score_max INTEGER,
    age_min INTEGER,
    age_max INTEGER,
    accepted_employment_statuses TEXT[],
    qualitative_criteria JSONB NOT NULL DEFAULT '[]'::JSONB
        CONSTRAINT loan_products_qualitative_criteria_array
        CHECK (jsonb_typeof(qualitative_criteria) = 'array'),
    source_evidence JSONB NOT NULL DEFAULT '[]'::JSONB
        CONSTRAINT loan_products_source_evidence_array
        CHECK (jsonb_typeof(source_evidence) = 'array'),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, source_url, product_name)
);

-- Safe for an existing Phase 1/2 database. The CREATE TABLE definition above
-- is used on fresh installs; these statements bring an already initialized
-- local database to the Workflow A shape without dropping product data.
ALTER TABLE loan_products
    ADD COLUMN IF NOT EXISTS income_currency CHAR(3) NOT NULL DEFAULT 'USD';

ALTER TABLE loan_products
    DROP CONSTRAINT IF EXISTS loan_products_income_currency_valid;

ALTER TABLE loan_products
    ADD CONSTRAINT loan_products_income_currency_valid
        CHECK (income_currency ~ '^[A-Z]{3}$');

ALTER TABLE loan_products
    ADD COLUMN IF NOT EXISTS source_evidence JSONB NOT NULL DEFAULT '[]'::JSONB;

ALTER TABLE loan_products
    ALTER COLUMN qualitative_criteria SET DEFAULT '[]'::JSONB;

UPDATE loan_products
SET qualitative_criteria = CASE
    WHEN qualitative_criteria = '{}'::JSONB THEN '[]'::JSONB
    ELSE jsonb_build_array(qualitative_criteria)
END
WHERE jsonb_typeof(qualitative_criteria) <> 'array';

ALTER TABLE loan_products
    DROP CONSTRAINT IF EXISTS loan_products_qualitative_criteria_array;

ALTER TABLE loan_products
    ADD CONSTRAINT loan_products_qualitative_criteria_array
        CHECK (jsonb_typeof(qualitative_criteria) = 'array');

ALTER TABLE loan_products
    DROP CONSTRAINT IF EXISTS loan_products_source_evidence_array;

ALTER TABLE loan_products
    ADD CONSTRAINT loan_products_source_evidence_array
        CHECK (jsonb_typeof(source_evidence) = 'array');

CREATE TABLE IF NOT EXISTS matches (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    user_record_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES loan_products(id) ON DELETE CASCADE,
    match_method TEXT NOT NULL,
    match_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notified_at TIMESTAMPTZ,
    UNIQUE (user_record_id, product_id)
);

ALTER TABLE matches
    ADD COLUMN IF NOT EXISTS evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- One row per Workflow B execution. Counts are the optimization record:
-- how many theoretical pairs SQL, deterministic rules, and the LLM each saw.
CREATE TABLE IF NOT EXISTS matching_runs (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed')),
    users_in_batch INTEGER NOT NULL CHECK (users_in_batch >= 0),
    active_products INTEGER NOT NULL CHECK (active_products >= 0),
    theoretical_pairs INTEGER NOT NULL CHECK (theoretical_pairs >= 0),
    sql_candidates INTEGER NOT NULL CHECK (sql_candidates >= 0),
    deterministic_remaining INTEGER NOT NULL CHECK (deterministic_remaining >= 0),
    qualitative_pairs INTEGER NOT NULL CHECK (qualitative_pairs >= 0),
    qualitative_llm_pairs INTEGER NOT NULL CHECK (qualitative_llm_pairs >= 0),
    llm_calls INTEGER NOT NULL CHECK (llm_calls >= 0),
    llm_errors INTEGER NOT NULL CHECK (llm_errors >= 0),
    matches_written INTEGER NOT NULL CHECK (matches_written >= 0),
    stale_matches_removed INTEGER NOT NULL DEFAULT 0 CHECK (stale_matches_removed >= 0),
    pages_processed INTEGER NOT NULL CHECK (pages_processed >= 0),
    products_skipped_currency INTEGER NOT NULL DEFAULT 0 CHECK (products_skipped_currency >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_batch_id ON users (batch_id);
CREATE INDEX IF NOT EXISTS idx_users_batch_keyset ON users (batch_id, id);
CREATE INDEX IF NOT EXISTS idx_users_numeric_eligibility
    ON users (monthly_income, credit_score, age);
CREATE INDEX IF NOT EXISTS idx_loan_products_active ON loan_products (active)
    WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_matches_batch_id ON matches (batch_id);
CREATE INDEX IF NOT EXISTS idx_matches_unnotified ON matches (batch_id, user_record_id)
    WHERE notified_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_matching_runs_batch_id
    ON matching_runs (batch_id, created_at DESC);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO loan_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO loan_app;

