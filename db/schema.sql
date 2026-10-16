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
    email TEXT NOT NULL,
    monthly_income NUMERIC(14, 2) NOT NULL CHECK (monthly_income >= 0),
    credit_score INTEGER NOT NULL CHECK (credit_score >= 0 AND credit_score <= 1000),
    employment_status TEXT NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 0 AND age <= 130),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (batch_id, user_id)
);

CREATE TABLE IF NOT EXISTS loan_products (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    product_name TEXT NOT NULL,
    interest_rate_min NUMERIC(7, 3),
    interest_rate_max NUMERIC(7, 3),
    minimum_monthly_income NUMERIC(14, 2),
    credit_score_min INTEGER,
    credit_score_max INTEGER,
    age_min INTEGER,
    age_max INTEGER,
    accepted_employment_statuses TEXT[],
    qualitative_criteria JSONB NOT NULL DEFAULT '{}'::JSONB,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, source_url, product_name)
);

CREATE TABLE IF NOT EXISTS matches (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    user_record_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES loan_products(id) ON DELETE CASCADE,
    match_method TEXT NOT NULL,
    match_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notified_at TIMESTAMPTZ,
    UNIQUE (user_record_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_users_batch_id ON users (batch_id);
CREATE INDEX IF NOT EXISTS idx_users_numeric_eligibility
    ON users (monthly_income, credit_score, age);
CREATE INDEX IF NOT EXISTS idx_loan_products_active ON loan_products (active)
    WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_matches_batch_id ON matches (batch_id);
CREATE INDEX IF NOT EXISTS idx_matches_unnotified ON matches (batch_id, user_record_id)
    WHERE notified_at IS NULL;

