-- Harvest Med Waste — Lead Generator Database Schema
-- Migration 001: Initial schema

-- Core leads table (source of truth)
CREATE TABLE IF NOT EXISTS leads (
    id              SERIAL PRIMARY KEY,
    lead_uid        VARCHAR(64) UNIQUE NOT NULL,
    facility_name   VARCHAR(255) NOT NULL,
    facility_type   VARCHAR(50) NOT NULL,
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(2) DEFAULT 'AL',
    zip5            VARCHAR(5),
    county          VARCHAR(100),
    phone           VARCHAR(20),
    fax             VARCHAR(20),
    administrator   VARCHAR(255),
    npi_number      VARCHAR(10),
    license_number  VARCHAR(50),
    taxonomy_code   VARCHAR(20),
    entity_type     VARCHAR(10),
    -- Enrichment fields
    bed_count                   INTEGER,
    estimated_waste_lbs_per_day NUMERIC(8,2),
    estimated_monthly_volume    NUMERIC(10,2),
    waste_tier                  VARCHAR(20),
    distance_from_birmingham    NUMERIC(6,1),
    service_zone                VARCHAR(20),
    completeness_score          NUMERIC(3,2),
    -- Scoring
    lead_score      INTEGER DEFAULT 0,
    priority_tier   VARCHAR(10),
    -- Status tracking
    status          VARCHAR(30) DEFAULT 'New',
    notes           TEXT,
    crm_id          VARCHAR(100),
    crm_synced_at   TIMESTAMP,
    -- Timestamps
    first_seen      TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW(),
    date_added      DATE DEFAULT CURRENT_DATE
);

-- Source attribution
CREATE TABLE IF NOT EXISTS lead_sources (
    id          SERIAL PRIMARY KEY,
    lead_id     INTEGER REFERENCES leads(id) ON DELETE CASCADE,
    source      VARCHAR(20) NOT NULL,
    source_id   VARCHAR(100) NOT NULL,
    raw_data    JSONB,
    match_confidence NUMERIC(3,2),
    ingested_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(lead_id, source)
);

-- Score history
CREATE TABLE IF NOT EXISTS lead_score_history (
    id          SERIAL PRIMARY KEY,
    lead_id     INTEGER REFERENCES leads(id) ON DELETE CASCADE,
    score       INTEGER,
    priority_tier VARCHAR(10),
    scored_at   TIMESTAMP DEFAULT NOW(),
    score_breakdown JSONB
);

-- CRM sync log
CREATE TABLE IF NOT EXISTS crm_sync_log (
    id          SERIAL PRIMARY KEY,
    lead_id     INTEGER REFERENCES leads(id) ON DELETE CASCADE,
    action      VARCHAR(20),
    crm_adapter VARCHAR(30),
    crm_lead_id VARCHAR(100),
    payload     JSONB,
    synced_at   TIMESTAMP DEFAULT NOW(),
    success     BOOLEAN DEFAULT TRUE,
    error_msg   TEXT
);

-- Pipeline run history
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMP DEFAULT NOW(),
    completed_at    TIMESTAMP,
    status          VARCHAR(20),
    stage_results   JSONB,
    new_leads       INTEGER DEFAULT 0,
    updated_leads   INTEGER DEFAULT 0,
    total_leads     INTEGER DEFAULT 0,
    error_log       TEXT
);

-- Staging tables for raw ingested data
CREATE TABLE IF NOT EXISTS staging_npi (
    id          SERIAL PRIMARY KEY,
    npi_number  VARCHAR(10) UNIQUE NOT NULL,
    raw_data    JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging_adph (
    id              SERIAL PRIMARY KEY,
    license_number  VARCHAR(50) UNIQUE NOT NULL,
    raw_data        JSONB NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging_cms (
    id              SERIAL PRIMARY KEY,
    provider_id     VARCHAR(20) UNIQUE NOT NULL,
    raw_data        JSONB NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_leads_facility_type ON leads(facility_type);
CREATE INDEX IF NOT EXISTS idx_leads_priority_tier ON leads(priority_tier);
CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(lead_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_city ON leads(city);
CREATE INDEX IF NOT EXISTS idx_leads_npi ON leads(npi_number);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_county ON leads(county);
CREATE INDEX IF NOT EXISTS idx_lead_sources_lead_id ON lead_sources(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_score_history_lead_id ON lead_score_history(lead_id);
