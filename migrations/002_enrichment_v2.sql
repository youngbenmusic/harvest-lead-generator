-- Harvest Med Waste — Enrichment v2 Schema Updates
-- Migration 002: Add geocoding coordinates and date fields

ALTER TABLE leads ADD COLUMN IF NOT EXISTS latitude NUMERIC(9,6);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS longitude NUMERIC(9,6);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS facility_established_date DATE;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS contract_expiry_date DATE;
