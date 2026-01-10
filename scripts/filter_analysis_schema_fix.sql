-- Fix filter analysis schema to match the old working system
-- This updates the existing tables to support proper filter analysis

-- Update filter_reference_suggestions table with proper columns
DROP TABLE IF EXISTS filter_reference_suggestions CASCADE;

CREATE TABLE filter_reference_suggestions (
    id SERIAL PRIMARY KEY,
    filter_field_id INTEGER,
    column_name VARCHAR(100) NOT NULL,
    from_value DOUBLE PRECISION,
    to_value DOUBLE PRECISION,
    
    -- Effectiveness metrics
    total_trades INTEGER,
    good_trades_before INTEGER,
    bad_trades_before INTEGER,
    good_trades_after INTEGER,
    bad_trades_after INTEGER,
    good_trades_kept_pct DOUBLE PRECISION,
    bad_trades_removed_pct DOUBLE PRECISION,
    
    -- Bad trades breakdown (remaining after filter)
    bad_negative_count INTEGER,
    bad_0_to_01_count INTEGER,
    bad_01_to_02_count INTEGER,
    bad_02_to_03_count INTEGER,
    
    analysis_hours INTEGER DEFAULT 6,
    minute_analyzed INTEGER DEFAULT 0,
    section VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_filter_reference_column ON filter_reference_suggestions(column_name);
CREATE INDEX idx_filter_reference_created ON filter_reference_suggestions(created_at);
CREATE INDEX idx_filter_reference_effectiveness ON filter_reference_suggestions(bad_trades_removed_pct DESC);
CREATE INDEX idx_filter_reference_section ON filter_reference_suggestions(section);
CREATE INDEX idx_filter_reference_minute ON filter_reference_suggestions(minute_analyzed);

-- Update filter_combinations table
DROP TABLE IF EXISTS filter_combinations CASCADE;

CREATE TABLE filter_combinations (
    id SERIAL PRIMARY KEY,
    combination_name VARCHAR(255),
    filter_count INTEGER NOT NULL,
    filter_ids TEXT,
    filter_columns TEXT,
    
    -- Effectiveness metrics
    total_trades INTEGER,
    good_trades_before INTEGER,
    bad_trades_before INTEGER,
    good_trades_after INTEGER,
    bad_trades_after INTEGER,
    good_trades_kept_pct DOUBLE PRECISION,
    bad_trades_removed_pct DOUBLE PRECISION,
    
    -- Comparison metrics
    best_single_bad_removed_pct DOUBLE PRECISION,
    improvement_over_single DOUBLE PRECISION,
    
    -- Bad trades breakdown
    bad_negative_count INTEGER,
    bad_0_to_01_count INTEGER,
    bad_01_to_02_count INTEGER,
    bad_02_to_03_count INTEGER,
    
    analysis_hours INTEGER DEFAULT 6,
    minute_analyzed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_filter_combos_created ON filter_combinations(created_at);
CREATE INDEX idx_filter_combos_effectiveness ON filter_combinations(bad_trades_removed_pct DESC);
CREATE INDEX idx_filter_combos_filter_count ON filter_combinations(filter_count);

-- Create scheduler runs tracking table if not exists
CREATE TABLE IF NOT EXISTS filter_scheduler_runs (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',
    total_filters_analyzed INTEGER,
    filters_saved INTEGER,
    best_bad_removed_pct DOUBLE PRECISION,
    best_good_kept_pct DOUBLE PRECISION,
    analysis_hours INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_scheduler_runs_timestamp ON filter_scheduler_runs(run_timestamp DESC);
CREATE INDEX idx_scheduler_runs_status ON filter_scheduler_runs(status);
