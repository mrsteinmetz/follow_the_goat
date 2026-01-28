-- Actions Table for System Event Logging
-- =========================================
-- General-purpose table for logging system events and automated actions
-- Supports multiple event types for extensibility
--
-- Usage:
--   psql -U ftg_user -d solcatcher -f scripts/create_actions_table.sql

CREATE TABLE IF NOT EXISTS actions (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    triggered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_actions_event_type ON actions(event_type);
CREATE INDEX IF NOT EXISTS idx_actions_triggered_at ON actions(triggered_at);
CREATE INDEX IF NOT EXISTS idx_actions_success ON actions(success);

-- Comments for documentation
COMMENT ON TABLE actions IS 'Logs all automated system actions and events';
COMMENT ON COLUMN actions.event_type IS 'Type of event: stream_restart, etc. Extensible for future events';
COMMENT ON COLUMN actions.triggered_at IS 'When the action was triggered';
COMMENT ON COLUMN actions.success IS 'Whether the action completed successfully';
COMMENT ON COLUMN actions.error_message IS 'Error details if action failed';
COMMENT ON COLUMN actions.metadata IS 'Flexible JSON field for event-specific data (latency, stream IDs, API responses)';
