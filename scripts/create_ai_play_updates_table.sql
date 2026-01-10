-- Table to track AI play updates history
-- Records when auto-filter patterns update plays with pattern_update_by_ai=1

CREATE TABLE IF NOT EXISTS ai_play_updates (
    id SERIAL PRIMARY KEY,
    play_id INTEGER NOT NULL,
    play_name VARCHAR(255),
    project_id INTEGER,
    project_name VARCHAR(255),
    pattern_count INTEGER,
    filters_applied INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_id VARCHAR(50),
    status VARCHAR(20) DEFAULT 'success'
);

CREATE INDEX idx_ai_play_updates_play_id ON ai_play_updates(play_id);
CREATE INDEX idx_ai_play_updates_updated_at ON ai_play_updates(updated_at DESC);
CREATE INDEX idx_ai_play_updates_run_id ON ai_play_updates(run_id);

COMMENT ON TABLE ai_play_updates IS 'Tracks history of plays updated by auto-filter AI system';
COMMENT ON COLUMN ai_play_updates.play_id IS 'FK to follow_the_goat_plays.id';
COMMENT ON COLUMN ai_play_updates.pattern_count IS 'Number of patterns in the auto-filter project';
COMMENT ON COLUMN ai_play_updates.filters_applied IS 'Number of filters applied to the play';
COMMENT ON COLUMN ai_play_updates.run_id IS 'Links to the scheduler run that performed the update';
