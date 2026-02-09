-- Insert the Pump Signal Auto-Entry play into follow_the_goat_plays
-- Run once to create the play. Note the returned ID and set PUMP_SIGNAL_PLAY_ID env var.
--
-- Tolerance rules rationale (updated Feb 2026):
--   Based on simulation of ~2,900 price samples over 25 hours:
--   - Clean pumps (gain>0.3%, dip<0.15%) have avg worst dip of -0.052%
--   - With SL=0.15%, clean pump win rate = 88%, 0 stop-loss exits
--   - Breakeven filter precision = 23.5% (very achievable)
--
--   Stop-loss: 0.15% — above clean pump worst dip, limits bad-entry losses
--   0-0.3% gain: 0.20% trailing — clean pumps avg +0.297% gain, room to develop
--   0.3-0.6% gain: 0.12% trailing — start tightening
--   0.6%+ gain: 0.06% trailing — lock in profits

INSERT INTO follow_the_goat_plays (
    name,
    description,
    is_active,
    pattern_validator_enable,
    find_wallets_sql,
    max_buys_per_cycle,
    live_trades,
    sell_logic
) VALUES (
    'Pump Signal Auto-Entry',
    'Automated SOL long entry on pump detection via statistical filter rules',
    1,
    0,
    NULL,
    1,
    0,
    '{"tolerance_rules": {"decreases": [{"range": [-999999, 0], "tolerance": 0.0015}], "increases": [{"range": [0.0, 0.003], "tolerance": 0.002}, {"range": [0.003, 0.006], "tolerance": 0.0012}, {"range": [0.006, 1.0], "tolerance": 0.0006}]}}'::jsonb
)
RETURNING id;

-- =============================================================================
-- UPDATE existing play (run this if play already exists, e.g. play_id=3)
-- =============================================================================
-- UPDATE follow_the_goat_plays
-- SET sell_logic = '{"tolerance_rules": {"decreases": [{"range": [-999999, 0], "tolerance": 0.0015}], "increases": [{"range": [0.0, 0.003], "tolerance": 0.002}, {"range": [0.003, 0.006], "tolerance": 0.0012}, {"range": [0.006, 1.0], "tolerance": 0.0006}]}}'::jsonb
-- WHERE id = 3;
