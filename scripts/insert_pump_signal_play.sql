-- Insert the Pump Signal Auto-Entry play into follow_the_goat_plays
-- Run once to create the play. Note the returned ID and set PUMP_SIGNAL_PLAY_ID env var.

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
    '{"tolerance_rules": {"decreases": [{"range": [-999999, 0], "tolerance": 0.001}], "increases": [{"range": [0.0, 0.002], "tolerance": 0.0015}, {"range": [0.002, 0.004], "tolerance": 0.001}, {"range": [0.004, 1.0], "tolerance": 0.0005}]}}'::jsonb
)
RETURNING id;
