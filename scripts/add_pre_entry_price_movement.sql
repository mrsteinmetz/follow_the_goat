-- Add pre-entry price movement analysis columns to buyin_trail_minutes
-- These columns store price movement BEFORE entry to filter out falling-price entries

ALTER TABLE buyin_trail_minutes 
ADD COLUMN IF NOT EXISTS pre_entry_price_1m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_price_2m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_price_5m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_price_10m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_1m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_2m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_5m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_10m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_trend VARCHAR(20);

-- Add index on pre_entry_change_10m for filtering queries
CREATE INDEX IF NOT EXISTS idx_buyin_trail_pre_entry_change_10m 
ON buyin_trail_minutes(pre_entry_change_10m) 
WHERE minute = 0;

-- Add comment
COMMENT ON COLUMN buyin_trail_minutes.pre_entry_change_10m IS 'Price change % from 10 minutes before entry - used to filter falling-price entries';
COMMENT ON COLUMN buyin_trail_minutes.pre_entry_trend IS 'Trend direction before entry: rising, falling, flat, unknown';
