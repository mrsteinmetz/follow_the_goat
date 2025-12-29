-- Add BTC and ETH price columns to buyin_trail_minutes table
-- This script is for existing tables that don't have these columns yet
-- Safe to run multiple times (uses IF NOT EXISTS-equivalent ALTER TABLE)

-- Add BTC Price Movements columns
ALTER TABLE buyin_trail_minutes
ADD COLUMN IF NOT EXISTS btc_price_change_1m DOUBLE AFTER pm_avg_price,
ADD COLUMN IF NOT EXISTS btc_price_change_5m DOUBLE AFTER btc_price_change_1m,
ADD COLUMN IF NOT EXISTS btc_price_change_10m DOUBLE AFTER btc_price_change_5m,
ADD COLUMN IF NOT EXISTS btc_volatility_pct DOUBLE AFTER btc_price_change_10m,
ADD COLUMN IF NOT EXISTS btc_open_price DOUBLE AFTER btc_volatility_pct,
ADD COLUMN IF NOT EXISTS btc_close_price DOUBLE AFTER btc_open_price;

-- Add ETH Price Movements columns
ALTER TABLE buyin_trail_minutes
ADD COLUMN IF NOT EXISTS eth_price_change_1m DOUBLE AFTER btc_close_price,
ADD COLUMN IF NOT EXISTS eth_price_change_5m DOUBLE AFTER eth_price_change_1m,
ADD COLUMN IF NOT EXISTS eth_price_change_10m DOUBLE AFTER eth_price_change_5m,
ADD COLUMN IF NOT EXISTS eth_volatility_pct DOUBLE AFTER eth_price_change_10m,
ADD COLUMN IF NOT EXISTS eth_open_price DOUBLE AFTER eth_volatility_pct,
ADD COLUMN IF NOT EXISTS eth_close_price DOUBLE AFTER eth_open_price;

