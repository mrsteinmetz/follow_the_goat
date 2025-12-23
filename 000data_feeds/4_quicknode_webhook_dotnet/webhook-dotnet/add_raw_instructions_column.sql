-- Add raw_instructions_data column to store the instruction data from perp transactions
-- This stores the raw Base58 instruction data for debugging and analysis

-- Main table
ALTER TABLE `sol_stablecoin_trades` 
ADD COLUMN `raw_instructions_data` TEXT NULL COMMENT 'JSON array of raw instruction data from perp programs';

-- Archive table
ALTER TABLE `sol_stablecoin_trades_archive` 
ADD COLUMN `raw_instructions_data` TEXT NULL COMMENT 'JSON array of raw instruction data from perp programs';

