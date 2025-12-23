-- Add raw_data_json column to whale_movements table
-- This stores the complete raw transaction data from QuickNode for debugging and analysis

ALTER TABLE `whale_movements` 
ADD COLUMN `raw_data_json` LONGTEXT NULL COMMENT 'Complete raw transaction data from QuickNode stream';

