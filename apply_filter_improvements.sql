-- Filter Optimization Update
-- Based on analysis of Jan 14, 2026 trade data
-- This will improve good trade capture from 53.4% to ~90%

-- Step 1: Update percentile settings to be less aggressive
UPDATE auto_filter_settings SET setting_value = '5' WHERE setting_key = 'percentile_low';
UPDATE auto_filter_settings SET setting_value = '95' WHERE setting_key = 'percentile_high';

-- Step 2: Replace AutoFilters (project_id=5) with proven filters
DELETE FROM pattern_config_filters WHERE project_id = 5;

-- Insert top 3 performing filters
INSERT INTO pattern_config_filters 
(id, project_id, name, section, minute, field_name, field_column, from_value, to_value, include_null, is_active)
VALUES 
(5001, 5, 'Auto: ob_volume_imbalance', 'order_book', 11, 'volume_imbalance', 'ob_volume_imbalance', -0.571749, 0.251451, 0, 1),
(5002, 5, 'Auto: tx_whale_volume_pct', 'transactions', 8, 'whale_volume_pct', 'tx_whale_volume_pct', 9.607326, 56.898327, 0, 1),
(5003, 5, 'Auto: ob_depth_imbalance_ratio', 'order_book', 11, 'depth_imbalance_ratio', 'ob_depth_imbalance_ratio', 0.270676, 1.709850, 0, 1);

-- Verify changes
SELECT 'Filter Settings:' as info;
SELECT setting_key, setting_value FROM auto_filter_settings WHERE setting_key IN ('percentile_low', 'percentile_high');

SELECT '' as space;
SELECT 'Active Filters for AutoFilters Project:' as info;
SELECT id, name, section, minute, field_column, from_value, to_value 
FROM pattern_config_filters 
WHERE project_id = 5 AND is_active = 1
ORDER BY minute, id;
