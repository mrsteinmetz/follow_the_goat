-- Verification script for raw_instructions_data field
-- Run this after deploying the fix to verify the field is populated correctly

-- 1. Check if the column exists in both tables
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'solcatcher'
  AND TABLE_NAME IN ('sol_stablecoin_trades', 'sol_stablecoin_trades_archive')
  AND COLUMN_NAME = 'raw_instructions_data';

-- 2. Check the most recent records with raw_instructions_data
SELECT 
    id,
    signature,
    wallet_address,
    direction,
    has_perp_position,
    perp_platform,
    CASE 
        WHEN raw_instructions_data IS NULL THEN 'NULL'
        WHEN raw_instructions_data = '' THEN 'EMPTY'
        ELSE 'POPULATED'
    END AS raw_data_status,
    CHAR_LENGTH(raw_instructions_data) AS data_length,
    created_at
FROM sol_stablecoin_trades
ORDER BY created_at DESC
LIMIT 10;

-- 3. View actual raw_instructions_data for the most recent record
SELECT 
    signature,
    wallet_address,
    raw_instructions_data,
    created_at
FROM sol_stablecoin_trades
WHERE raw_instructions_data IS NOT NULL
ORDER BY created_at DESC
LIMIT 1;

-- 4. Count records by raw_instructions_data status
SELECT 
    CASE 
        WHEN raw_instructions_data IS NULL THEN 'NULL'
        WHEN raw_instructions_data = '' THEN 'EMPTY'
        ELSE 'POPULATED'
    END AS status,
    COUNT(*) AS count
FROM sol_stablecoin_trades
GROUP BY status;

-- 5. Parse a specific instruction (example using JSON functions)
-- This shows how to extract data from the JSON array
SELECT 
    signature,
    JSON_LENGTH(raw_instructions_data) AS instruction_count,
    JSON_EXTRACT(raw_instructions_data, '$[0].program_id') AS first_program_id,
    JSON_EXTRACT(raw_instructions_data, '$[0].base58_data') AS first_instruction_data,
    created_at
FROM sol_stablecoin_trades
WHERE raw_instructions_data IS NOT NULL
  AND JSON_VALID(raw_instructions_data)
ORDER BY created_at DESC
LIMIT 5;

-- 6. Find transactions with perp positions and their instruction data
SELECT 
    signature,
    perp_platform,
    perp_direction,
    JSON_LENGTH(raw_instructions_data) AS num_instructions,
    raw_instructions_data,
    created_at
FROM sol_stablecoin_trades
WHERE has_perp_position = TRUE
  AND raw_instructions_data IS NOT NULL
ORDER BY created_at DESC
LIMIT 3;

