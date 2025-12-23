-- Migration: Create whale_movements table
-- This table tracks whale wallet movements (large SOL holders)
-- Date: 2025-10-24

CREATE TABLE IF NOT EXISTS whale_movements (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    signature VARCHAR(88) NOT NULL UNIQUE,
    wallet_address VARCHAR(44) NOT NULL,
    whale_type VARCHAR(20) NOT NULL,
    current_balance DECIMAL(18,2),
    sol_change DECIMAL(18,4),
    abs_change DECIMAL(18,4),
    percentage_moved DECIMAL(5,2),
    direction VARCHAR(20),
    action VARCHAR(20),
    movement_significance VARCHAR(20),
    previous_balance DECIMAL(18,2),
    fee_paid DECIMAL(10,6),
    block_time BIGINT,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    slot BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_timestamp (timestamp),
    INDEX idx_wallet_address (wallet_address),
    INDEX idx_whale_type (whale_type),
    INDEX idx_movement_significance (movement_significance),
    INDEX idx_direction (direction),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Whale type values:
-- MEGA_WHALE: 100,000+ SOL
-- SUPER_WHALE: 50,000-100,000 SOL
-- WHALE: 10,000-50,000 SOL
-- LARGE_HOLDER: 5,000-10,000 SOL
-- MODERATE_HOLDER: 1,000-5,000 SOL

-- Movement significance values:
-- CRITICAL: 5,000+ SOL moved
-- HIGH: 1,000-5,000 SOL moved
-- MEDIUM: 500-1,000 SOL moved
-- LOW: 50-500 SOL moved

-- Direction values:
-- sending: Whale sent SOL out
-- receiving: Whale received SOL

