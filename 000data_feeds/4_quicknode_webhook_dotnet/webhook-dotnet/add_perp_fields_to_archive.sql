-- Add perp position fields to the archive table
-- Run this if you haven't already added these fields to sol_stablecoin_trades_archive

ALTER TABLE `sol_stablecoin_trades_archive` 
ADD COLUMN `has_perp_position` BOOLEAN DEFAULT FALSE COMMENT 'Whether wallet had open perp at time of trade',
ADD COLUMN `perp_platform` ENUM('drift', 'jupiter', 'mango', 'zeta') NULL COMMENT 'Which perp platform if position exists',
ADD COLUMN `perp_direction` ENUM('long', 'short') NULL COMMENT 'Direction of perp position',
ADD COLUMN `perp_size` DECIMAL(18,9) NULL COMMENT 'Size of perp position in SOL',
ADD COLUMN `perp_leverage` DECIMAL(10,2) NULL COMMENT 'Leverage used (e.g., 5.00 = 5x)',
ADD COLUMN `perp_entry_price` DECIMAL(12,2) NULL COMMENT 'Entry price of perp position',
ADD KEY `idx_has_perp` (`has_perp_position`),
ADD KEY `idx_perp_platform` (`perp_platform`),
ADD KEY `idx_perp_direction` (`perp_direction`),
ADD KEY `idx_wallet_perp` (`wallet_address`, `has_perp_position`, `perp_direction`);

