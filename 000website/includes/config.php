<?php
/**
 * Central Configuration
 * 
 * This file is auto-updated by start_scheduler.sh with the current WSL IP.
 * If running on Windows, use 127.0.0.1 (localhost).
 * If running from WSL, the startup script sets the correct IP.
 */

// WSL IP - updated by start_scheduler.sh
// Default to localhost for Windows-only setups
define('WSL_HOST_IP', getenv('WSL_HOST_IP') ?: '172.19.254.84');

// PostgreSQL API URL (Flask website_api.py server)
// Use server IP for external access (required for CORS)
// Port 5051 = Website API (can restart freely)
// Port 5052 = Trading Logic API (master2.py local API)
define('DATABASE_API_URL', 'http://195.201.84.5:5051');

// Legacy alias for backward compatibility
define('DUCKDB_API_URL', DATABASE_API_URL);

// .NET Webhook API URL (running on Windows IIS)
define('WEBHOOK_API_URL', 'http://195.201.84.5');

// Include authentication system
require_once __DIR__ . '/auth.php';

// Auto-require authentication for all pages (auth.php handles login page exclusion)
requireAuth();

