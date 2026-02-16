<?php
/**
 * Central Configuration
 * 
 * This file is auto-updated by start_scheduler.sh with the current WSL IP.
 * If running on Windows, use 127.0.0.1 (localhost).
 * If running from WSL, the startup script sets the correct IP.
 */

// Load .env from project root (same file Python uses) so PHP gets DB_* without web server env
if (!function_exists('_ftg_load_dotenv')) {
    function _ftg_load_dotenv(string $path): array {
        $env = [];
        if (!is_readable($path)) return $env;
        $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        foreach ($lines as $line) {
            $line = trim($line);
            if ($line === '' || $line[0] === '#') continue;
            if (preg_match('/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/', $line, $m)) {
                $v = trim($m[2]);
                if ((strlen($v) >= 2 && $v[0] === "'" && substr($v, -1) === "'") || (strlen($v) >= 2 && $v[0] === '"' && substr($v, -1) === '"'))
                    $v = substr($v, 1, -1);
                $env[$m[1]] = $v;
            }
        }
        return $env;
    }
}
$ftg_env = _ftg_load_dotenv(__DIR__ . '/../../.env');

// WSL IP - updated by start_scheduler.sh
define('WSL_HOST_IP', getenv('WSL_HOST_IP') ?: '172.19.254.84');

// Website API (Flask website_api.py, port 5051)
define('DATABASE_API_URL', 'http://127.0.0.1:5051');
define('DATABASE_API_PUBLIC_URL', 'http://195.201.84.5:5051');

// PostgreSQL: .env first (same as Python), then getenv(), then defaults
define('PG_HOST', $ftg_env['DB_HOST'] ?? getenv('DB_HOST') ?: '127.0.0.1');
define('PG_PORT', $ftg_env['DB_PORT'] ?? getenv('DB_PORT') ?: '5432');
define('PG_DATABASE', $ftg_env['DB_DATABASE'] ?? getenv('DB_DATABASE') ?: 'solcatcher');
define('PG_USER', $ftg_env['DB_USER'] ?? getenv('DB_USER') ?: 'ftg_user');
define('PG_PASSWORD', $ftg_env['DB_PASSWORD'] ?? getenv('DB_PASSWORD') ?: '');

// Legacy alias for backward compatibility
define('DUCKDB_API_URL', DATABASE_API_URL);

// .NET Webhook API URL (running on Windows IIS)
define('WEBHOOK_API_URL', 'http://195.201.84.5');

// Include authentication system
require_once __DIR__ . '/auth.php';

// Auto-require authentication for all pages (auth.php handles login page exclusion)
requireAuth();

