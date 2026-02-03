<?php
/**
 * Central Database API Client
 * 
 * PHP client for the Python Flask API server (website_api.py).
 * Provides access to PostgreSQL database through unified API.
 * 
 * Port 5051: Flask Website API (website_api.py)
 * Backend: PostgreSQL (shared by all processes)
 */

class DatabaseClient {
    private string $apiBaseUrl;
    private int $timeout;
    private $curlHandle = null;  // Reuse curl handle for connection pooling
    
    public function __construct(string $apiBaseUrl = 'http://127.0.0.1:5051', int $timeout = 30) {
        $this->apiBaseUrl = rtrim($apiBaseUrl, '/');
        $this->timeout = $timeout;
    }
    
    public function __destruct() {
        if ($this->curlHandle !== null) {
            curl_close($this->curlHandle);
        }
    }
    
    // =========================================================================
    // HTTP Request Methods
    // =========================================================================
    
    /**
     * Make a GET request to the API
     */
    public function get(string $endpoint, array $params = []): ?array {
        $url = $this->apiBaseUrl . $endpoint;
        if (!empty($params)) {
            $url .= '?' . http_build_query($params);
        }
        
        return $this->request($url, 'GET');
    }
    
    /**
     * Make a POST request to the API
     */
    private function post(string $endpoint, array $data = []): ?array {
        $url = $this->apiBaseUrl . $endpoint;
        return $this->request($url, 'POST', $data);
    }
    
    /**
     * Make a PUT request to the API
     */
    private function put(string $endpoint, array $data = []): ?array {
        $url = $this->apiBaseUrl . $endpoint;
        return $this->request($url, 'PUT', $data);
    }
    
    /**
     * Make a DELETE request to the API
     */
    private function delete(string $endpoint): ?array {
        $url = $this->apiBaseUrl . $endpoint;
        return $this->request($url, 'DELETE');
    }
    
    /**
     * Make HTTP request using cURL with connection reuse
     */
    private function request(string $url, string $method = 'GET', ?array $data = null): ?array {
        // Reuse curl handle for connection pooling (HTTP keep-alive)
        if ($this->curlHandle === null) {
            $this->curlHandle = curl_init();
            curl_setopt_array($this->curlHandle, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => $this->timeout,
                CURLOPT_CONNECTTIMEOUT => 5,
                CURLOPT_HTTPHEADER => [
                    'Content-Type: application/json', 
                    'Accept: application/json',
                    'Connection: keep-alive'  // Enable HTTP keep-alive for faster requests
                ],
                CURLOPT_TCP_KEEPALIVE => 1,  // Enable TCP keep-alive
            ]);
        }
        
        // Reset options for this request
        curl_setopt($this->curlHandle, CURLOPT_URL, $url);
        
        if ($method === 'POST') {
            curl_setopt($this->curlHandle, CURLOPT_POST, true);
            if ($data !== null) {
                curl_setopt($this->curlHandle, CURLOPT_POSTFIELDS, json_encode($data));
            }
        } elseif ($method === 'PUT') {
            curl_setopt($this->curlHandle, CURLOPT_CUSTOMREQUEST, 'PUT');
            if ($data !== null) {
                curl_setopt($this->curlHandle, CURLOPT_POSTFIELDS, json_encode($data));
            }
        } elseif ($method === 'DELETE') {
            curl_setopt($this->curlHandle, CURLOPT_CUSTOMREQUEST, 'DELETE');
        } else {
            curl_setopt($this->curlHandle, CURLOPT_HTTPGET, true);
        }
        
        $response = curl_exec($this->curlHandle);
        $httpCode = curl_getinfo($this->curlHandle, CURLINFO_HTTP_CODE);
        $error = curl_error($this->curlHandle);
        
        if ($error) {
            error_log("Database API cURL error: {$error}");
            // Reset handle on error
            curl_close($this->curlHandle);
            $this->curlHandle = null;
            return null;
        }
        
        if ($httpCode >= 400) {
            // Only log server errors (500+) and connection failures
            // 404s are expected for optional data (e.g., missing projects)
            if ($httpCode >= 500) {
                error_log("Database API HTTP error: {$httpCode} - Response: {$response}");
            }
            return null;
        }
        
        $decoded = json_decode($response, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            error_log("Database API JSON decode error: " . json_last_error_msg());
            return null;
        }
        
        return $decoded;
    }
    
    // =========================================================================
    // Health & Status
    // =========================================================================
    
    /**
     * Check if API is available
     */
    public function isAvailable(): bool {
        $result = $this->get('/health');
        return $result !== null && isset($result['status']) && in_array($result['status'], ['ok', 'degraded', 'healthy']);
    }
    
    /**
     * Get health check info
     */
    public function healthCheck(): ?array {
        return $this->get('/health');
    }
    
    /**
     * Get database statistics
     */
    public function getStats(): ?array {
        return $this->get('/stats');
    }
    
    /**
     * Get scheduler job status - shows when each job last ran
     */
    public function getSchedulerStatus(): ?array {
        return $this->get('/scheduler_status');
    }

    /**
     * Get scheduler components (canonical dashboard source).
     */
    public function getSchedulerComponents(): ?array {
        return $this->get('/scheduler/components');
    }

    /**
     * Enable/disable a scheduler component.
     */
    public function setSchedulerComponentEnabled(string $componentId, bool $enabled, ?string $note = null): ?array {
        return $this->put("/scheduler/components/{$componentId}", [
            'enabled' => $enabled,
            'note' => $note
        ]);
    }

    /**
     * Get recent scheduler errors.
     */
    public function getSchedulerErrors(?string $componentId = null, float $hours = 24.0, int $limit = 200): ?array {
        $params = [
            'hours' => $hours,
            'limit' => $limit
        ];
        if ($componentId !== null && $componentId !== '') {
            $params['component_id'] = $componentId;
        }
        return $this->get('/scheduler/errors', $params);
    }
    
    /**
     * Get trades diagnostic - compare trade counts across all data sources
     * 
     * @param int $minutes Time window in minutes (default: 5)
     */
    public function getTradesDiagnostic(int $minutes = 5): ?array {
        return $this->get('/trades_diagnostic', ['minutes' => $minutes]);
    }
    
    // =========================================================================
    // Plays
    // =========================================================================
    
    /**
     * Get all plays
     */
    public function getPlays(): ?array {
        return $this->get('/plays');
    }
    
    /**
     * Get a single play by ID
     */
    public function getPlay(int $playId): ?array {
        return $this->get("/plays/{$playId}");
    }
    
    /**
     * Get a single play with all fields for editing
     */
    public function getPlayForEdit(int $playId): ?array {
        return $this->get("/plays/{$playId}/for_edit");
    }
    
    /**
     * Create a new play
     * 
     * @param array $data Play data with keys: name, description, find_wallets_sql, etc.
     */
    public function createPlay(array $data): ?array {
        return $this->post('/plays', $data);
    }
    
    /**
     * Update a play
     * 
     * @param int $playId Play ID
     * @param array $data Fields to update
     */
    public function updatePlay(int $playId, array $data): ?array {
        return $this->put("/plays/{$playId}", $data);
    }
    
    /**
     * Delete a play
     */
    public function deletePlay(int $playId): ?array {
        return $this->delete("/plays/{$playId}");
    }
    
    /**
     * Duplicate a play
     * 
     * @param int $playId Play ID to duplicate
     * @param string $newName Name for the duplicated play
     */
    public function duplicatePlay(int $playId, string $newName): ?array {
        return $this->post("/plays/{$playId}/duplicate", ['new_name' => $newName]);
    }
    
    /**
     * Get performance metrics for a single play
     * 
     * @param int $playId Play ID
     * @param string $hours Time window ('all', '24', '12', '6', '2')
     */
    public function getPlayPerformance(int $playId, string $hours = 'all'): ?array {
        return $this->get("/plays/{$playId}/performance", ['hours' => $hours]);
    }
    
    /**
     * Get performance metrics for all plays (batch operation)
     * 
     * @param string $hours Time window ('all', '24', '12', '6', '2')
     */
    public function getAllPlaysPerformance(string $hours = 'all'): ?array {
        return $this->get('/plays/performance', ['hours' => $hours]);
    }
    
    // =========================================================================
    // Buyins (Trades)
    // =========================================================================
    
    /**
     * Get buyins/trades
     * 
     * @param int|null $playId Filter by play ID
     * @param string|null $status Filter by status (pending, sold, no_go, etc.)
     * @param string $hours Limit to last N hours (default: 24)
     * @param int $limit Max records (default: 100)
     * @param string|null $excludeStatus Exclude statuses (comma-separated: 'no_go,error')
     */
    public function getBuyins(?int $playId = null, ?string $status = null, string $hours = '24', int $limit = 100, ?string $excludeStatus = null): ?array {
        $params = ['hours' => $hours, 'limit' => $limit];
        if ($playId !== null) $params['play_id'] = $playId;
        if ($status !== null) $params['status'] = $status;
        if ($excludeStatus !== null) $params['exclude_status'] = $excludeStatus;
        
        return $this->get('/buyins', $params);
    }
    
    /**
     * Create a new buyin/trade
     */
    public function createBuyin(array $data): ?array {
        return $this->post('/buyins', $data);
    }
    
    /**
     * Update a buyin/trade
     */
    public function updateBuyin(int $buyinId, array $data): ?array {
        return $this->put("/buyins/{$buyinId}", $data);
    }
    
    /**
     * Get a single buyin/trade by ID
     * 
     * @param int $buyinId Buyin ID
     */
    public function getSingleBuyin(int $buyinId): ?array {
        return $this->get("/buyins/{$buyinId}");
    }
    
    /**
     * Delete all no_go trades older than 24 hours
     */
    public function cleanupNoGos(): ?array {
        return $this->delete('/buyins/cleanup_no_gos');
    }
    
    // =========================================================================
    // Price Checks
    // =========================================================================
    
    /**
     * Get price checks for a buyin
     */
    public function getPriceChecks(int $buyinId, string $hours = '24', int $limit = 100): ?array {
        return $this->get('/price_checks', [
            'buyin_id' => $buyinId,
            'hours' => $hours,
            'limit' => $limit
        ]);
    }
    
    /**
     * Create a price check
     */
    public function createPriceCheck(array $data): ?array {
        return $this->post('/price_checks', $data);
    }
    
    // =========================================================================
    // Price Points
    // =========================================================================
    
    /**
     * Get price points for charting
     * 
     * @param string $token Token symbol (BTC, ETH, SOL)
     * @param string $startDatetime Start datetime
     * @param string $endDatetime End datetime
     * @param int $maxPoints Maximum number of points to return (default: 5000, set to 0 for all)
     * @return array|null Array with 'prices' and 'count' or null on error
     */
    public function getPricePoints(string $token, string $startDatetime, string $endDatetime, int $maxPoints = 5000): ?array {
        $data = [
            'token' => $token,
            'start_datetime' => $startDatetime,
            'end_datetime' => $endDatetime,
        ];
        if ($maxPoints > 0) {
            $data['max_points'] = $maxPoints;
        }
        return $this->post('/price_points', $data);
    }
    
    /**
     * Get latest prices for all tokens
     * 
     * @return array|null Array with 'prices' keyed by token or null on error
     */
    public function getLatestPrices(): ?array {
        return $this->get('/latest_prices');
    }
    
    // =========================================================================
    // Price Analysis & Cycle Tracker
    // =========================================================================
    
    /**
     * Get price analysis data
     */
    public function getPriceAnalysis(int $coinId = 5, string $hours = '24', int $limit = 100): ?array {
        return $this->get('/price_analysis', [
            'coin_id' => $coinId,
            'hours' => $hours,
            'limit' => $limit
        ]);
    }
    
    /**
     * Get cycle tracker data
     */
    public function getCycleTracker(?float $threshold = null, string $hours = '24', int $limit = 100): ?array {
        $params = ['hours' => $hours, 'limit' => $limit];
        if ($threshold !== null) $params['threshold'] = $threshold;
        
        return $this->get('/cycle_tracker', $params);
    }
    
    // =========================================================================
    // Wallet Profiles
    // =========================================================================
    
    /**
     * Get wallet profiles data
     * 
     * @param float|null $threshold Filter by threshold value
     * @param string $hours Time window ('all', '1', '24', etc.)
     * @param int $limit Max records to return
     * @param string $orderBy Ordering: 'recent' or 'trade_count'
     * @param string|null $wallet Filter by specific wallet address
     */
    public function getProfiles(
        ?float $threshold = null,
        string $hours = '24',
        int $limit = 100,
        string $orderBy = 'recent',
        ?string $wallet = null
    ): ?array {
        $params = [
            'hours' => $hours,
            'limit' => $limit,
            'order_by' => $orderBy
        ];
        
        if ($threshold !== null) $params['threshold'] = $threshold;
        if ($wallet !== null) $params['wallet'] = $wallet;
        
        return $this->get('/profiles', $params);
    }
    
    /**
     * Get wallet profiles statistics
     * 
     * @param float|null $threshold Filter by threshold value
     * @param string $hours Time window ('all', '1', '24', etc.)
     */
    public function getProfilesStats(?float $threshold = null, string $hours = 'all'): ?array {
        $params = ['hours' => $hours];
        if ($threshold !== null) $params['threshold'] = $threshold;
        
        return $this->get('/profiles/stats', $params);
    }
    
    // =========================================================================
    // Generic Query
    // =========================================================================
    
    /**
     * Execute a generic query
     * 
     * @param string $table Table name
     * @param array|null $columns Columns to select (null for all)
     * @param array|null $where WHERE conditions as key => value
     * @param string|null $orderBy ORDER BY clause
     * @param int $limit LIMIT clause
     * @param string $source Data source (legacy parameter, ignored)
     */
    public function query(
        string $table,
        ?array $columns = null,
        ?array $where = null,
        ?string $orderBy = null,
        int $limit = 100,
        string $source = 'auto'
    ): ?array {
        $data = [
            'table' => $table,
            'limit' => $limit,
            'source' => $source
        ];
        
        if ($columns !== null) $data['columns'] = $columns;
        if ($where !== null) $data['where'] = $where;
        if ($orderBy !== null) $data['order_by'] = $orderBy;
        
        return $this->post('/query', $data);
    }
    
    // =========================================================================
    // Pattern Config
    // =========================================================================
    
    /**
     * Get all pattern config projects with filter counts
     */
    public function getPatternProjects(): ?array {
        return $this->get('/patterns/projects');
    }
    
    /**
     * Get a single pattern config project by ID
     */
    public function getPatternProject(int $projectId): ?array {
        return $this->get("/patterns/projects/{$projectId}");
    }
    
    /**
     * Create a new pattern config project
     * 
     * @param string $name Project name
     * @param string|null $description Optional description
     */
    public function createPatternProject(string $name, ?string $description = null): ?array {
        return $this->post('/patterns/projects', [
            'name' => $name,
            'description' => $description
        ]);
    }
    
    /**
     * Delete a pattern config project and all its filters
     */
    public function deletePatternProject(int $projectId): ?array {
        return $this->delete("/patterns/projects/{$projectId}");
    }
    
    /**
     * Get all filters for a pattern config project
     */
    public function getPatternFilters(int $projectId): ?array {
        return $this->get("/patterns/projects/{$projectId}/filters");
    }
    
    /**
     * Create a new pattern config filter
     * 
     * @param array $data Filter data: project_id, name, field_name, section, minute, etc.
     */
    public function createPatternFilter(array $data): ?array {
        return $this->post('/patterns/filters', $data);
    }
    
    /**
     * Update a pattern config filter
     * 
     * @param int $filterId Filter ID
     * @param array $data Fields to update
     */
    public function updatePatternFilter(int $filterId, array $data): ?array {
        return $this->put("/patterns/filters/{$filterId}", $data);
    }
    
    /**
     * Delete a pattern config filter
     */
    public function deletePatternFilter(int $filterId): ?array {
        return $this->delete("/patterns/filters/{$filterId}");
    }
    
    // =========================================================================
    // Trail Data (for Pattern Builder analysis)
    // =========================================================================
    
    /**
     * Get available trail data sections and their fields
     * 
     * @param string|null $section Specific section to get fields for (null = list all sections)
     */
    public function getTrailSections(?string $section = null): ?array {
        $params = [];
        if ($section !== null) {
            $params['section'] = $section;
        }
        return $this->get('/trail/sections', $params);
    }
    
    /**
     * Get field statistics for a section/minute, broken down by gain ranges
     * 
     * @param array $options Query options:
     *   - project_id: Project ID for filters
     *   - section: Section name (price_movements, order_book_signals, etc.)
     *   - minute: Minute value (0-14)
     *   - status: Trade status filter (all, sold, no_go)
     *   - hours: Time window in hours
     *   - analyse_mode: 'all' or 'passed' (apply filters)
     */
    public function getTrailFieldStats(array $options): ?array {
        return $this->post('/trail/field_stats', $options);
    }
    
    /**
     * Get trade count distribution across gain ranges
     * 
     * @param array $options Query options:
     *   - project_id: Project ID for filters
     *   - minute: Minute value (0-14)
     *   - status: Trade status filter (all, sold, no_go)
     *   - hours: Time window in hours
     *   - apply_filters: Whether to apply project filters
     */
    public function getTrailGainDistribution(array $options): ?array {
        return $this->post('/trail/gain_distribution', $options);
    }
    
    /**
     * Get 15-minute trail data for a specific buyin
     * 
     * @param int $buyinId The buyin ID
     * @param string $source Data source (legacy parameter, ignored)
     * @return array|null Trail data (15 rows, one per minute) or null on error
     */
    public function getTrailForBuyin(int $buyinId, string $source = 'postgres'): ?array {
        return $this->get("/trail/buyin/{$buyinId}", ['source' => $source]);
    }
    
    // =========================================================================
    // Filter Analysis (Auto-filter suggestions dashboard)
    // =========================================================================
    
    /**
     * Get complete filter analysis dashboard data
     * Returns: summary, suggestions, combinations, scheduler runs, consistency data, etc.
     */
    public function getFilterAnalysisDashboard(): ?array {
        return $this->get('/filter-analysis/dashboard');
    }
    
    /**
     * Get auto filter settings
     */
    public function getFilterSettings(): ?array {
        return $this->get('/filter-analysis/settings');
    }
    
    /**
     * Save auto filter settings
     * 
     * @param array $settings Key-value pairs of settings to save
     */
    public function saveFilterSettings(array $settings): ?array {
        return $this->post('/filter-analysis/settings', ['settings' => $settings]);
    }
    
    // =========================================================================
    // Admin Operations
    // =========================================================================
    
    /**
     * Initialize database tables
     */
    public function initTables(): ?array {
        return $this->post('/admin/init_tables');
    }
    
    /**
     * Cleanup old data
     */
    public function cleanup(int $hours = 24): ?array {
        return $this->post('/admin/cleanup', ['hours' => $hours]);
    }
    
    /**
     * Sync data (legacy method, no-op)
     */
    public function syncFromMySQL(int $hours = 24, ?array $tables = null): ?array {
        $params = ['hours' => $hours];
        if ($tables !== null) {
            foreach ($tables as $table) {
                $params['tables[]'] = $table;
            }
        }
        
        $url = $this->apiBaseUrl . '/admin/sync_from_mysql?' . http_build_query($params);
        return $this->request($url, 'POST');
    }
    
    // =========================================================================
    // Live Trade Feed
    // =========================================================================
    
    /**
     * Get recent trades from sol_stablecoin_trades
     * 
     * @param int $limit Max number of trades to return
     * @param int $minutes Time window in minutes
     * @param string $direction Trade direction: 'buy', 'sell', or 'all'
     * @return array|null Array with 'trades', 'count', 'source' or null on error
     */
    public function getRecentTrades(int $limit = 100, int $minutes = 5, string $direction = 'buy'): ?array {
        return $this->get('/recent_trades', [
            'limit' => $limit,
            'minutes' => $minutes,
            'direction' => $direction
        ]);
    }
    
    /**
     * Get all wallets being tracked by active plays
     * 
     * Returns wallet addresses extracted from cashe_wallets_settings JSON field
     * 
     * @return array|null Array with 'plays', 'all_wallets', 'total_wallet_count' or null on error
     */
    public function getTrackedWallets(): ?array {
        return $this->get('/tracked_wallets');
    }
    
    /**
     * Get job execution metrics with execution time analysis
     * 
     * Returns per-job statistics including:
     * - avg_duration_ms: Average execution duration
     * - max_duration_ms: Maximum execution duration
     * - min_duration_ms: Minimum execution duration
     * - execution_count: Number of executions in time window
     * - error_count: Number of failed executions
     * - expected_interval_ms: Expected job interval
     * - is_slow: True if avg duration > 80% of expected interval
     * - recent_executions: Last 50 executions with timestamps and durations
     * 
     * @param int $hours Number of hours of history to analyze (default: 1)
     * @return array|null Array with 'jobs' containing metrics per job or null on error
     */
    public function getJobMetrics(float $hours = 1): ?array {
        return $this->get('/job_metrics', ['hours' => $hours]);
    }
    
    // =========================================================================
    // SQL Tester
    // =========================================================================
    
    /**
     * Execute custom SQL query (read-only)
     * 
     * @param string $sql SQL query to execute (SELECT only)
     * @return array|null Array with 'success', 'columns', 'rows', 'count' or null on error
     */
    public function executeSQL(string $sql): ?array {
        return $this->post('/query_sql', ['sql' => $sql]);
    }
    
    /**
     * Get database schema (all tables and columns)
     * 
     * @return array|null Array with 'success', 'schema' (table => columns), 'table_count' or null on error
     */
    public function getSchema(): ?array {
        return $this->get('/schema');
    }
}

// Backward compatibility alias
class_alias('DatabaseClient', 'DuckDBClient');
