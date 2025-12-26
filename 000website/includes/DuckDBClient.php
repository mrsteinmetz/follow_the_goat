<?php
/**
 * Central DuckDB API Client
 * 
 * PHP client for the Python DuckDB API server.
 * Provides access to both DuckDB (hot data) and MySQL (historical) through unified API.
 * 
 * Migrated from: 000old_code/solana_node/chart/build_pattern_config/DuckDBClient.php
 */

class DuckDBClient {
    private string $apiBaseUrl;
    private int $timeout;
    
    public function __construct(string $apiBaseUrl = 'http://127.0.0.1:5050', int $timeout = 30) {
        $this->apiBaseUrl = rtrim($apiBaseUrl, '/');
        $this->timeout = $timeout;
    }
    
    // =========================================================================
    // HTTP Request Methods
    // =========================================================================
    
    /**
     * Make a GET request to the API
     */
    private function get(string $endpoint, array $params = []): ?array {
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
     * Make HTTP request using cURL
     */
    private function request(string $url, string $method = 'GET', ?array $data = null): ?array {
        $ch = curl_init();
        
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => $this->timeout,
            CURLOPT_CONNECTTIMEOUT => 5,
            CURLOPT_HTTPHEADER => ['Content-Type: application/json', 'Accept: application/json'],
        ]);
        
        if ($method === 'POST') {
            curl_setopt($ch, CURLOPT_POST, true);
            if ($data !== null) {
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
            }
        } elseif ($method === 'PUT') {
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
            if ($data !== null) {
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
            }
        } elseif ($method === 'DELETE') {
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'DELETE');
        }
        
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);
        
        if ($error) {
            error_log("DuckDB API cURL error: {$error}");
            return null;
        }
        
        if ($httpCode >= 400) {
            error_log("DuckDB API HTTP error: {$httpCode} - Response: {$response}");
            return null;
        }
        
        $decoded = json_decode($response, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            error_log("DuckDB API JSON decode error: " . json_last_error_msg());
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
        return $result !== null && isset($result['status']) && in_array($result['status'], ['ok', 'degraded']);
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
     * @param string $hours Limit to last N hours (default: 24, use 'all' for MySQL historical)
     * @param int $limit Max records (default: 100)
     */
    public function getBuyins(?int $playId = null, ?string $status = null, string $hours = '24', int $limit = 100): ?array {
        $params = ['hours' => $hours, 'limit' => $limit];
        if ($playId !== null) $params['play_id'] = $playId;
        if ($status !== null) $params['status'] = $status;
        
        return $this->get('/buyins', $params);
    }
    
    /**
     * Create a new buyin/trade (dual-write to DuckDB + MySQL)
     */
    public function createBuyin(array $data): ?array {
        return $this->post('/buyins', $data);
    }
    
    /**
     * Update a buyin/trade (dual-write to DuckDB + MySQL)
     */
    public function updateBuyin(int $buyinId, array $data): ?array {
        return $this->put("/buyins/{$buyinId}", $data);
    }
    
    /**
     * Get a single buyin/trade by ID
     * 
     * @param int $buyinId Buyin ID
     * @param string $source 'live' for active trades, 'archive' for completed
     */
    public function getSingleBuyin(int $buyinId, string $source = 'live'): ?array {
        return $this->get("/buyins/{$buyinId}", ['source' => $source]);
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
     * Create a price check (dual-write)
     */
    public function createPriceCheck(array $data): ?array {
        return $this->post('/price_checks', $data);
    }
    
    // =========================================================================
    // Price Points (Legacy)
    // =========================================================================
    
    /**
     * Get price points for charting
     * 
     * @param string $token Token symbol (BTC, ETH, SOL)
     * @param string $startDatetime Start datetime
     * @param string $endDatetime End datetime
     * @return array|null Array with 'prices' and 'count' or null on error
     */
    public function getPricePoints(string $token, string $startDatetime, string $endDatetime): ?array {
        return $this->post('/price_points', [
            'token' => $token,
            'start_datetime' => $startDatetime,
            'end_datetime' => $endDatetime,
        ]);
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
     * @param string $source Data source: 'auto', 'duckdb', or 'mysql'
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
    // Admin Operations
    // =========================================================================
    
    /**
     * Initialize DuckDB tables
     */
    public function initTables(): ?array {
        return $this->post('/admin/init_tables');
    }
    
    /**
     * Cleanup old data from DuckDB hot tables
     */
    public function cleanup(int $hours = 24): ?array {
        return $this->post('/admin/cleanup', ['hours' => $hours]);
    }
    
    /**
     * Sync data from MySQL to DuckDB
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
}
