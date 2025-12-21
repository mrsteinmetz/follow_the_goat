<?php
/**
 * DuckDB API Client
 * 
 * Simplified client for the Python DuckDB API server.
 * Migrated from: 000old_code/solana_node/chart/build_pattern_config/DuckDBClient.php
 */

class DuckDBClient {
    private string $apiBaseUrl;
    private int $timeout;
    
    public function __construct(string $apiBaseUrl = 'http://127.0.0.1:5050', int $timeout = 30) {
        $this->apiBaseUrl = rtrim($apiBaseUrl, '/');
        $this->timeout = $timeout;
    }
    
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
            error_log("DuckDB API HTTP error: {$httpCode}");
            return null;
        }
        
        $decoded = json_decode($response, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            error_log("DuckDB API JSON decode error: " . json_last_error_msg());
            return null;
        }
        
        return $decoded;
    }
    
    /**
     * Check if API is available
     */
    public function isAvailable(): bool {
        $result = $this->get('/health');
        return $result !== null && isset($result['status']) && $result['status'] === 'ok';
    }
    
    /**
     * Get health check info
     */
    public function healthCheck(): ?array {
        return $this->get('/health');
    }
    
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
    
    /**
     * Get database statistics
     * 
     * @return array|null Stats array or null on error
     */
    public function getStats(): ?array {
        return $this->get('/stats');
    }
}

