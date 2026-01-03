<?php
/**
 * API Proxy - Forwards requests from browser to Flask API
 * 
 * This proxy allows the browser to make API calls without CORS issues
 * and without needing to expose the Flask API publicly.
 * 
 * Usage: /api/proxy.php?endpoint=/plays/performance&hours=all
 * Or with PATH_INFO: /api/proxy.php/plays/performance?hours=all
 */

// Flask API URL (internal server address)
define('FLASK_API_URL', 'http://127.0.0.1:5051');

// Set JSON content type for response
header('Content-Type: application/json');

// Get the endpoint from query string or path info
$endpoint = '';

// Try PATH_INFO first (cleaner URLs)
if (!empty($_SERVER['PATH_INFO'])) {
    $endpoint = $_SERVER['PATH_INFO'];
} elseif (isset($_GET['endpoint'])) {
    $endpoint = $_GET['endpoint'];
    unset($_GET['endpoint']); // Remove from query params
}

if (empty($endpoint)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'error' => 'No endpoint specified']);
    exit;
}

// Ensure endpoint starts with /
if ($endpoint[0] !== '/') {
    $endpoint = '/' . $endpoint;
}

// Build the full URL
$url = FLASK_API_URL . $endpoint;

// Add remaining query parameters
$queryParams = $_GET;
if (!empty($queryParams)) {
    $url .= '?' . http_build_query($queryParams);
}

// Initialize cURL
$ch = curl_init();

// Get request method
$method = $_SERVER['REQUEST_METHOD'];

// Set cURL options
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 30);
curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);

// Handle different HTTP methods
switch ($method) {
    case 'POST':
        curl_setopt($ch, CURLOPT_POST, true);
        $input = file_get_contents('php://input');
        if ($input) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $input);
            curl_setopt($ch, CURLOPT_HTTPHEADER, [
                'Content-Type: application/json',
                'Content-Length: ' . strlen($input)
            ]);
        }
        break;
        
    case 'PUT':
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
        $input = file_get_contents('php://input');
        if ($input) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $input);
            curl_setopt($ch, CURLOPT_HTTPHEADER, [
                'Content-Type: application/json',
                'Content-Length: ' . strlen($input)
            ]);
        }
        break;
        
    case 'DELETE':
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'DELETE');
        break;
        
    case 'GET':
    default:
        // GET is default
        break;
}

// Execute request
$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);

curl_close($ch);

// Handle errors
if ($error) {
    http_response_code(503);
    echo json_encode([
        'success' => false, 
        'error' => 'API connection failed: ' . $error,
        'hint' => 'Ensure master.py is running on the server'
    ]);
    exit;
}

// Forward the HTTP status code
http_response_code($httpCode);

// Return the response
echo $response;

