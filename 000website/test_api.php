<?php
// Test if PHP can reach the API
header('Content-Type: application/json');

$api_url = 'http://127.0.0.1:5051/health';

$ch = curl_init();
curl_setopt_array($ch, [
    CURLOPT_URL => $api_url,
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT => 5,
    CURLOPT_CONNECTTIMEOUT => 5,
]);

$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

echo json_encode([
    'api_url' => $api_url,
    'http_code' => $httpCode,
    'response' => $response,
    'error' => $error ?: null,
    'php_version' => phpversion(),
    'curl_enabled' => function_exists('curl_init'),
], JSON_PRETTY_PRINT);

