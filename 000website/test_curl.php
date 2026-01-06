<?php
// Direct test of the HTTP request
date_default_timezone_set('UTC');

$url = 'http://127.0.0.1:5051/price_points';
$data = [
    'token' => 'SOL',
    'start_datetime' => gmdate('Y-m-d H:i:s', strtotime('-2 hours')),
    'end_datetime' => gmdate('Y-m-d H:i:s'),
    'max_points' => 10
];

echo "URL: $url\n";
echo "Data: " . json_encode($data) . "\n\n";

// Test with simple curl
$ch = curl_init();
curl_setopt_array($ch, [
    CURLOPT_URL => $url,
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST => true,
    CURLOPT_POSTFIELDS => json_encode($data),
    CURLOPT_HTTPHEADER => [
        'Content-Type: application/json',
        'Accept: application/json'
    ],
    CURLOPT_TIMEOUT => 30
]);

$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

echo "HTTP Code: $httpCode\n";
echo "Error: " . ($error ?: 'none') . "\n";
echo "Response: " . substr($response, 0, 500) . "\n\n";

if ($response) {
    $decoded = json_decode($response, true);
    if ($decoded) {
        echo "Decoded successfully:\n";
        echo "Status: " . ($decoded['status'] ?? 'N/A') . "\n";
        echo "Count: " . ($decoded['count'] ?? 'N/A') . "\n";
        echo "Total Available: " . ($decoded['total_available'] ?? 'N/A') . "\n";
    } else {
        echo "JSON decode failed: " . json_last_error_msg() . "\n";
    }
}

