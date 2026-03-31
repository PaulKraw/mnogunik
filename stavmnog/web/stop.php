<?php
define('ACCESS_KEY', 'YOUR_SECRET_KEY');
define('STATUS_DIR', dirname(__FILE__) . '/status');

header('Content-Type: application/json');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo '{"error":"forbidden"}'; exit;
}

$client = preg_replace('/[^a-z0-9_]/i', '', $_GET['client'] ?? '');
$op     = preg_replace('/[^a-z0-9_]/i', '', $_GET['op']     ?? '');
if (!$client) { echo '{"error":"client required"}'; exit; }

$flags = [
    'download'     => "stop_download_{$client}.flag",
    'build_export' => "stop_build_{$client}.flag",
    'apply_bids'   => "stop_bids_{$client}.flag",
];

$created = [];
if ($op && isset($flags[$op])) {
    file_put_contents(STATUS_DIR . '/' . $flags[$op], date('Y-m-d H:i:s'));
    $created[] = $flags[$op];
} else {
    // без op — ставим все флаги
    foreach ($flags as $f) {
        file_put_contents(STATUS_DIR . '/' . $f, date('Y-m-d H:i:s'));
        $created[] = $f;
    }
}

echo json_encode(['status' => 'ok', 'client' => $client, 'flags' => $created]);