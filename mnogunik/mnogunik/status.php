<?php
if (($_GET['key'] ?? '') !== 'super123Lisa') {
    http_response_code(403); exit('Access denied');
}

$base_dir  = '/var/www/mnogunik.ru/mnogunik';
$log_file  = $base_dir . '/log.txt';
$pattern   = '/var/www/mnogunik.ru/mnogunik/go.py';

$cmd  = 'pgrep -fa -u www-data ' . escapeshellarg($pattern) . ' 2>/dev/null';
$out  = shell_exec($cmd) ?? '';
$rows = array_values(array_filter(array_map('trim', explode("\n", $out))));

$pids = [];
foreach ($rows as $r) {
    $parts = explode(' ', $r, 2);
    if (isset($parts[0]) && ctype_digit($parts[0])) $pids[] = (int)$parts[0];
}

header('Content-Type: application/json; charset=utf-8');
echo json_encode([
    'count' => count($pids),
    'pids'  => $pids,
    'time'  => date('c'),
], JSON_UNESCAPED_UNICODE);
