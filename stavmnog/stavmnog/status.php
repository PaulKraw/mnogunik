<?php
/**
 * status.php — возвращает JSON статуса операции
 * GET: op=download|build_stats|export|bids|build_export, client=evg
 */

define('ACCESS_KEY', 'YOUR_SECRET_KEY');
define('STATUS_DIR', dirname(__FILE__) . '/status');

header('Content-Type: application/json');

$key = $_GET['key'] ?? '';
if ($key !== ACCESS_KEY) { http_response_code(403); echo '{}'; exit; }

$op     = $_GET['op']     ?? '';
$client = $_GET['client'] ?? '';
if (!$op || !$client) { echo '{}'; exit; }

// build_export — возвращаем статус export (он последний)
$op_file = ($op === 'build_export') ? 'export' : $op;

$path = STATUS_DIR . "/{$op_file}_{$client}.json";
if (!file_exists($path)) { echo '{}'; exit; }

$content = file_get_contents($path);
header('Cache-Control: no-store');
echo $content ?: '{}';