<?php
/**
 * status.php — возвращает JSON статуса операции
 * GET: op=download|build_stats|export|bids|build_export, client=evg
 */

define('ACCESS_KEY', 'YOUR_SECRET_KEY');
// define('STATUS_DIR', dirname(__FILE__) . '/status');


define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');
define('STAVMNOG_DIR', MONOREPO_ROOT . '/stavmnog');
define('STATUS_DIR',   STAVMNOG_DIR . '/web/status');


header('Content-Type: application/json');

$key = $_GET['key'] ?? '';
if ($key !== ACCESS_KEY) { http_response_code(403); echo '{}'; exit; }

$op     = $_GET['op']     ?? '';
$client = $_GET['client'] ?? '';
if (!$op || !$client) { echo '{}'; exit; }

// build_export — возвращаем статус export (он последний)
if ($op === 'build_export') {
    $bs_path = STATUS_DIR . "/build_stats_{$client}.json";
    $ex_path = STATUS_DIR . "/export_{$client}.json";
    $bs = file_exists($bs_path) ? json_decode(file_get_contents($bs_path), true) : [];
    $ex = file_exists($ex_path) ? json_decode(file_get_contents($ex_path), true) : [];

    // Берём самый свежий по started_at как основной
    $bs_ts = strtotime($bs['started_at'] ?? '1970-01-01');
    $ex_ts = strtotime($ex['started_at'] ?? '1970-01-01');
    $main = ($ex_ts >= $bs_ts && !empty($ex)) ? $ex : $bs;

    // Если build_stats упал — показываем его ошибку
    if (($bs['status'] ?? '') === 'error') {
        $main['status'] = 'error';
        $main['error'] = 'build_stats: ' . ($bs['error'] ?? 'unknown');
    }
    // Если оба done — считаем done по export
    echo json_encode($main, JSON_UNESCAPED_UNICODE);
    exit;
}

$path = STATUS_DIR . "/{$op}_{$client}.json";
if (!file_exists($path)) { echo '{}'; exit; }
echo file_get_contents($path) ?: '{}';