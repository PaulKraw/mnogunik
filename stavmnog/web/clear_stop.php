<?php
define('ACCESS_KEY', 'YOUR_SECRET_KEY');
// define('STATUS_DIR', dirname(__FILE__) . '/status');


define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');
define('STAVMNOG_DIR', MONOREPO_ROOT . '/stavmnog');
define('STATUS_DIR',   STAVMNOG_DIR . '/web/status');


header('Content-Type: application/json');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo '{"error":"forbidden"}'; exit;
}

$client = preg_replace('/[^a-z0-9_]/i', '', $_GET['client'] ?? '');
if (!$client) { echo '{"error":"client required"}'; exit; }

$removed = [];
$op_keys = ['download', 'build_stats', 'export', 'bids'];

foreach ($op_keys as $opk) {
    // стоп-флаги
    foreach (["stop_{$opk}_{$client}.flag", "stop_download_{$client}.flag",
              "stop_bids_{$client}.flag", "stop_build_{$client}.flag"] as $f) {
        $fp = STATUS_DIR . "/$f";
        if (file_exists($fp)) { unlink($fp); $removed[] = $f; }
    }
    // PID-файлы — удаляем если процесс мёртв
    $pf = STATUS_DIR . "/{$opk}_{$client}.pid";
    if (file_exists($pf)) {
        $pid   = (int)trim(file_get_contents($pf));
        $alive = $pid > 0 && file_exists("/proc/{$pid}");
        if (!$alive) {
            unlink($pf);
            $removed[] = "{$opk}_{$client}.pid (pid=$pid мёртв)";
            // сбрасываем статус
            $sf = STATUS_DIR . "/{$opk}_{$client}.json";
            if (file_exists($sf)) {
                $s = json_decode(file_get_contents($sf), true);
                if (($s['status'] ?? '') === 'running') {
                    $s['status'] = 'error'; $s['error'] = 'Сброшен вручную';
                    $s['finished_at'] = date('Y-m-d H:i:s');
                    file_put_contents($sf, json_encode($s, JSON_UNESCAPED_UNICODE|JSON_PRETTY_PRINT));
                    $removed[] = "reset {$opk}_{$client}.json";
                }
            }
        }
    }
}

echo json_encode(['status' => 'ok', 'client' => $client, 'removed' => array_unique($removed)], JSON_UNESCAPED_UNICODE);