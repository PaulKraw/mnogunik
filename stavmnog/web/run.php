<?php

define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');
define('STAVMNOG_DIR', MONOREPO_ROOT . '/stavmnog');

$python = MONOREPO_ROOT . '/.venv/bin/python';
$clients = json_decode(file_get_contents(MONOREPO_ROOT . '/db/config/clients.json'), true) ?? [];
define('ACCESS_KEY', 'YOUR_SECRET_KEY');

$status_dir = STAVMNOG_DIR . '/web/status';
$log_dir    = STAVMNOG_DIR . '/web/logs';

header('Content-Type: application/json');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo json_encode(['error'=>'forbidden']); exit;
}

$op     = $_POST['op']     ?? '';
$client = preg_replace('/[^a-z0-9_]/i', '', $_POST['client'] ?? '');

if (!$op || !$client) {
    http_response_code(400); echo json_encode(['error'=>'op and client required']); exit;
}
if (!array_key_exists($client, $clients)) {
    http_response_code(400); echo json_encode(['error'=>'unknown client']); exit;
}

foreach ([$status_dir, $log_dir] as $dir) {
    if (!is_dir($dir)) mkdir($dir, 0755, true);
}

function check_and_clean(string $sd, string $op_key, string $client): bool {
    $pf = $sd . "/{$op_key}_{$client}.pid";
    $sf = $sd . "/{$op_key}_{$client}.json";
    if (!file_exists($pf)) return false;
    $pid = (int)trim(file_get_contents($pf));
    $alive = $pid > 0 && file_exists("/proc/{$pid}");
    if ($alive) return true;
    unlink($pf);
    if (file_exists($sf)) {
        $s = json_decode(file_get_contents($sf), true);
        if (($s['status'] ?? '') === 'running') {
            $s['status'] = 'error';
            $s['error'] = 'Процесс завершился неожиданно';
            $s['finished_at'] = date('Y-m-d H:i:s');
            file_put_contents($sf, json_encode($s, JSON_UNESCAPED_UNICODE|JSON_PRETTY_PRINT));
        }
    }
    return false;
}

switch ($op) {
    case 'download':
        $op_key = 'download';
        $script = MONOREPO_ROOT . '/scripts/download.py';
        $rw = (int)($_POST['rewrite_days'] ?? 0);
        if ($rw < 1) {
            $sf = $status_dir . '/setting_rewrite_last_days.txt';
            $rw = file_exists($sf) ? max(1, (int)trim(file_get_contents($sf))) : 1;
        }
        $args = "--client=" . escapeshellarg($client) . " --rewrite=" . (int)$rw;
        $logfile_abs = $log_dir . "/download_{$client}.log";
        $pidfile_abs = $status_dir . "/download_{$client}.pid";
        @unlink($status_dir . "/stop_download_{$client}.flag");
        break;

    case 'apply_bids':
        $op_key = 'bids';
        $script = MONOREPO_ROOT . '/scripts/apply_bids.py';
        $args = "--client=" . escapeshellarg($client);
        $logfile_abs = $log_dir . "/bids_{$client}.log";
        $pidfile_abs = $status_dir . "/bids_{$client}.pid";
        @unlink($status_dir . "/stop_bids_{$client}.flag");
        break;

    case 'build_export':
        $op_key = 'build_stats';
        $script1 = MONOREPO_ROOT . '/scripts/build_stats.py';
        $script2 = MONOREPO_ROOT . '/scripts/export_stats.py';
        $logfile_abs = $log_dir . "/build_export_{$client}.log";
        $pidfile_abs = $status_dir . "/build_stats_{$client}.pid";
        break;

    default:
        echo json_encode(['error'=>'unknown op']); exit;
}

if (check_and_clean($status_dir, $op_key, $client)) {
    echo json_encode(['status'=>'already_running','op'=>$op,'client'=>$client]); exit;
}

if ($op === 'build_export') {
    $cmd = sprintf(
        'cd %s && PYTHONPATH=%s nohup %s -u %s --client=%s && PYTHONPATH=%s nohup %s -u %s --client=%s >> %s 2>&1 & echo $! > %s',
        escapeshellarg(MONOREPO_ROOT),
        escapeshellarg(MONOREPO_ROOT),
        escapeshellarg($python), escapeshellarg($script1), escapeshellarg($client),
        escapeshellarg(MONOREPO_ROOT),
        escapeshellarg($python), escapeshellarg($script2), escapeshellarg($client),
        escapeshellarg($logfile_abs), escapeshellarg($pidfile_abs)
    );
} else {
    $cmd = sprintf(
        'cd %s && PYTHONPATH=%s nohup %s -u %s %s >> %s 2>&1 & echo $! > %s',
        escapeshellarg(MONOREPO_ROOT),
        escapeshellarg(MONOREPO_ROOT),
        escapeshellarg($python),
        escapeshellarg($script),
        $args,
        escapeshellarg($logfile_abs),
        escapeshellarg($pidfile_abs)
    );
}

shell_exec($cmd);
usleep(500000);

$pid = 0;
if (file_exists($pidfile_abs)) {
    $pid = (int)trim(file_get_contents($pidfile_abs));
}

echo json_encode([
    'status' => 'started',
    'op' => $op,
    'client' => $client,
    'pid' => $pid
]);