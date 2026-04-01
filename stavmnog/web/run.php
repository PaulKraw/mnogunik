<?php
define('ACCESS_KEY', 'YOUR_SECRET_KEY');

//$base_dir   = __DIR__;
//$python     = $base_dir . '/.venv/bin/python';

$MONOREPO_ROOT = '/var/www/mnogunik.ru/mng';
$base_dir = $MONOREPO_ROOT;
$python = $MONOREPO_ROOT . '/.venv/bin/python';

$status_dir = $base_dir . '/status';
$log_dir    = $base_dir . '/logs';

header('Content-Type: application/json');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo json_encode(['error'=>'forbidden']); exit;
}

$op     = $_POST['op']     ?? '';
$client = preg_replace('/[^a-z0-9_]/i', '', $_POST['client'] ?? '');

if (!$op || !$client) {
    http_response_code(400); echo json_encode(['error'=>'op and client required']); exit;
}

$clients = json_decode(file_get_contents($base_dir . '/config/clients.json'), true) ?? [];
if (!array_key_exists($client, $clients)) {
    http_response_code(400); echo json_encode(['error'=>'unknown client']); exit;
}

foreach ([$status_dir, $log_dir] as $dir) {
    if (!is_dir($dir)) mkdir($dir, 0755, true);
}

// --- проверка живого процесса + автоочистка мёртвых ---
function check_and_clean(string $sd, string $op_key, string $client): bool {
    $pf = $sd . "/{$op_key}_{$client}.pid";
    $sf = $sd . "/{$op_key}_{$client}.json";
    if (!file_exists($pf)) return false;
    $pid   = (int)trim(file_get_contents($pf));
    $alive = $pid > 0 && file_exists("/proc/{$pid}");
    if ($alive) return true;
    // мёртв — чистим
    unlink($pf);
    if (file_exists($sf)) {
        $s = json_decode(file_get_contents($sf), true);
        if (($s['status'] ?? '') === 'running') {
            $s['status']      = 'error';
            $s['error']       = 'Процесс завершился неожиданно';
            $s['finished_at'] = date('Y-m-d H:i:s');
            file_put_contents($sf, json_encode($s, JSON_UNESCAPED_UNICODE|JSON_PRETTY_PRINT));
        }
    }
    return false;
}

switch ($op) {
    case 'download':
        $op_key  = 'download';
        $script  = 'scripts/download.py';
        // rewrite_days из POST или из файла настроек
        $rw = (int)($_POST['rewrite_days'] ?? 0);
        if ($rw < 1) {
            $sf = $status_dir . '/setting_rewrite_last_days.txt';
            $rw = file_exists($sf) ? max(1,(int)trim(file_get_contents($sf))) : 1;
        }
        $args    = "--client={$client} --rewrite={$rw}";
        $logfile = "logs/download_{$client}.log";
        $pidfile = "status/download_{$client}.pid";
        @unlink($status_dir . "/stop_download_{$client}.flag");
        break;

    case 'build_export':
        $op_key  = 'build_stats';
        $script  = null;
        $logfile = "logs/build_export_{$client}.log";
        $pidfile = "status/build_stats_{$client}.pid";
        break;

    case 'apply_bids':
        $op_key  = 'bids';
        $script  = 'scripts/apply_bids.py';
        $args    = "--client={$client}";
        $logfile = "logs/bids_{$client}.log";
        $pidfile = "status/bids_{$client}.pid";
        @unlink($status_dir . "/stop_bids_{$client}.flag");
        break;

    default:
        echo json_encode(['error'=>'unknown op']); exit;
}

if (check_and_clean($status_dir, $op_key, $client)) {
    echo json_encode(['status'=>'already_running','op'=>$op,'client'=>$client]); exit;
}

// --- запуск ---
if ($op === 'build_export') {
    $cmd = sprintf(
        'bash -lc \'cd %s && nohup %s -u scripts/build_stats.py --client=%s && %s -u scripts/export_stats.py --client=%s >> %s 2>&1 & echo $! > %s\'',
        escapeshellarg($base_dir),
        escapeshellarg($python), escapeshellarg($client),
        escapeshellarg($python), escapeshellarg($client),
        escapeshellarg($logfile), escapeshellarg($pidfile)
    );
} else {
    $cmd = sprintf(
        'bash -lc \'cd %s && nohup %s -u %s %s >> %s 2>&1 & echo $! > %s\'',
        escapeshellarg($base_dir),
        escapeshellarg($python),
        escapeshellarg($script),
        $args,
        escapeshellarg($logfile),
        escapeshellarg($pidfile)
    );
}

shell_exec($cmd);
usleep(500000);

$pid = 0;
$pf  = $base_dir . '/' . $pidfile;
if (file_exists($pf)) $pid = (int)trim(file_get_contents($pf));

echo json_encode(['status'=>'started','op'=>$op,'client'=>$client,'pid'=>$pid]);