<?php
/**
 * kill.php — убивает процесс по PID-файлу
 * GET: client=evg&op=download|build_export|apply_bids
 */
define('ACCESS_KEY', 'YOUR_SECRET_KEY');
define('STATUS_DIR', dirname(__FILE__) . '/status');

header('Content-Type: application/json');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo '{"error":"forbidden"}'; exit;
}

$client = preg_replace('/[^a-z0-9_]/i', '', $_GET['client'] ?? '');
$op     = preg_replace('/[^a-z0-9_]/i', '', $_GET['op']     ?? '');
if (!$client || !$op) { echo '{"error":"params required"}'; exit; }

$op_key_map = [
    'download'     => 'download',
    'build_export' => 'build_stats',
    'apply_bids'   => 'bids',
];
$op_key  = $op_key_map[$op] ?? $op;
$pid_file = STATUS_DIR . "/{$op_key}_{$client}.pid";

if (!file_exists($pid_file)) {
    echo json_encode(['status' => 'no_pid', 'client' => $client, 'op' => $op]);
    exit;
}

$pid = (int)trim(file_get_contents($pid_file));
if ($pid <= 0) {
    unlink($pid_file);
    echo json_encode(['status' => 'bad_pid']); exit;
}

// убиваем процесс и всех детей
$killed = false;
if (file_exists("/proc/{$pid}")) {
    // сначала SIGTERM — мягкая остановка
    posix_kill($pid, SIGTERM);
    usleep(500000);
    // если ещё жив — SIGKILL
    if (file_exists("/proc/{$pid}")) {
        posix_kill($pid, SIGKILL);
    }
    $killed = true;
}

// убираем PID-файл
@unlink($pid_file);

// сбрасываем статус
$status_file = STATUS_DIR . "/{$op_key}_{$client}.json";
if (file_exists($status_file)) {
    $s = json_decode(file_get_contents($status_file), true);
    if (($s['status'] ?? '') === 'running') {
        $s['status']      = 'error';
        $s['error']       = 'Остановлено вручную';
        $s['finished_at'] = date('Y-m-d H:i:s');
        file_put_contents($status_file,
            json_encode($s, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
    }
}

echo json_encode([
    'status'  => $killed ? 'killed' : 'not_running',
    'pid'     => $pid,
    'client'  => $client,
    'op'      => $op,
]);