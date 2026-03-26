<?php
// status.php
header('Content-Type: application/json; charset=utf-8');

$PROJECT_ROOT = realpath(__DIR__ . '/..') . '/src/pricecraft';
$RUNNERS_DIR = $PROJECT_ROOT . '/runners';
$STATUS_FILE = $RUNNERS_DIR . '/status.json';
$PID_FILE = $PROJECT_ROOT . '/py.pid';

$result = [
    'status' => 'idle',
    'message' => '',
    'module' => null,
    'pid' => null,
    'running' => false,
    'timestamp' => date('c')
];

if (file_exists($STATUS_FILE)) {
    $json = @file_get_contents($STATUS_FILE);
    $data = @json_decode($json, true);
    if ($data) {
        $result = array_merge($result, $data);
    }
}

// check pid
if (file_exists($PID_FILE)) {
    $pid = trim(file_get_contents($PID_FILE));
    $result['pid'] = $pid;
    if (is_numeric($pid)) {
        // ps check
        $out = trim(shell_exec("ps -p " . intval($pid) . " -o pid= 2>/dev/null"));
        if ($out !== '') $result['running'] = true;
    }
}

echo json_encode($result, JSON_UNESCAPED_UNICODE|JSON_PRETTY_PRINT);
