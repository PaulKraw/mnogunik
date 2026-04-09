<?php
/**
 * log.php — возвращает последние N строк из лог-файла
 * GET: client=evg&op=download&lines=40
 */

define('ACCESS_KEY', 'YOUR_SECRET_KEY');
// define('LOG_DIR',    dirname(__FILE__) . '/logs');

define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');
define('STAVMNOG_DIR', MONOREPO_ROOT . '/web/stavmnog');
define('LOG_DIR',      STAVMNOG_DIR . '/web/logs');


header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo '[]'; exit;
}

$client = preg_replace('/[^a-z0-9_]/i', '', $_GET['client'] ?? '');
$op     = preg_replace('/[^a-z0-9_]/i', '', $_GET['op']     ?? '');
$lines  = min(200, max(5, (int)($_GET['lines'] ?? 60)));

if (!$client || !$op) { echo '[]'; exit; }

// op → имя файла лога
$map = [
    'download'     => "download_{$client}.log",
    'build_export' => "build_export_{$client}.log",
    'build_stats'  => "build_stats_{$client}.log",
    'export'       => "export_{$client}.log",
    'apply_bids'   => "bids_{$client}.log",
];
$filename = $map[$op] ?? null;
if (!$filename) { echo '[]'; exit; }

$path = LOG_DIR . '/' . $filename;
if (!file_exists($path)) { echo '[]'; exit; }

// читаем последние $lines строк эффективно
$file = new SplFileObject($path, 'r');
$file->seek(PHP_INT_MAX);
$total = $file->key();

$start  = max(0, $total - $lines);
$result = [];
$file->seek($start);
while (!$file->eof()) {
    $line = rtrim($file->fgets());
    if ($line !== '') {
        $result[] = $line;
    }
}

echo json_encode($result, JSON_UNESCAPED_UNICODE);