<?php
/**
 * set_setting.php — сохраняет/читает настройки панели
 * POST: name=rewrite_last_days&value=3
 * GET:  get=rewrite_last_days
 */
define('ACCESS_KEY', 'YOUR_SECRET_KEY');
// define('STATUS_DIR', dirname(__FILE__) . '/status');

define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');
define('STAVMNOG_DIR', MONOREPO_ROOT . '/stavmnog');
define('STATUS_DIR',   STAVMNOG_DIR . '/web/status');






header('Content-Type: application/json');

if (($_GET['key'] ?? '') !== ACCESS_KEY) {
    http_response_code(403); echo '{"error":"forbidden"}'; exit;
}

if (!is_dir(STATUS_DIR)) mkdir(STATUS_DIR, 0755, true);

// GET — читаем настройку
if (isset($_GET['get'])) {
    $name = preg_replace('/[^a-z0-9_]/i', '', $_GET['get']);
    $file = STATUS_DIR . "/setting_{$name}.txt";
    $val  = file_exists($file) ? trim(file_get_contents($file)) : '';
    echo json_encode(['name' => $name, 'value' => $val]);
    exit;
}

// POST — сохраняем
$name  = preg_replace('/[^a-z0-9_]/i', '', $_POST['name']  ?? '');
$value = trim($_POST['value'] ?? '');

if (!$name) { echo '{"error":"name required"}'; exit; }

// валидация по имени
if ($name === 'rewrite_last_days') {
    $value = max(1, min(30, (int)$value));
}

file_put_contents(STATUS_DIR . "/setting_{$name}.txt", $value);
echo json_encode(['status' => 'saved', 'name' => $name, 'value' => $value]);