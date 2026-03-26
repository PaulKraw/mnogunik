<?php
if (($_GET['key'] ?? '') !== 'super123Lisa') {
    http_response_code(403); exit('Access denied');
}
$base_dir = '/var/www/mnogunik.ru/mnogunik';
$log_file = $base_dir . '/log.txt';
@file_put_contents($log_file, '');
header('Content-Type: application/json; charset=utf-8');
echo json_encode(['ok' => true, 'msg' => 'log cleared']);
