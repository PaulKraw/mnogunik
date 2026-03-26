<?php
header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-cache');
$status_file = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft/runners/buttons_status.json';

echo file_get_contents($status_file);