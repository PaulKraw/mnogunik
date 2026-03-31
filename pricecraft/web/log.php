<?php
// log.php
header('Content-Type: text/plain; charset=utf-8');
$PROJECT_ROOT = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft';
// $PROJECT_ROOT = realpath(__DIR__ . '/..') . '/src/pricecraft';
$LOG_FILE = $PROJECT_ROOT . '/runners/log.txt';

if (!file_exists($LOG_FILE)) {
    echo "Log file not found";
    exit;
}

echo file_get_contents($LOG_FILE);
