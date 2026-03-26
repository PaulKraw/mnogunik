<?php
// clear_log.php
$logFile = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft/runners/log.txt';

try {
    if (!file_exists($logFile)) {
        echo "Файл не найден: $logFile";
        exit;
    }

    // Очистка файла
    file_put_contents($logFile, "");
    echo "Лог очищен успешно.";
} catch (Exception $e) {
    echo "Ошибка: " . $e->getMessage();
}
