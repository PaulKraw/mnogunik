<?php
/**
 * Установка Python-пакетов для работы с Google Sheets (gspread, google-auth и др.)
 * Запустить один раз: php install_python_packages.php
 * Или через браузер (но после использования удалить файл).
 */

// Отключаем ограничение времени выполнения
set_time_limit(300);

// Функция для безопасного вывода и выполнения команд
function runCommand($cmd, &$output = null) {
    echo "<pre style='margin:0;'>\$ $cmd\n</pre>";
    $returnVar = 0;
    exec($cmd . " 2>&1", $cmdOutput, $returnVar);
    $output = implode("\n", $cmdOutput);
    echo "<pre style='color:" . ($returnVar === 0 ? "green" : "red") . ";'>{$output}\n</pre>";
    return $returnVar === 0;
}

echo "<h2>Установка Python-пакетов для Google Sheets API</h2>";

// 1. Определяем путь к python3
// $python = trim(shell_exec("which python3"));
$python = '/var/www/mnogunik.ru/mng/.venv/bin/python3';
if (!$python || !file_exists($python)) {
    die("<p style='color:red;'>❌ python3 не найден в системе. Установите python3.</p>");
}
echo "<p>✅ Найден Python: $python</p>";

// 2. Проверяем наличие pip3
$pip = trim(shell_exec("which pip3"));
if (!$pip || !file_exists($pip)) {
    echo "<p>⚠️ pip3 не найден, пробуем python3 -m pip...</p>";
    $pip = $python . " -m pip";
}
echo "<p>🔧 Используем pip: $pip</p>";

// 3. Список необходимых пакетов
$packages = [
    'gspread',
    'google-auth',
    'google-auth-oauthlib',
    'google-auth-httplib2'
];

// 4. Проверяем, какие пакеты уже установлены
$missing = [];
foreach ($packages as $pkg) {
    $check = runCommand("$python -c \"import $pkg\" 2>/dev/null", $output);
    if (!$check) {
        $missing[] = $pkg;
        echo "<p style='color:orange;'>❌ Пакет $pkg НЕ установлен.</p>";
    } else {
        echo "<p style='color:green;'>✅ Пакет $pkg уже установлен.</p>";
    }
}

// 5. Если есть недостающие, устанавливаем их с --user (без прав root)
if (!empty($missing)) {
    echo "<h3>Устанавливаем недостающие пакеты:</h3>";
    $cmd = "$pip install --user " . implode(' ', $missing);
    if (runCommand($cmd, $output)) {
        echo "<p style='color:green;'>✅ Все пакеты успешно установлены.</p>";
    } else {
        echo "<p style='color:red;'>❌ Ошибка при установке. Возможно, нужны права root или установите pip.</p>";
        echo "<p>Альтернатива: выполните вручную на сервере:<br><code>sudo $pip install " . implode(' ', $missing) . "</code></p>";
    }
} else {
    echo "<p style='color:green;'>✅ Все необходимые пакеты уже установлены. Можете удалить этот скрипт.</p>";
}

// 6. Проверка финальной работоспособности
echo "<h3>Финальная проверка импорта:</h3>";
$testCmd = "$python -c \"import gspread; from google.oauth2.service_account import Credentials; print('Импорт успешен')\"";
runCommand($testCmd);

echo "<hr><p>Если всё зелёное, скрипт export_stats.py должен работать. Удалите этот файл после использования.</p>";
?>