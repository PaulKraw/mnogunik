<?php
/**
 * Исправление прав и установка пакетов в .venv
 * Запустить один раз, затем удалить.
 */

header('Content-Type: text/plain; charset=utf-8');
echo "=== Исправление прав и установка пакетов в .venv ===\n\n";

$venv_python = '/var/www/mnogunik.ru/mng/.venv/bin/python3';
$venv_dir = dirname(dirname($venv_python)); // /var/www/mnogunik.ru/mng/.venv
$monorepo_root = '/var/www/mnogunik.ru/mng';

// 1. Проверяем существование python3 в venv
if (!file_exists($venv_python)) {
    die("❌ $venv_python не найден. Проверьте путь.\n");
}
echo "✅ Найден Python: $venv_python\n\n";

// 2. Проверяем, можем ли мы его выполнить
$test = shell_exec($venv_python . ' -c "print(123)" 2>&1');
if (trim($test) === '123') {
    echo "✅ Python в venv уже выполним.\n";
} else {
    echo "⚠️ Python в venv НЕ выполним текущим пользователем.\n";
    echo "Ошибка: " . ($test ?: "Permission denied или другой сбой") . "\n";
    
    // Пытаемся дать права (если мы владелец или root)
    $current_user = exec('whoami');
    $venv_owner = fileowner($venv_dir);
    $owner_name = function_exists('posix_getpwuid') ? posix_getpwuid($venv_owner)['name'] : 'неизвестно';
    
    echo "Текущий пользователь PHP: $current_user\n";
    echo "Владелец папки .venv: $owner_name (UID: $venv_owner)\n";
    
    if ($current_user === $owner_name || $current_user === 'root') {
        echo "Пробуем дать права на выполнение...\n";
        exec("chmod -R u+rx $venv_dir/bin", $out, $ret);
        if ($ret === 0) {
            echo "✅ Права установлены.\n";
        } else {
            echo "❌ Не удалось изменить права. Код ошибки: $ret\n";
        }
    } else {
        echo "❌ Вы не владелец и не root. Исправьте права вручную:\n";
        echo "sudo chown -R www-data:www-data $venv_dir\n";
        echo "или\n";
        echo "sudo chmod -R a+rx $venv_dir/bin\n";
        echo "sudo chmod -R a+rX $vend_dir/lib\n";
        exit;
    }
}

// 3. Проверяем наличие pip
$pip = $venv_python . ' -m pip';
$pip_test = shell_exec("$pip --version 2>&1");
if (strpos($pip_test, 'pip') === false) {
    echo "❌ pip не работает в venv. Возможно, повреждено окружение.\n";
    echo "Создайте venv заново:\n";
    echo "rm -rf $venv_dir\n";
    echo "python3 -m venv $venv_dir\n";
    exit;
}
echo "✅ pip работает: " . trim($pip_test) . "\n\n";

// 4. Устанавливаем пакеты
$packages = ['gspread', 'google-auth', 'google-auth-oauthlib', 'google-auth-httplib2'];
echo "Устанавливаем пакеты: " . implode(', ', $packages) . "\n";

$install_cmd = "$pip install " . implode(' ', $packages);
echo "Выполняется: $install_cmd\n";
exec($install_cmd . " 2>&1", $output, $ret);
echo implode("\n", $output) . "\n";

if ($ret === 0) {
    echo "\n✅ Пакеты успешно установлены.\n";
} else {
    echo "\n❌ Ошибка установки. Попробуйте вручную:\n";
    echo "cd $monorepo_root\n";
    echo "source .venv/bin/activate\n";
    echo "pip install " . implode(' ', $packages) . "\n";
    exit;
}

// 5. Финальная проверка
echo "\n=== Проверка импорта ===\n";
$test_cmd = "$venv_python -c \"import gspread; from google.oauth2.service_account import Credentials; print('Импорт успешен')\"";
exec($test_cmd . " 2>&1", $test_out, $test_ret);
echo implode("\n", $test_out) . "\n";
if ($test_ret === 0) {
    echo "\n✅ Всё готово! Можете удалить этот скрипт.\n";
} else {
    echo "\n❌ Импорт не удался. Проверьте установку вручную.\n";
}