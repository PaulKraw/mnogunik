<?php
$python = '/var/www/mnogunik.ru/mng/.venv/bin/python';
exec("$python --version 2>&1", $out, $code);
echo "Код возврата: $code\n";
echo "Вывод: " . implode("\n", $out);