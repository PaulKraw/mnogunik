<?php
$python = '/var/www/mnogunik.ru/mng/.venv/bin/python';
exec("$python -c 'import pandas; print(pandas.__version__)' 2>&1", $out, $code);
echo "Код: $code\n";
echo implode("\n", $out);