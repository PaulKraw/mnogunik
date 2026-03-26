<?php

if ($_GET['key'] !== 'super123Lisa') {
    http_response_code(403);
    exit('Access denied');
}

header('Content-Type: text/plain');
header('X-Accel-Buffering: no'); // важно для nginx
@ob_end_clean();
ini_set('output_buffering', 'off');
ini_set('zlib.output_compression', false);
ob_implicit_flush(true);

$command = "/opt/python/python-3.8.8/bin/python3 /var/www/u1168406/data/www/paulkraw.ru/mnogunik/go.py";
$process = popen($command . " 2>&1", 'r');

putenv("OPENBLAS_NUM_THREADS=1");
putenv("OMP_NUM_THREADS=1");


if (is_resource($process)) {
    while (!feof($process)) {
        $line = fgets($process);
        echo $line;
        flush(); // отправляем в браузер
    }
    pclose($process);
}
?>
