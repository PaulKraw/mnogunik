<?php


if (($_GET['key'] ?? '') !== 'super123Lisa') { http_response_code(403); exit('Access denied'); }
// file_put_contents(__DIR__ . '/stop.flag', '1');

$base_dir  = __DIR__;
$stop_flag = $base_dir . '/stop.flag';
$start_ix  = $base_dir . '/start_index.txt';

@file_put_contents($stop_flag, "1\n");
@file_put_contents($start_ix, "0\n");
echo "OK";

