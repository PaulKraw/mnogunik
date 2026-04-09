<?php

// // Проверка ключа, если нужна (оставь или убери):
// if (($_GET['key'] ?? '') !== 'super123Lisa') { 
//     http_response_code(403); 
//     exit('Access denied'); 
// }



$base_dir_gen = '/var/www/mnogunik.ru/mng/generator'; 



$base_dir = __DIR__;
$flag_file = $base_dir_gen . '/genimg.flag';

$set = $_GET['set'] ?? '0';
// file_put_contents($flag_file, "1\n");
// file_put_contents($flag_file, "1\n");   
// Если set=1 — создаём флаг
if ($set === '1') {
    file_put_contents($flag_file, "1\n");
    exit("FLAG ON");
}

// Если set=0 — удаляем флаг
if (file_exists($flag_file)) {
    unlink($flag_file);
    echo "FLAG OFF";
}


