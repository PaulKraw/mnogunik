<?php


// $log_file = __DIR__ . '/log.txt'; когда все было в одной папке


$base_dir_gen = '/var/www/mnogunik.ru/mng/generator'; 

$log_file =$base_dir_gen . '/log.txt';

header('Content-Type: text/plain; charset=utf-8');
echo file_exists($log_file) ? file_get_contents($log_file) : "Лог пуст или ещё не создан.";




// $log_file = '/var/www/u1168406/data/www/paulkraw.ru/mnogunik/log.txt';

// if (file_exists($log_file)) {
//     echo file_get_contents($log_file);
// } else {
//     echo 'Лог не найден';
// }
