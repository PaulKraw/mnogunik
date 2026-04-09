<?php
if (($_GET['key'] ?? '') !== 'super123Lisa') {  // не меняю имён, лишь защищаю от Notice
    echo "Скрипт НЕ запущен. См. лог.";
    http_response_code(403);
    exit('Access denied');
}
echo "Скрипт запускается. См. лог.";

//$base_dir   = '/var/www/mnogunik.ru/mnogunik';
$base_dir = '/var/www/mnogunik.ru/mng'; 
$python = $base_dir . '/.venv/bin/python';

$go_py      = $base_dir . '/generator/go.py';

$base_dir_gen = '/var/www/mnogunik.ru/mng/generator'; 

$log_file   = $base_dir_gen . '/log.txt';
// if (file_exists($log_file)) {
//     header('Content-Type: text/plain');
//     echo file_get_contents($log_file);
// } else {
//     echo "Лог-файл не найден";
// }

$start_ix   = $base_dir_gen . '/start_index.txt';
if (file_exists($start_ix)) {
    header('Content-Type: text/plain');
    echo file_get_contents($start_ix);
} else {
    echo "start_ix не найден";
}
$stop_flag  = $base_dir_gen . '/stop.flag';




if (file_exists($stop_flag)) unlink($stop_flag);

$startIndex = isset($_POST['startIndex']) ? trim($_POST['startIndex']) : '';
if ($startIndex === '' || !ctype_digit($startIndex)) $startIndex = '0';
// file_put_contents($start_ix, $startIndex . "\n");
file_put_contents($log_file, ''); // или unlink($log_file); потом touch
unlink($log_file);
@touch($log_file);

file_put_contents($log_file, "=== START " . date('Y-m-d H:i:s') . " (startIndex=$startIndex) ===\n", FILE_APPEND);

// Главное изменение: добавляем PYTHONPATH
// $cmd = "cd $base_dir && PYTHONPATH=$base_dir nohup $python -u $go_py >> $log_file 2>&1 & echo $! > py.pid";
$cmd = "cd $base_dir && PYTHONPATH=$base_dir nohup $python $go_py >> $log_file 2>&1 & echo $! > py.pid";
// $cmd = sprintf(
//   'bash -lc \'cd %s && if [ -f py.pid ] && kill -0 $(cat py.pid) 2>/dev/null; then echo "already running"; else nohup %s -u %s >> %s 2>&1 & echo $! > py.pid; fi\'',
//   escapeshellarg($base_dir),
//   escapeshellarg($python),
//   escapeshellarg($go_py),
//   escapeshellarg($log_file)
// );
shell_exec($cmd);
echo "Скрипт запущен. См. лог.";

// if (file_exists($stop_flag)) unlink($stop_flag);  // <-- фикс: та же переменная

// $python = $base_dir . '/.venv/bin/python';

// @touch($log_file);
// @touch($start_ix);

// $startIndex = isset($_POST['startIndex']) ? trim($_POST['startIndex']) : '';
// if ($startIndex === '' || !ctype_digit($startIndex)) $startIndex = '0';
// file_put_contents($start_ix, $startIndex."\n");

// // дубль на всякий случай, но переменная та же
// if (file_exists($stop_flag)) @unlink($stop_flag);

// file_put_contents($log_file, "=== START ".date('Y-m-d H:i:s')." (startIndex=$startIndex) ===\n", FILE_APPEND);



// $cmd = sprintf(
//   'bash -lc \'cd %s && if [ -f py.pid ] && kill -0 $(cat py.pid) 2>/dev/null; then echo "already running"; else nohup %s -u %s >> %s 2>&1 & echo $! > py.pid; fi\'',
//   escapeshellarg($base_dir),
//   escapeshellarg($python),
//   escapeshellarg($go_py),
//   escapeshellarg($log_file)
// );

// shell_exec($cmd);
// echo "Скрипт запущен. См. лог.";

