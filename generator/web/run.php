<?php
if (($_GET['key'] ?? '') !== 'super123Lisa') {  // не меняю имён, лишь защищаю от Notice
    http_response_code(403);
    exit('Access denied');
}

//$base_dir   = '/var/www/mnogunik.ru/mnogunik';
$base_dir = '/var/www/mnogunik.ru/mng'; 
$go_py      = $base_dir . '/generator/go.py';


$log_file   = $base_dir . '/log.txt';
$start_ix   = $base_dir . '/start_index.txt';
$stop_flag  = $base_dir . '/stop.flag';

// БЫЛО: if (file_exists($stop_file)) unlink($stop_file);
if (file_exists($stop_flag)) unlink($stop_flag);  // <-- фикс: та же переменная

$python = $base_dir . '/.venv/bin/python';

@touch($log_file);
@touch($start_ix);

$startIndex = isset($_POST['startIndex']) ? trim($_POST['startIndex']) : '';
if ($startIndex === '' || !ctype_digit($startIndex)) $startIndex = '0';
file_put_contents($start_ix, $startIndex."\n");

// дубль на всякий случай, но переменная та же
if (file_exists($stop_flag)) @unlink($stop_flag);

file_put_contents($log_file, "=== START ".date('Y-m-d H:i:s')." (startIndex=$startIndex) ===\n", FILE_APPEND);

// $cmd = sprintf(
//   'bash -lc \'cd %s && nohup %s -u %s >> %s 2>&1 & echo $! > py.pid\'',
//   escapeshellarg($base_dir),
//   escapeshellarg($python),
//   escapeshellarg($go_py),
//   escapeshellarg($log_file)
// );

$cmd = sprintf(
  'bash -lc \'cd %s && if [ -f py.pid ] && kill -0 $(cat py.pid) 2>/dev/null; then echo "already running"; else nohup %s -u %s >> %s 2>&1 & echo $! > py.pid; fi\'',
  escapeshellarg($base_dir),
  escapeshellarg($python),
  escapeshellarg($go_py),
  escapeshellarg($log_file)
);

shell_exec($cmd);
echo "Скрипт запущен. См. лог.";



// $commands = [
//     'which python',
//     'which python3',
//     'python --version',
//     'python3 --version',
//     'whoami',
//     'echo $PATH',
//     'python3 -m pip list'
// ];


// echo "<pre>";

// foreach ($commands as $cmd) {
//     echo "👉 <b>Команда:</b> $cmd\n";
//     echo shell_exec($cmd . ' 2>&1') . "\n";
//     echo str_repeat('-', 40) . "\n";
// }

// echo "</pre>";


// $python_path = '/opt/python/python-3.8.8/bin/python3';
// $script_path = __DIR__ . '/go.py';  // путь к go.py в той же папке

// file_put_contents("log.txt", "");

// Проверим правильность пути
// echo "Python path: $python_path\n";
// echo "Script path: $script_path\n";

// Команда: python3 + stderr в stdout
// $command = $python_path . ' ' . escapeshellarg($script_path) . ' > /dev/null 2>&1 &';

// putenv("OPENBLAS_NUM_THREADS=1");
// putenv("OMP_NUM_THREADS=1");

// Запуск
// shell_exec($command);
// Запускаем в фоне
// shell_exec("$python_path $script_path 2>&1 ");
// $output = shell_exec($command);
// echo "Скрипт запущен";



// Вывод
// echo "<pre>$output</pre>";


// // $log_file = '/var/www/u1168406/data/www/paulkraw.ru/mnogunik/log.txt';
// $cmd        = "cd " . escapeshellarg($base_dir) . " && nohup " . escapeshellcmd($python) . " " . escapeshellarg($script) . " >> " . escapeshellarg($log_file) . " 2>&1 &";

// echo $cmd;

// if (file_exists($log_file)) {
//     file_put_contents($log_file, "хуйня кобылы"); // очистим
// }

// if ($_SERVER['REQUEST_METHOD'] === 'POST') {
//     $val = isset($_POST['start_index']) ? trim($_POST['start_index']) : '';

//     if ($val !== '' && !preg_match('/^\d+$/', $val)) {
//         $val = '';
//     }

//     // записываем стартовый индекс
//     file_put_contents($start_file, $val);

//     // чистим лог
//     file_put_contents($log_file, "");

//     // ограничения потоков
//     putenv("OPENBLAS_NUM_THREADS=1");
//     putenv("OMP_NUM_THREADS=1");


//      // Диагностика старта
//     $hdr  = "=== RUN START ===\n";
//     $hdr .= "time: " . date('c') . "\n";
//     $hdr .= "whoami: " . trim(shell_exec('whoami')) . "\n";
//     $hdr .= "pwd: " . getcwd() . "\n";
//     $hdr .= "cmd: " . $cmd . "\n";
//     file_put_contents($log_file, $hdr, FILE_APPEND);
    
//     $out = [];
//     $ret = 0;
//     exec($cmd, $out, $ret);
//     file_put_contents($log_file, "exec ret=$ret\n", FILE_APPEND);


//     // запуск в фоне
//     // $cmd = 'nohup ' . escapeshellcmd($python) . ' ' . escapeshellarg($script) . ' > ' . escapeshellarg($log_file) . ' 2>&1 &';
//     // exec($cmd);

//     echo 'Скрипт запущен. Стартовый индекс: ' . ($val === '' ? '0' : $val);
//     exit;
// } else {
//     echo 'POST only';
// }

// echo "Используйте POST-запрос для запуска.";