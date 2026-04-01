<?php
// action.php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
try {
    
// удаление стоп флага перед запуском какого либо дейсвтия. возможны баги, нужна проверка. для удаления стоп флага есть кнопка на фронте
    // Удаляем стоп-флаг при запуске любого действия
    // $stop_flag = $PROJECT_ROOT . '/stop.flag';
    // if (file_exists($stop_flag)) {
    //     @unlink($stop_flag);
    // }


    // echo "start";
    header('Content-Type: application/json; charset=utf-8');

    // Настройки — подправь пути под свое размещение
    // $PROJECT_ROOT = realpath(__DIR__ . '/..') . '/src/pricecraft'; // если фронт в WebData, поправь путь
    // Пример альтернативы: $PROJECT_ROOT = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft';
   // $PROJECT_ROOT = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft';

$MONOREPO_ROOT = '/var/www/mnogunik.ru/mng';
$PROJECT_ROOT = $MONOREPO_ROOT . '/pricecraft';
$PYTHON = $MONOREPO_ROOT . '/.venv/bin/python';


    $CONFIG_DIR = $PROJECT_ROOT . '/config';
    $RUNNERS_DIR = $PROJECT_ROOT . '/runners';
    $LOG_FILE = $RUNNERS_DIR . '/log.txt';
    $PID_FILE = $PROJECT_ROOT . '/py.pid';
    $STOP_FLAG = $PROJECT_ROOT . '/stop.flag';

    // читаем POST
    // $pwd = $_POST['password'] ?? '';
    $pwd = 'genMnogunik3472';

    
    // $action = $_POST['action'] ?? '';
    $action = $_POST['action'] ?? '';
    if (!$action) {
        echo json_encode(['ok' => false, 'error' => 'no action']);
        exit;
    }
    $status_file = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft/runners/buttons_status.json';

    $data = json_decode(file_get_contents($status_file), true);
    if (!isset($data[$action])) {
        echo json_encode(['ok' => false, 'error' => 'unknown action']);
        exit;
    }


    $data[$action]['status'] = 'generating';
    $data[$action]['generated_at'] = null;
    
    file_put_contents(
        $status_file,
        json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT)
    );
    // echo json_encode(['ok' => true]);

    // echo "action " . $action;
    // echo "pwd " . $pwd;

    if (!$pwd || !$action) {
        http_response_code(400);
        echo json_encode(['ok'=>false,'error'=>'missing_password_or_action']);
        exit;
    }

    // --- проверка пароля (hash хранится в config/password-hash.txt) ---
    $hash_file = $CONFIG_DIR . '/password_hash.txt';
    if (!file_exists($hash_file)) {
        http_response_code(500);
        echo json_encode(['ok'=>false,'error'=>'no_password_hash']);
        exit;
    }
    $hash = trim(file_get_contents($hash_file));
    if (!password_verify($pwd, $hash)) {
        http_response_code(403);
        echo json_encode(['ok'=>false,'error'=>'bad_password']);
        exit;
    }



    // --- проверяем allow list ---
    $allowed_file = $CONFIG_DIR . '/allowed_modules.json';
    
    if (!file_exists($allowed_file)) {
        http_response_code(500);
        echo json_encode(['ok'=>false,'error'=>'no_allowed_modules']);
        exit;
    }

    $allowed = json_decode(file_get_contents($allowed_file), true);
    // echo " allowed" . $allowed . " ";
    if (!is_array($allowed) || !in_array($action, $allowed)) {
        http_response_code(403);
        echo json_encode(['ok'=>false,'error'=>'module_not_allowed']);
        exit;
    }

    // удаляем стоп-флаг, если есть (каждый запуск очищает стоп-флаг)
    if (file_exists($STOP_FLAG)) @unlink($STOP_FLAG);

    // путь к python - поправь если используешь другое расположение venv
    $PYTHON = $PROJECT_ROOT . '/.venv/bin/python';
    $module_script = $PROJECT_ROOT . "/modules/{$action}.py";
    if (!file_exists($module_script)) {
        http_response_code(404);
        echo json_encode(['ok'=>false,'error'=>'module_not_found','script'=>$module_script]);
        exit;
    }

    // убеждаемся, что папка runners и лог существуют
    @mkdir($RUNNERS_DIR, 0755, true);
    @touch($LOG_FILE);

    // формируем команду: nohup python module.py >> runners/log.txt 2>&1 & echo $! > py.pid
    $cmd = sprintf(
        "bash -lc 'cd %s && nohup %s %s >> %s 2>&1 & echo $! > %s'",
        escapeshellarg($PROJECT_ROOT),
        escapeshellarg($PYTHON),
        escapeshellarg($module_script),
        escapeshellarg($LOG_FILE),
        escapeshellarg($PID_FILE)
    );

    // выполняем
    shell_exec("echo \"=== START {$action} ".date('c')." ===\" >> " . escapeshellarg($LOG_FILE));
    shell_exec($cmd);

    // читаем PID и возвращаем успех
    $pid = @trim(@file_get_contents($PID_FILE));
    echo json_encode(['ok'=>true,'action'=>$action,'pid'=>$pid]);
    exit;
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['ok'=>false, 'error'=> $e->getMessage(), 'trace'=> $e->getTraceAsString()]);
    exit;
}