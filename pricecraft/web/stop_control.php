<?php
// stop_control.php
header('Content-Type: application/json');

$PROJECT_ROOT = '/var/www/mnogunik.ru/code/pricecraft/src/pricecraft';
$STOP_FLAG = $PROJECT_ROOT . '/stop.flag';
$PASSWORD = 'genMnogunik3472'; // ваш пароль

$action = $_POST['action'] ?? '';
$pwd = $_POST['password'] ?? '';

if (!$pwd || $pwd !== $PASSWORD) {
    echo json_encode(['ok' => false, 'error' => 'bad_password']);
    exit;
}

$response = ['ok' => true];

switch ($action) {
    case 'set':
        file_put_contents($STOP_FLAG, time());
        $response['message'] = 'Стоп-флаг установлен';
        break;
        
    case 'clear':
        if (file_exists($STOP_FLAG)) {
            unlink($STOP_FLAG);
            $response['message'] = 'Стоп-флаг удален';
        } else {
            $response['message'] = 'Стоп-флаг уже удален';
        }
        break;
        
    case 'status':
        $response['stopped'] = file_exists($STOP_FLAG);
        $response['message'] = $response['stopped'] ? 'Процессы остановлены' : 'Все процессы работают';
        break;
        
    default:
        $response = ['ok' => false, 'error' => 'unknown_action'];
}

echo json_encode($response);