<?php


define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');
define('STAVMNOG_DIR', MONOREPO_ROOT . '/stavmnog');

// define('PYTHON',       MONOREPO_ROOT . '/.venv/bin/python');

define('DB_PATH',      MONOREPO_ROOT . '/db/avito.db');
define('STATUS_DIR',   STAVMNOG_DIR . '/web/status');
define('LOG_DIR',      STAVMNOG_DIR . '/web/logs');
define('CONFIG_PATH',  MONOREPO_ROOT . '/db//config/clients.json');


// define('BASE_DIR',    __DIR__);
// define('CONFIG_PATH', BASE_DIR . '/config/clients.json');
// define('DB_PATH',     BASE_DIR . '/data/avito.db');
// define('STATUS_DIR',  BASE_DIR . '/status');
define('ACCESS_KEY',  'YOUR_SECRET_KEY');

$key  = $_COOKIE['panel_key'] ?? $_GET['key'] ?? $_POST['key'] ?? '';
$auth = ($key === ACCESS_KEY);
if (!$auth && isset($_POST['key'])) {
    if ($_POST['key'] === ACCESS_KEY) {
        setcookie('panel_key', ACCESS_KEY, time() + 86400 * 30, '/');
        header('Location: ' . $_SERVER['PHP_SELF']); exit;
    } else { $loginError = true; }
}
if (isset($_GET['logout'])) { setcookie('panel_key','',0,'/'); header('Location: ./'); exit; }

function load_clients(): array {
    if (!file_exists(CONFIG_PATH)) return [];
    $d = json_decode(file_get_contents(CONFIG_PATH), true);
    return is_array($d) ? $d : [];
}
function load_status(string $op, string $client): array {
    $p = STATUS_DIR . "/{$op}_{$client}.json";
    if (!file_exists($p)) return [];
    $d = json_decode(file_get_contents($p), true);
    return is_array($d) ? $d : [];
}
function is_alive(string $op_key, string $client): bool {
    $f = STATUS_DIR . "/{$op_key}_{$client}.pid";
    if (!file_exists($f)) return false;
    $pid = (int)trim(file_get_contents($f));
    return $pid > 0 && file_exists("/proc/{$pid}");
}
function db_val(string $sql, array $p = []) {
    if (!file_exists(DB_PATH)) return null;
    try {
        $db = new PDO('sqlite:' . DB_PATH);
        $st = $db->prepare($sql); $st->execute($p);
        $r  = $st->fetch(PDO::FETCH_NUM);
        return $r ? $r[0] : null;
    } catch (Exception $e) { return null; }
}
function load_setting(string $name, $default = '') {
    $f = STATUS_DIR . "/setting_{$name}.txt";
    return file_exists($f) ? trim(file_get_contents($f)) : $default;
}

$clients = load_clients();
$rewrite_days = (int)load_setting('rewrite_last_days', 1);
?>
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Авито · Панель</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Unbounded:wght@700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#080c14;--surf:#0d1420;--brd:#1a2535;--brd2:#243348;--text:#c8d8f0;--muted:#4a6080;
  --acc:#00d4ff;--grn:#00e87a;--yel:#ffd000;--red:#ff4060;
  --mono:'JetBrains Mono',monospace;--head:'Unbounded',sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--mono);font-size:13px;min-height:100vh;
  background-image:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(0,212,255,.07) 0%,transparent 60%),
  repeating-linear-gradient(0deg,transparent,transparent 39px,rgba(255,255,255,.018) 40px),
  repeating-linear-gradient(90deg,transparent,transparent 39px,rgba(255,255,255,.018) 40px)}
#bg-con{position:fixed;top:0;left:0;width:100vw;height:100vh;pointer-events:none;z-index:0;
  overflow:hidden;padding:14px 18px;display:flex;flex-direction:column;justify-content:flex-end}
.cl{font-family:var(--mono);font-size:11px;line-height:1.65;white-space:nowrap;color:rgba(0,212,255,.45)}
.cl.e{color:rgba(255,64,96,.88)}.cl.ok{color:rgba(0,232,122,.58)}.cl.sep{color:rgba(0,212,255,.15)}
.wrap{position:relative;z-index:1;max-width:1160px;margin:0 auto;padding:20px 18px}
.hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 0 14px;
  margin-bottom:20px;border-bottom:1px solid var(--brd);position:sticky;top:0;z-index:10;
  background:rgba(8,12,20,.92);backdrop-filter:blur(4px)}
.logo{font-family:var(--head);font-size:17px;font-weight:900;color:#fff}
.logo span{color:var(--acc)}
.hdr-r{display:flex;align-items:center;gap:10px;font-size:11px;color:var(--muted)}
.run-pill{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:20px;
  font-size:10px;font-weight:700;background:rgba(0,212,255,.12);color:var(--acc);
  border:1px solid rgba(0,212,255,.3);margin-left:8px}
.run-pill .dot{width:6px;height:6px;border-radius:50%;background:var(--acc);animation:bl 1s infinite}
@keyframes bl{0%,100%{opacity:1}50%{opacity:.2}}
.gbar,.sbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;
  background:rgba(13,20,32,.88);border:1px solid var(--brd);border-radius:11px;padding:11px 15px;margin-bottom:14px}
.sbar{font-size:11px;gap:12px}
.sbar label{color:var(--muted)}
.sbar input[type=number]{width:58px;padding:4px 8px;background:var(--bg);border:1px solid var(--brd2);
  border-radius:6px;color:var(--text);font-family:var(--mono);font-size:12px}
.sbar input:focus{outline:none;border-color:var(--acc)}
.sep-v{width:1px;height:20px;background:var(--brd);margin:0 4px}
.active-info{flex:1;text-align:right;font-size:10px;color:var(--muted)}
button,.btn{cursor:pointer;border:none;border-radius:7px;font-family:var(--mono);font-weight:600;
  font-size:11px;letter-spacing:.4px;padding:7px 13px;transition:all .15s;text-decoration:none;display:inline-block}
.btn-a{background:rgba(13,20,32,.9);color:var(--text);border:1px solid var(--brd2)}
.btn-a:hover{border-color:var(--acc);color:var(--acc)}
.btn-stop-all{background:rgba(255,64,96,.15);color:var(--red);border:1px solid rgba(255,64,96,.4)}
.btn-stop-all:hover{background:rgba(255,64,96,.28)}
.btn-r{background:rgba(0,212,255,.1);color:var(--acc);border:1px solid rgba(0,212,255,.28);width:100%;text-align:left}
.btn-r:hover{background:rgba(0,212,255,.18)}.btn-r:disabled{opacity:.35;cursor:not-allowed}
.btn-r.g{background:rgba(0,232,122,.09);color:var(--grn);border-color:rgba(0,232,122,.28)}
.btn-r.g:hover{background:rgba(0,232,122,.18)}
.btn-r.y{background:rgba(255,208,0,.09);color:var(--yel);border-color:rgba(255,208,0,.28)}
.btn-r.y:hover{background:rgba(255,208,0,.18)}
.btn-s{background:rgba(255,64,96,.09);color:var(--red);border:1px solid rgba(255,64,96,.28);
  width:100%;margin-top:5px;display:none}
.btn-s:hover{background:rgba(255,64,96,.2)}
.badge{display:none;align-items:center;gap:4px;padding:2px 7px;border-radius:20px;font-size:9px;
  font-weight:700;background:rgba(0,212,255,.1);color:var(--acc);border:1px solid rgba(0,212,255,.25);margin-left:6px}
.badge.on{display:inline-flex}
.badge .dot{width:5px;height:5px;border-radius:50%;background:var(--acc);animation:bl 1s infinite}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:14px}
.card{background:rgba(13,20,32,.88);border:1px solid var(--brd);border-radius:13px;overflow:hidden;transition:border-color .2s}
.card:hover{border-color:var(--brd2)}
.chdr{display:flex;align-items:center;justify-content:space-between;padding:12px 15px 9px;border-bottom:1px solid var(--brd)}
.cname{font-family:var(--head);font-size:12px;font-weight:700;color:#fff}
.ckey{color:var(--muted);font-size:10px;margin-top:2px}
.clink{color:var(--muted);font-size:10px;text-decoration:none;border:1px solid var(--brd);
  border-radius:5px;padding:3px 7px;transition:all .15s}
.clink:hover{color:var(--acc);border-color:var(--acc)}
.cbody{padding:11px 15px}
.op{margin-bottom:10px}.op:last-child{margin-bottom:0}
.stl{display:flex;align-items:center;gap:5px;margin-top:5px;font-size:10px;color:var(--muted);min-height:15px}
.di{width:6px;height:6px;border-radius:50%;background:var(--muted);flex-shrink:0}
.dr{width:6px;height:6px;border-radius:50%;background:var(--acc);flex-shrink:0;animation:bl 1s infinite}
.dd{width:6px;height:6px;border-radius:50%;background:var(--grn);flex-shrink:0}
.de{width:6px;height:6px;border-radius:50%;background:var(--red);flex-shrink:0}
.pw{margin-top:6px;display:none}.pb{height:3px;background:var(--brd);border-radius:2px;overflow:hidden;margin-bottom:3px}
.pf{height:100%;background:var(--acc);width:0%;transition:width .5s}.pt{font-size:10px;color:var(--muted)}
.csum{display:grid;grid-template-columns:1fr 1fr;gap:5px;padding:9px 15px;
  border-top:1px solid var(--brd);background:rgba(0,0,0,.15)}
.sl{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.sv{font-size:12px;font-weight:700;color:#fff;margin-top:1px}
.sv.y{color:var(--yel)}.sv.g{color:var(--grn)}
.login-wrap{display:flex;align-items:center;justify-content:center;min-height:90vh}
.lbox{background:rgba(13,20,32,.95);border:1px solid var(--brd2);border-radius:14px;
  padding:36px 44px;width:340px;text-align:center}
.lbox h2{font-family:var(--head);font-size:15px;margin-bottom:22px;color:#fff}
.lbox input{width:100%;padding:11px 14px;background:var(--bg);border:1px solid var(--brd2);
  border-radius:9px;color:var(--text);font-family:var(--mono);font-size:13px;margin-bottom:10px;outline:none}
.lbox input:focus{border-color:var(--acc)}
.lerr{color:var(--red);font-size:11px;margin-bottom:8px}

.wrap {
      max-width: 360px;
    margin: 0 auto 0 66%;
    padding: 20px 18px 200px;
}
</style>
</head>
<body>
<div id="bg-con"></div>

<?php if (!$auth): ?>
<div class="login-wrap">
  <div class="lbox">
    <h2>АВИТО<span style="color:var(--acc)">·</span>ПАНЕЛЬ</h2>
    <?php if (!empty($loginError)): ?><div class="lerr">Неверный ключ</div><?php endif; ?>
    <form method="POST">
      <input type="password" name="key" placeholder="Ключ доступа" autofocus>
      <button type="submit" class="btn" style="width:100%;padding:11px;background:var(--acc);color:#050a14">Войти</button>
    </form>
  </div>
</div>
<?php else: ?>

<div class="wrap">

  <div class="hdr">
    <div>
      <span class="logo">АВИТО<span>·</span>ПАНЕЛЬ</span>
      <span id="hdr-pill" class="run-pill" style="display:none">
        <span class="dot"></span><span id="hdr-txt">работает</span>
      </span>
    </div>
    <div class="hdr-r">
      <span><?= date('d.m.Y H:i') ?></span>
      <a href="?logout=1" style="color:var(--muted);text-decoration:none">выйти</a>
    </div>
  </div>

  <div class="gbar">
    <span style="color:var(--muted);font-size:11px">ВСЕ:</span>
    <button class="btn btn-a" onclick="runAll('download')">↓ Скачать стат.</button>
    <button class="btn btn-a" onclick="runAll('build_export')">≡ Аналитика + Sheets</button>
    <button class="btn btn-a" onclick="runAll('apply_bids')">▶ Применить ставки</button>
    <div class="sep-v"></div>
    <button class="btn btn-stop-all" onclick="stopAll()">■ СТОП ВСЁ</button>
    <div class="active-info" id="act-info">нет активных процессов</div>
  </div>

  <div class="sbar">
    <label>Перезаписывать последних дней:</label>
    <input type="number" id="rw-days" value="<?= $rewrite_days ?>" min="1" max="30"
           onchange="saveSetting('rewrite_last_days', this.value)">
    <span style="color:var(--muted);font-size:10px">(при скачивании статистики)</span>
    <span id="sv-ok" style="color:var(--grn);font-size:10px;display:none">✓ сохранено</span>
  </div>

  <div class="grid">
  <?php foreach ($clients as $ckey => $cfg):
    if (empty($cfg['active'])) continue;
    $last  = db_val("SELECT MAX(stat_date) FROM item_stats WHERE client_key=?", [$ckey]);
    $cnt   = db_val("SELECT COUNT(DISTINCT item_id) FROM item_stats WHERE client_key=?", [$ckey]);
    $spend = db_val("SELECT ROUND(SUM(all_spend),0) FROM item_stats WHERE client_key=? AND stat_date=DATE('now')", [$ckey]);
    $st_dl = load_status('download',    $ckey);
    $st_bs = load_status('build_stats', $ckey);
    $st_ex = load_status('export',      $ckey);
    $st_bi = load_status('bids',        $ckey);
    $dl_on = is_alive('download',    $ckey);
    $bs_on = is_alive('build_stats', $ckey);
    $bi_on = is_alive('bids',        $ckey);
    $any   = $dl_on || $bs_on || $bi_on;
    $what  = $dl_on ? 'скачивание' : ($bs_on ? 'аналитика' : ($bi_on ? 'ставки' : ''));
    $sheet = 'https://docs.google.com/spreadsheets/d/' . ($cfg['sheet_id'] ?? '');
  ?>
  <div class="card" id="card-<?= $ckey ?>">
    <div class="chdr">
      <div>
        <div class="cname">
          <?= htmlspecialchars($cfg['name'] ?? $ckey) ?>
          <span class="badge <?= $any ? 'on' : '' ?>" id="badge-<?= $ckey ?>">
            <span class="dot"></span>
            <span id="badge-txt-<?= $ckey ?>"><?= $what ?></span>
          </span>
        </div>
        <div class="ckey"><?= $ckey ?> · <?= $cfg['user_id'] ?? '—' ?></div>
      </div>
      <?php if (!empty($cfg['sheet_id'])): ?>
      <a class="clink" href="<?= $sheet ?>" target="_blank">→ Sheet</a>
      <?php endif; ?>
    </div>

    <div class="cbody">

      <div class="op">
        <button class="btn-r" id="btn-download-<?= $ckey ?>"
                onclick="runOp('<?= $ckey ?>','download')"
                <?= $dl_on ? 'disabled' : '' ?>>↓ Скачать статистику</button>
        <button class="btn-s" id="stop-download-<?= $ckey ?>"
                onclick="stopOp('<?= $ckey ?>','download')"
                style="<?= $dl_on?'display:block':'' ?>">■ Стоп</button>
        <div class="stl" id="st-<?= $ckey ?>-download"><?php stRender($st_dl,$dl_on) ?></div>
      </div>

      <div class="op">
        <button class="btn-r g" id="btn-build_export-<?= $ckey ?>"
                onclick="runOp('<?= $ckey ?>','build_export')"
                <?= $bs_on ? 'disabled' : '' ?>>≡ Аналитика + в Sheets</button>
        <button class="btn-s" id="stop-build_export-<?= $ckey ?>"
                onclick="stopOp('<?= $ckey ?>','build_export')"
                style="<?= $bs_on?'display:block':'' ?>">■ Стоп</button>
        <div class="stl" id="st-<?= $ckey ?>-build_export"><?php stRender(!empty($st_ex)?$st_ex:$st_bs,$bs_on) ?></div>
      </div>

      <div class="op">
        <button class="btn-r y" id="btn-apply_bids-<?= $ckey ?>"
                onclick="runOp('<?= $ckey ?>','apply_bids')"
                <?= $bi_on ? 'disabled' : '' ?>>▶ Применить ставки</button>
        <button class="btn-s" id="stop-apply_bids-<?= $ckey ?>"
                onclick="stopOp('<?= $ckey ?>','apply_bids')"
                style="<?= $bi_on?'display:block':'' ?>">■ Стоп</button>
        <div class="stl" id="st-<?= $ckey ?>-apply_bids"><?php stRender($st_bi,$bi_on) ?></div>
        <div class="pw" id="pw-<?= $ckey ?>" style="<?= $bi_on?'display:block':'' ?>">
          <div class="pb"><div class="pf" id="pf-<?= $ckey ?>"></div></div>
          <div class="pt" id="pt-<?= $ckey ?>"></div>
        </div>
      </div>

    </div>

    <div class="csum">
      <div><div class="sl">Последняя дата</div>
           <div class="sv"><?= $last ? date('d.m.Y',strtotime($last)) : '—' ?></div></div>
      <div><div class="sl">Объявлений</div>
           <div class="sv"><?= $cnt ?: '—' ?></div></div>
      <div><div class="sl">Расход сегодня</div>
           <div class="sv y"><?= $spend ? number_format($spend,0,'.',' ').' ₽' : '—' ?></div></div>
      <div><div class="sl">Аналитика</div>
           <div class="sv g" id="ana-<?= $ckey ?>">
             <?= !empty($st_bs['finished_at']) ? date('d.m H:i',strtotime($st_bs['finished_at'])) : '—' ?>
           </div></div>
    </div>
  </div>
  <?php endforeach; ?>
  </div>
</div>

<script>
const KEY     = '<?= htmlspecialchars(ACCESS_KEY) ?>';
const CLIENTS = <?= json_encode(array_keys(array_filter($clients, fn($c)=>!empty($c['active'])))) ?>;
const OPS     = ['download','build_export','apply_bids'];
const OP_LABELS = {download:'скачивание', build_export:'аналитика', apply_bids:'ставки'};
const pollers = {};

// ---------------------------------------------------------------------------
// Настройка
// ---------------------------------------------------------------------------
async function saveSetting(name, value) {
    const fd = new FormData();
    fd.append('name', name); fd.append('value', value);
    await fetch(`set_setting.php?key=${encodeURIComponent(KEY)}`, {method:'POST',body:fd});
    const el = document.getElementById('sv-ok');
    el.style.display = 'inline';
    setTimeout(() => el.style.display = 'none', 2000);
}

// ---------------------------------------------------------------------------
// Запуск
// ---------------------------------------------------------------------------
/**
 * Запускает операцию (например, "apply_rates") для указанного клиента.
 * Вызывается при нажатии кнопки в панели, сгенерированной через foreach по списку клиентов.
 *
 * @param {Object} client - Объект или идентификатор клиента (используется для формирования ID кнопки и передачи на сервер).
 * @param {string} op - Название операции (например, 'download', 'apply_rates' и т.д.).
 */
async function runOp(client, op) {

    console.group(`%c[RUN] ${op} / ${client}`, 'color:#ffd000;font-weight:bold');
    console.log('time:', new Date().toISOString());
    console.log('button:', document.getElementById(`btn-${op}-${client}`));
    console.groupEnd();
    // 1. Блокируем кнопку операции для этого клиента, чтобы предотвратить повторный запуск
    const btn = document.getElementById(`btn-${op}-${client}`);
    if (btn) {
        btn.disabled = true;
    }

    // 2. Формируем данные POST-запроса
    const fd = new FormData();
    fd.append('client', client);
    fd.append('op', op);

    // Для операции 'download' добавляем дополнительный параметр — количество дней для перезаписи
    if (op === 'download') {
        fd.append('rewrite_days', document.getElementById('rw-days').value);
    }

    // 3. Отправляем асинхронный запрос на сервер (run.php)
    try {
        const r = await fetch(`run.php?key=${encodeURIComponent(KEY)}`, {
            method: 'POST',
            body: fd
        });
        const j = await r.json();
        console.log(`[RUN RESPONSE] ${client}/${op}:`, j);
        if (j.pid) console.log(`%cPID=${j.pid}`, 'color:#00d4ff');
        if (j.error) console.error(`[RUN ERROR]`, j.error);

        // Если сервер сообщает, что операция уже запущена — синхронизируем состояние интерфейса
        if (j.status === 'already_running') {
            // Вызов функции, которая, предположительно, устанавливает строковое состояние клиента.
            // Передаётся: client, op, объект статуса {status:'running'}, и флаг true (вероятно, принудительное обновление).
            setStLine(client, op, { status: 'running' }, true);
        }
    } catch (e) {
        console.error('runOp error:', e);
    }

    // 4. Обновляем интерфейс независимо от результата запроса (показываем кнопку Stop, запускаем опрос статуса и консоль)
    // showStop — отображает кнопку остановки для данного клиента и операции.
    showStop(client, op);

    // startPoll — начинает периодический опрос сервера для получения текущего статуса операции.
    startPoll(client, op);

    // startConsole — подключает/обновляет консоль вывода для данного клиента и операции.
    startConsole(client, op);

    // updateBadges — обновляет значки (бейджи) на панели клиента (например, счётчики или индикаторы).
    updateBadges();
}
async function runAll(op) {
    for (const c of CLIENTS) runOp(c, op);
}

// ---------------------------------------------------------------------------
// Стоп
// ---------------------------------------------------------------------------
async function stopOp(client, op) {
    fetch(`stop.php?key=${encodeURIComponent(KEY)}&client=${client}&op=${op}`);
    fetch(`kill.php?key=${encodeURIComponent(KEY)}&client=${client}&op=${op}`);
}
async function stopAll() {
    for (const c of CLIENTS) for (const op of OPS) stopOp(c, op);
}

function showStop(c, op){ const e=document.getElementById(`stop-${op}-${c}`); if(e) e.style.display='block'; }
function hideStop(c, op){ const e=document.getElementById(`stop-${op}-${c}`); if(e) e.style.display='none'; }

// ---------------------------------------------------------------------------
// Status line
// ---------------------------------------------------------------------------
function stHtml(s, alive) {
    if (!s || !s.status) return '<span class="di"></span>ожидание';
    const st = alive ? 'running' : s.status;
    const dc = {running:'dr',done:'dd',error:'de'}[st] || 'di';
    let txt = '';
    const ts = s.finished_at || s.started_at;
    if (ts) txt += new Date(ts.replace(' ','T')).toLocaleString('ru',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) + ' · ';
    if (st==='running') txt += 'выполняется…';
    else if (st==='done') {
        if (s.rows_fetched    != null) txt += `строк: ${s.rows_fetched}`;
        if (s.rows_written    != null) txt += `записей: ${s.rows_written}`;
        if (s.items_processed != null) txt += `объявл: ${s.items_processed}`;
        if (s.total_spend_rub != null) txt += ` · ${s.total_spend_rub} ₽`;
    } else if (st==='error') txt += 'ошибка: ' + String(s.error||'').slice(0,55);
    return `<span class="${dc}"></span>${txt}`;
}
function setStLine(c, op, s, alive) {
    const e = document.getElementById(`st-${c}-${op}`);
    if (e) e.innerHTML = stHtml(s, alive);
}

// ---------------------------------------------------------------------------
// Badges & header indicator
// ---------------------------------------------------------------------------
function updateBadges() {
    const running = [];
    for (const c of CLIENTS) {
        let what = null;
        for (const op of OPS) if (pollers[`${c}:${op}`]) { what = OP_LABELS[op]; break; }
        const b = document.getElementById(`badge-${c}`);
        const bt = document.getElementById(`badge-txt-${c}`);
        if (b) { if (what) { b.classList.add('on'); if(bt) bt.textContent=what; } else b.classList.remove('on'); }
        if (what) running.push(`${c}:${what}`);
    }
    const pill = document.getElementById('hdr-pill');
    const ptxt = document.getElementById('hdr-txt');
    const info = document.getElementById('act-info');
    if (running.length) {
        if (pill) pill.style.display='inline-flex';
        if (ptxt) ptxt.textContent = running.join(', ');
        if (info) info.textContent = 'работает: ' + running.join(', ');
    } else {
        if (pill) pill.style.display='none';
        if (info) info.textContent = 'нет активных процессов';
    }
}

// ---------------------------------------------------------------------------
// Polling (мониторинг статуса операции через периодические запросы)
// ---------------------------------------------------------------------------

/**
 * Запускает циклический опрос сервера для отслеживания статуса операции.
 * Используется после вызова runOp или при возобновлении наблюдения.
 *
 * @param {string|Object} client - Идентификатор клиента (используется в ключе pollers и в URL)
 * @param {string} op - Название операции ('apply_bids', 'download' и т.д.)
 */
function startPoll(client, op) {
    const pk = `${client}:${op}`;
    console.group(`%c[POLL START] ${pk}`, 'color:#00d4ff;font-weight:bold');
    console.log('time:', new Date().toISOString());
    
    if (pollers[pk]) {
        console.warn('уже был опрос, перезапускаем');
        clearInterval(pollers[pk]);
    }
    console.groupEnd();

    if (op === 'apply_bids') {
        const pw = document.getElementById(`pw-${client}`);
        if (pw) pw.style.display = 'block';
    }

    let tick = 0;
    pollers[pk] = setInterval(async () => {
        tick++;
        const url = `status.php?key=${encodeURIComponent(KEY)}&op=${op}&client=${client}&_=${Date.now()}`;
        try {
            const r = await fetch(url, {cache:'no-store'});
            const txt = await r.text();
            console.log(`[POLL ${pk} #${tick}] HTTP ${r.status}, body:`, txt.slice(0,300));
            
            if (!r.ok) return;
            let s;
            try { s = JSON.parse(txt); } 
            catch(e) { console.error(`[POLL ${pk}] JSON parse error:`, e, txt); return; }
            
            if (!s || !s.status) { 
                console.log(`[POLL ${pk}] пустой статус — ждём`); 
                return; 
            }
            
            console.log(`[POLL ${pk}] status=${s.status}`, s);
            setStLine(client, op, s, s.status === 'running');
            
            if (op === 'apply_bids') {
                updProg(client, s);
                if (s.overall) {
                    console.log(`[BIDS ${client}] taken=${s.overall.taken} done=${s.overall.done} ok=${s.overall.ok} err=${s.overall.err} skip=${s.overall.skip}`);
                }
            }

            if (s.status === 'done' || s.status === 'error') {
                console.log(`%c[POLL ${pk}] FINISH: ${s.status}`, 'color:' + (s.status==='done'?'#00e87a':'#ff4060'));
                if (s.error) console.error(`[${pk}] ERROR:`, s.error);
                clearInterval(pollers[pk]);
                delete pollers[pk];
                hideStop(client, op);
                const btn = document.getElementById(`btn-${op}-${client}`);
                if (btn) btn.disabled = false;
                if (op === 'apply_bids') {
                    const pw = document.getElementById(`pw-${client}`);
                    if (pw) pw.style.display = 'none';
                }
                updateBadges();
            }
        } catch(e) { 
            console.error(`[POLL ${pk}] network error:`, e); 
        }
    }, 2500);
    updateBadges();
}


function updProg(c, s) {
    const pf=document.getElementById(`pf-${c}`), pt=document.getElementById(`pt-${c}`);
    if (!pf||!pt||!s.overall) return;
    const done=s.overall.done||0, total=s.total||0, ok=s.overall.ok||0, err=s.overall.err||0;
    const pct=total>0?Math.min(100,Math.round(done/total*100)):0;
    pf.style.width=pct+'%';
    pt.textContent=`${pct}% · ${done}/${total} · OK:${ok} ERR:${err}`;
}

// ---------------------------------------------------------------------------
// Console
// ---------------------------------------------------------------------------
const MAX_CL = Math.floor(window.innerHeight / 18);
let conLines = [], conPoll = null, conLast = 0;

function clsFor(l) {
    const s=l.toLowerCase();
    if(s.includes('ошибка')||s.includes('error')||s.includes('traceback')) return 'e';
    if(s.includes('готово')||s.startsWith('===')) return 'ok';
    if(s.startsWith('---')||s.startsWith('===')) return 'sep';
    return '';
}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function renderCon(){
    const el=document.getElementById('bg-con'); if(!el) return;
    el.innerHTML=conLines.slice(-MAX_CL).map(l=>`<div class="cl ${clsFor(l)}">${esc(l)}</div>`).join('');
}
async function fetchCon(client,op){
    try{
        const r=await fetch(`log.php?key=${encodeURIComponent(KEY)}&client=${client}&op=${op}&lines=100&_=${Date.now()}`,{cache:'no-store'});
        if(!r.ok) return;
        const lines=await r.json(); if(!Array.isArray(lines)) return;
        if(lines.length>conLast){ conLines=conLines.concat(lines.slice(conLast)); conLast=lines.length; renderCon(); }
    }catch(e){}
}
function startConsole(client,op){
    if(conPoll) clearInterval(conPoll);
    conLast=0; conLines.push('',`--- ${client} / ${op} ---`);
    fetchCon(client,op);
    conPoll=setInterval(()=>fetchCon(client,op),1500);
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
    <?php foreach ($clients as $ckey => $cfg): if (empty($cfg['active'])) continue;
        if (is_alive('download',    $ckey)) echo "startPoll('$ckey','download');startConsole('$ckey','download');\n";
        if (is_alive('build_stats', $ckey)) echo "startPoll('$ckey','build_export');startConsole('$ckey','build_export');\n";
        if (is_alive('bids',        $ckey)) echo "startPoll('$ckey','apply_bids');startConsole('$ckey','apply_bids');\n";
    endforeach; ?>
    updateBadges();
});

window.dbg = function(client, op) {
    fetch(`log.php?key=${encodeURIComponent(KEY)}&client=${client}&op=${op}&lines=200`)
        .then(r => r.json())
        .then(lines => {
            console.group(`%c[LOG] ${client}/${op} (${lines.length} строк)`, 'color:#00d4ff');
            lines.forEach(l => console.log(l));
            console.groupEnd();
        });
};
</script>
<?php endif; ?>
</body>
</html>
<?php
function stRender(array $s, bool $alive=false){
    if(empty($s)){echo '<span class="di"></span>ожидание';return;}
    $st=$alive?'running':($s['status']??'idle');
    $c=['running'=>'dr','done'=>'dd','error'=>'de'][$st]??'di';
    $txt='';
    $ts=$s['finished_at']??$s['started_at']??'';
    if($ts) $txt.=date('d.m H:i',strtotime($ts)).' · ';
    if($st==='running') $txt.='выполняется…';
    elseif($st==='done'){
        if(isset($s['rows_fetched']))    $txt.='строк: '.$s['rows_fetched'];
        if(isset($s['rows_written']))    $txt.='записей: '.$s['rows_written'];
        if(isset($s['items_processed'])) $txt.='объявл: '.$s['items_processed'];
        if(isset($s['total_spend_rub'])) $txt.=' · '.$s['total_spend_rub'].' ₽';
    }elseif($st==='error') $txt.='ошибка: '.htmlspecialchars(substr($s['error']??'',0,55));
    echo "<span class=\"$c\"></span>".htmlspecialchars($txt);
}




?>