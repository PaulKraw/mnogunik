<?php
// web/index.php
?>
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Pricecraft Control</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    :root{--bg:#0f172a; --panel:#0b1220; --muted:#94a3b8; --accent:#ff7a18; --accent2:#8b5cf6; --accent3:#2ED92B; --accent4:#44A642; --card:#081022; --text:#e6eef8;}
    *{box-sizing:border-box}
    body{font-family:Inter,Arial; margin:0; padding:24px; background:linear-gradient(180deg,#071023 0%, #071322 100%); color:var(--text)}
    .wrap{max-width:1100px; margin:0 auto}
    h1{font-size:20px; margin:0 0 12px 0}
    .panel{background:var(--panel); padding:18px; border-radius:12px; box-shadow:0 8px 30px rgba(0,0,0,.6)}
    form {display:flex; gap:8px; flex-wrap:wrap; align-items:center}
    input[type="password"]{padding:10px 12px; border-radius:8px; border:1px solid #223; background:#07111a; color:var(--text)}
    button{padding:10px 14px; border-radius:10px; border:0; cursor:pointer; font-weight:700}
    .primary{background:linear-gradient(90deg,var(--accent),var(--accent2)); color:#071022}
    .primary_raz{background:linear-gradient(90deg,var(--accent3),var(--accent4)); color:#071022}
    .ghost{background:transparent; border:1px solid rgba(255,255,255,.06); color:var(--text)}
    .cols{display:grid; grid-template-columns: 1fr 360px; gap:12px; margin-top:12px}
    pre{background:linear-gradient(180deg,#04101a,#021018); padding:12px; border-radius:10px; min-height:160px; overflow:auto; color:#cfe8ff; border:1px solid rgba(255,255,255,.04)}
    .statusline{font-size:13px; color:var(--muted); margin-top:8px}
    .badge{display:inline-block; padding:6px 10px; border-radius:999px; background:rgba(255,122,24,.12); color:var(--text); font-weight:700}
    #logArea {max-width:90vw; width:90%;  overflow: auto;}
    .flex{
     min-width: 300px;
    max-width: 400px;
    flex: 1 1 400px;
    display: flex;
    flex-direction: column;
    padding: .5em;
    margin: .3em;
    border: 1px solid #e4e4e466;
    }


    .flexrow{
     /* min-width: 300px; */
    /* max-width: 400px; */
    flex: 1 1 100%;
    display: flex;
    /* flex-direction: column; */
    padding: .5em;
    margin: .3em;
    border: 1px solid #e4e4e466;
    }
   .flexrow > *{
    margin: .3em;

   }
      .btn-wrap{
      display: flex;
    flex-direction: column;
    padding: .5em;
    margin: .3em;
    border: 1px solid #3d4c7d;
    }

    .btn-wrap[data-action="stop"] button {
    transition: all 0.3s ease;
}

.btn-wrap[data-action="stop"] button:hover {
    transform: scale(1.05);
}

/* Для статуса остановки */
.status-stopped {
    background: linear-gradient(90deg, #ef4444, #dc2626) !important;
    color: white !important;
}

.status-running {
    background: linear-gradient(90deg, #10b981, #059669) !important;
    color: white !important;
}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Pricecraft · Панель управления</h1>

    <div class="panel">


    <form id="controlForm" onsubmit="return false;">
  <div class="flex">
  <label><input id="pwd" type="password" placeholder="Пароль" required=""></label>
  

<div class="btn-wrap" data-action="create_configurations">
  <button id="btnCreate" class="primary" data-action="create_configurations">
    Создать конфигурации
  </button>
  <span class="btn-status">✅ 2026-01-27 13:06:58</span>
</div>


<div class="btn-wrap" data-action="generate_ozon_content">
  <button class="primary_raz" data-action="generate_ozon_content">
    Создать контент для Ozon
  </button>
  <span class="btn-status">✅ 2026-01-20 19:05:14</span>
</div>
</div>

 <div class="flex">
 <div class="btn-wrap" data-action="generate_ozon_file">
  <button class="primary" data-action="generate_ozon_file">
    Создать прайс Ozon
  </button>
  <span class="btn-status">✅ 2026-01-27 13:08:10</span>
</div>
<div class="btn-wrap" data-action="update_ozon_prices">
  <button class="primary" data-action="update_ozon_prices">
    Обновить цены Ozon
  </button>
  <span class="btn-status">✅ 2026-01-27 13:09:31</span>
</div>

<div class="btn-wrap" data-action="actualize_ozon">
  <button class="primary" data-action="actualize_ozon">
    🔄 Актуализировать Ozon
  </button>
  <span class="btn-status"></span>
</div>

</div>



 <div class="flex">
<div class="btn-wrap" data-action="generate_wb_content">
  <button class="primary_raz" data-action="generate_wb_content">
    Создать контент для WB
  </button>
  <span class="btn-status"></span>
</div>
<div class="btn-wrap" data-action="generate_wb_file">
  <button class="primary" data-action="generate_wb_file">
    Создать прайс WB
  </button>
  <span class="btn-status"></span>
</div>
<div class="btn-wrap" data-action="update_wb_prices">
  <button class="primary_raz" data-action="update_wb_prices">
    Обновить цены WB
  </button>
  <span class="btn-status"></span>
</div>
</div>

<!-- 
        <button id="btnCreate" class="primary" data-action="create_configurations">Создать конфигурации</button>
        <button id="btnUpdateOzon" class="primary" data-action="update_ozon_prices">Создать контент для Ozon</button>

        <button id="btnBuildOzon" class="primary" data-action="generate_ozon_file">Создать прайс Ozon</button>
        <button id="btnUpdateOzon" class="primary" data-action="update_ozon_prices">Обновить цены Ozon</button>


        <button id="btnUpdateOzon" class="primary" data-action="update_ozon_prices">Создать контент для WB</button>

        <button id="btnBuildWB" class="primary_raz" data-action="generate_wb_file">Создать прайс WB</button>
        <button id="btnUpdateWB" class="primary_raz" data-action="update_wb_prices">Обновить цены WB</button>
       -->
      </form>

      <div class="cols">
        <div><div class="flexrow">
          <h3>Лог выполнения</h3><button id="clearLogBtn">Очистить лог</button>
          <!-- Кнопка остановки -->
<div class="btn-wrap" id="stopControl">
    <button id="stopBtn" class="ghost" style="border-color: #ef4444; color: #ef4444">
        ⏸️ Остановить все
    </button>
    <span class="btn-status"></span>
</div> </div>
          <pre id="logArea">Лог пуст...</pre>
        </div>
        



       
        
      </div>
      <div>
          <h3>Статус</h3>
          <pre id="statusArea">Подключение...</pre>
          <div class="statusline">
            <span class="badge" id="runFlag">idle</span>
            <span id="lastMsg" style="margin-left:8px; color:var(--muted)">—</span>
          </div>
          <button class="primary" >активный модуль</button><br>
        <button  class="primary_raz" >разрабатывается</button><br>
        <button  class="ghost" >заглушка</button>
 
        </div>
    </div>
  </div>
  <script>
document.getElementById('clearLogBtn').addEventListener('click', async () => {
  // if (!confirm('Точно очистить лог?')) return;

  try {
    const res = await fetch('clear_log.php', { method: 'POST' });
    const data = await res.text();
    // alert(data);
  } catch (err) {
    alert('Ошибка: ' + err);
  }
});
</script>
<script>
const POLL_INTERVAL = 2000; // 2s
let pollTimer = null;
let logTimer = null;
let running = false;

const pwdEl = document.getElementById('pwd');
const statusArea = document.getElementById('statusArea');
const logArea = document.getElementById('logArea');
const runFlag = document.getElementById('runFlag');
const lastMsg = document.getElementById('lastMsg');

// helper: post action to action.php
async function runAction(action) {
  const pwd = pwdEl.value.trim();
  if(!pwd){ alert('Введите пароль'); return; }
  disableButtons(true);
  runFlag.textContent = 'starting';
  try {
    const form = new URLSearchParams();
    form.append('password', pwd);
    form.append('action', action);

    const res = await fetch('action.php', { method:'POST', body: form });
    const txt = await res.text();
    // assume server returns quick response; status polling will update real state
    return txt;
  } catch(e) {
    alert('Ошибка запуска: ' + e);
    disableButtons(false);
    runFlag.textContent = 'idle';
    throw e;
  }
}

function disableButtons(disable){
  document.querySelectorAll('button[data-action]').forEach(b=>b.disabled = disable);
}

// attach buttons
document.querySelectorAll('button[data-action]').forEach(btn=>{
  btn.addEventListener('click', async (e)=>{
    e.preventDefault();
    const action = btn.getAttribute('data-action');
    try {
      await runAction(action);
      // start polling status/log
      if (!pollTimer) pollTimer = setInterval(loadStatus, POLL_INTERVAL);
      if (!logTimer) logTimer = setInterval(loadLog, POLL_INTERVAL);
      loadStatus(); loadLog();
    } catch(e){}
  });
});

// load status
async function loadStatus(){
  try {
    const r = await fetch('status.php?ts=' + Date.now());
    if(!r.ok){ statusArea.textContent = 'Status unavailable'; return; }
    const j = await r.json();
    statusArea.textContent = JSON.stringify(j, null, 2);
    runFlag.textContent = (j.status || j.running) ? 'running' : 'idle';
    lastMsg.textContent = j.message || j.status_text || '';
    if((j.status && j.status !== 'running') || j.running === false){
      // finished or error
      disableButtons(false);
      // stop polling if idle
      // keep logs for a bit
      // do not auto-clear timers here — keep user control
    } else {
      disableButtons(true);
    }
  } catch(err){
    statusArea.textContent = 'Status error: ' + err;
  }
}

// load log text
async function loadLog(){
  try {
    const r = await fetch('log.php?ts=' + Date.now());
    if(!r.ok){ logArea.textContent = 'Log unavailable'; return; }
    const text = await r.text();
    logArea.textContent = text || '—';
    logArea.scrollTop = logArea.scrollHeight;
  } catch(e){
    logArea.textContent = 'Log error: ' + e;
  }
}

// start polling immediately for status & log
loadStatus();
loadLog();
if (!pollTimer) pollTimer = setInterval(loadStatus, POLL_INTERVAL);
if (!logTimer) logTimer = setInterval(loadLog, POLL_INTERVAL);
</script>
<!-- 
<script>
document.querySelectorAll('.btn-wrap button').forEach(btn => {
  btn.addEventListener('click', async () => {
    const wrap = btn.closest('.btn-wrap');
    const action = wrap.dataset.action;

    await fetch('start_action.php', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: 'action=' + encodeURIComponent(action)
    });

    // мгновенно показываем статус
    wrap.querySelector('.btn-status').textContent = '⏳ Генерируется...';
  });
});
</script> -->

<script>
const STATUS_URL = './get_buttons_status.php';

async function updateStatuses(){
  try {
    const r = await fetch(STATUS_URL + '?t=' + Date.now());
    const data = await r.json();
    console.log("222");
    document.querySelectorAll('.btn-wrap').forEach(wrap => {
      const action = wrap.dataset.action;
      if (!data[action]) return;

      const span = wrap.querySelector('.btn-status');
      const st = data[action];

      if (st.status === 'generating') {
        span.textContent = '⏳ Генерируется...';
      } else if (st.status === 'done') {
        span.textContent = '✅ ' + st.generated_at;
      } else {
        span.textContent = '';
      }
    });
  } catch(e){}
}

setInterval(updateStatuses, 3000);
updateStatuses();
</script>

<script>
// Управление стоп-кнопкой
const stopBtn = document.getElementById('stopBtn');
const stopWrap = document.getElementById('stopControl');

async function updateStopButton() {
    try {
        const form = new URLSearchParams();
        form.append('password', document.getElementById('pwd').value || 'genMnogunik3472');
        form.append('action', 'status');
        
        const res = await fetch('stop_control.php', {
            method: 'POST',
            body: form
        });
        const data = await res.json();
        
        if (data.stopped) {
            stopBtn.innerHTML = '▶️ Возобновить';
            stopBtn.className = 'ghost';
            stopBtn.style.borderColor = '#10b981';
            stopBtn.style.color = '#10b981';
            stopWrap.querySelector('.btn-status').textContent = '🛑 Остановлено';
        } else {
            stopBtn.innerHTML = '⏸️ Остановить все';
            stopBtn.className = 'ghost';
            stopBtn.style.borderColor = '#ef4444';
            stopBtn.style.color = '#ef4444';
            stopWrap.querySelector('.btn-status').textContent = '';
        }
    } catch(e) {
        console.error('Stop status error:', e);
    }
}

stopBtn.addEventListener('click', async () => {
    const pwd = document.getElementById('pwd').value;
    if (!pwd) {
        alert('Введите пароль');
        return;
    }
    
    const isStopped = stopBtn.innerHTML.includes('Возобновить');
    const action = isStopped ? 'clear' : 'set';
    
    try {
        const form = new URLSearchParams();
        form.append('password', pwd);
        form.append('action', action);
        
        const res = await fetch('stop_control.php', { method: 'POST', body: form });
        const data = await res.json();
        
        if (data.ok) {
            updateStopButton();
            // Обновляем статус всех кнопок
            loadStatus();
        } else {
            alert('Ошибка: ' + data.error);
        }
    } catch(e) {
        alert('Ошибка сети: ' + e);
    }
});

// Обновляем статус кнопки каждые 5 секунд
setInterval(updateStopButton, 5000);
updateStopButton();
</script>

</body>
</html>
