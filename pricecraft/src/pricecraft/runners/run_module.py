#!/usr/bin/env python3
# runners/run_module.py
import sys, os, json, subprocess, time
from datetime import datetime
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from services.status_updater import StatusUpdater

if len(sys.argv) < 2:
    print("Usage: run_module.py <module_name>")
    sys.exit(2)

module = sys.argv[1].strip()
allowed_file = os.path.join(PROJECT_ROOT, 'config', 'allowed_modules.json')
if not os.path.exists(allowed_file):
    print("allowed_modules.json not found")
    sys.exit(3)
allowed = json.load(open(allowed_file))
if module not in allowed:
    print("Module not allowed")
    sys.exit(4)

module_path = os.path.join(PROJECT_ROOT, 'modules', f"{module}.py")
if not os.path.exists(module_path):
    print("Module file not found:", module_path)
    sys.exit(5)

status_file = os.path.join(PROJECT_ROOT, 'web', 'status.json')
updater = StatusUpdater(status_file)

updater.write_start(module, os.getpid(), message="Launching module")

# Start module as subprocess (so errors are captured)
proc = subprocess.Popen([sys.executable, module_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

try:
    while True:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=1)
            if proc.returncode == 0:
                updater.write_finish("finished", message="Completed successfully")
                sys.exit(0)
            else:
                err_text = err.decode('utf-8', errors='ignore') + "\n" + out.decode('utf-8', errors='ignore')
                updater.write_error(err_text)
                sys.exit(proc.returncode)
        else:
            updater.write_heartbeat(message="running")
            time.sleep(2)
except Exception as e:
    updater.write_error(str(e))
    proc.kill()
    sys.exit(1)
