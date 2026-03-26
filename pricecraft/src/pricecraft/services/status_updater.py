# services/status_updater.py
import json, os
from datetime import datetime

class StatusUpdater:
    def __init__(self, path):
        self.path = path

    def _write(self, data):
        tmp = self.path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def _read(self):
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}

    def write_start(self, module, pid, message="starting"):
        data = {
            "module": module,
            "status": "running",
            "pid": pid,
            "started_at": datetime.utcnow().isoformat() + "Z",
            "last_heartbeat": datetime.utcnow().isoformat() + "Z",
            "progress": "",
            "message": message
        }
        self._write(data)

    def write_heartbeat(self, message="running"):
        data = self._read() or {}
        data.update({
            "last_heartbeat": datetime.utcnow().isoformat() + "Z",
            "message": message
        })
        self._write(data)

    def write_finish(self, status="finished", message="done"):
        data = self._read() or {}
        data.update({
            "status": status,
            "last_heartbeat": datetime.utcnow().isoformat() + "Z",
            "message": message
        })
        self._write(data)

    def write_error(self, error_msg):
        data = self._read() or {}
        data.update({
            "status": "error",
            "last_heartbeat": datetime.utcnow().isoformat() + "Z",
            "message": error_msg
        })
        self._write(data)
