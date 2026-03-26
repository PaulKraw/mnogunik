#!/usr/bin/env bash
set -euo pipefail

PROJECT="/var/www/mnogunik.ru/mnogunik"

cd "$PROJECT"

# 1) create venv if missing
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

# 2) activate and install deps
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt

# 3) quick verification
python - <<'PY'
import sys
import pandas, PIL, requests, chardet
from dateutil import parser
print("OK: модули стоят.")
print("Python:", sys.version.split()[0])
print("VENV_PY:", sys.executable)
PY

deactivate
