"""
stavmnog/utils/formulas.py — Формулы расчёта ставок и метрик.

Заменяет 2 копии calc_bid() из build_stats.py и export_stats.py.

Использование:
    from stavmnog.utils.formulas import calc_bid, safe_div, delta_pct
"""

import math


def calc_bid(
    minn: float,
    maxx: float,
    prev_bid: float,
    ctr: float,
    clicks: int,
    leads: int,
) -> float:
    """
    Рассчитывает ставку по формуле.

    Логика (аналог Excel из листа Ставки):
    - CTR=0 и кликов>10 → ceil(мин/2)
    - CTR=0 и кликов≤10 → мин
    - CTR<3% → 5₽
    - Нет лидов → мин
    - Лидов≤1 → max(мин, round(prev_bid × 0.7))
    - Лидов>1 → prev_bid
    - Результат не выше макс

    Args:
        minn: Минимальная ставка.
        maxx: Максимальная ставка.
        prev_bid: Предыдущая ставка / корректировка.
        ctr: CTR (показ→просмотр) за 7 дней.
        clicks: Просмотры за 7 дней.
        leads: Контакты за 7 дней.

    Returns:
        Рассчитанная ставка в рублях.
    """
    minn = minn or 0
    maxx = maxx or 0
    prev_bid = prev_bid or 0

    if ctr == 0:
        inner = math.ceil(minn / 2) if clicks > 10 else minn
    elif ctr < 0.03:
        inner = 5.0
    elif leads == 0:
        inner = minn
    elif leads <= 1:
        inner = max(minn, round(prev_bid * 0.7))
    else:
        inner = prev_bid

    result = min(maxx, inner) if maxx > 0 else inner
    # ставка должна быть кратна рублю — иначе Avito API отдаёт 400
    return int(round(result))


def safe_div(a: float, b: float) -> float:
    """Безопасное деление — возвращает 0 если делитель = 0."""
    return round(a / b, 4) if b and b != 0 else 0.0


def delta_pct(new_val: float, old_val: float) -> float:
    """Расчёт дельты в процентах."""
    if old_val and old_val != 0:
        return round((new_val - old_val) / old_val * 100, 2)
    return 0.0


def safe_float(val) -> float:
    """Безопасное преобразование ячейки Sheets в float."""
    if val is None or str(val).strip() == "":
        return 0.0
    try:
        return float(str(val).replace(",", ".").replace(" ", ""))
    except ValueError:
        return 0.0


def col_letter(n: int) -> str:
    """Индекс колонки (1-based) → буква A1-нотации."""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def norm_avito_id(raw):
    """
    Приводит AvitoId к строковому виду целого числа (без дробной части).
    Примеры:
        12345 -> "12345"
        12345.0 -> "12345"
        "12345" -> "12345"
        "12345.0" -> "12345"
        "12 345" -> "12345"
        "12345abc" -> "12345"
        None, "", "none" -> ""
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return ""
    # Оставляем только цифры (и минус на случай отрицательных, но в AvitoId минуса нет)
    import re
    digits = re.sub(r"[^\d-]", "", s)
    if not digits:
        return ""
    try:
        # Преобразуем в целое число (отбрасывает дробную часть)
        integer_value = int(float(digits))
        return str(integer_value)
    except (ValueError, TypeError):
        return ""

# ═══════════════════════════════════════════
# GOOGLE SHEETS — ОТКРЫТИЕ ЛИСТА
# ═══════════════════════════════════════════

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _extract_sheet_id(raw: str) -> str:
    """ID таблицы из URL или уже чистого ID."""
    if "/d/" in raw:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", raw)
        if m:
            return m.group(1)
    return raw.strip()


def open_worksheet(sheet_id_raw: str, sheet_name: str, cred_file: str, logger=None):
    """
    Открывает worksheet по ИМЕНИ (не по gid).
    gspread сам ищет лист с таким именем — не надо хранить gid в конфиге.
    """
    import gspread
    from google.oauth2.service_account import Credentials

    sheet_id = _extract_sheet_id(sheet_id_raw)
    creds = Credentials.from_service_account_file(cred_file, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(sheet_name)
    except Exception as e:
        available = [w.title for w in sh.worksheets()]
        raise ValueError(
            f"Лист '{sheet_name}' не найден. Доступные: {available}"
        ) from e

    if logger:
        logger.info(f"Открыт лист '{sheet_name}' (id={ws.id})")
    return ws



# ═══════════════════════════════════════════
# ИНДЕКС КОЛОНОК ПО ИМЕНИ
# ═══════════════════════════════════════════

def build_header_index(ws, required_columns, logger=None):
    """
    Возвращает {имя_колонки: 1-based_индекс} для указанных имён.
    Если какой-то колонки в листе нет — добавляет её в конец шапки.
    Имена сравниваются после strip() — пробелы по краям не ломают.

    Args:
        ws: gspread Worksheet.
        required_columns: список имён колонок которые должны быть.
        logger: опционально для вывода.

    Returns:
        Dict[str, int] — имя → позиция (1-based).
    """
    header = [h.strip() for h in ws.row_values(1)]
    added = []
    for name in required_columns:
        if name not in header:
            header.append(name)
            added.append(name)
    if added:
        last = col_letter(len(header))
        ws.update(range_name=f"A1:{last}1", values=[header], value_input_option="USER_ENTERED")
        if logger:
            logger.info(f"В шапку добавлены колонки: {added}")
    return {name: i + 1 for i, name in enumerate(header)}