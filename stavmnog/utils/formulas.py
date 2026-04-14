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
    return round(result, 2)


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


def norm_avito_id(raw) -> str:
    """
    Приводит AvitoId к канонической строковой форме.
    Обрабатывает: пробелы, апострофы, запятые, scientific notation, float с .0.
    Возвращает пустую строку если не удалось распознать.
    """
    if raw is None:
        return ""
    s = str(raw).strip().replace("'", "").replace(" ", "").replace(",", "")
    if not s or s.lower() in ("none", "nan", "avitoid", "null"):
        return ""
    # scientific notation или float
    if "e" in s.lower() or "." in s:
        try:
            return str(int(float(s)))
        except (ValueError, OverflowError):
            return ""
    # чистое целое в строке
    try:
        return str(int(s))
    except ValueError:
        return ""