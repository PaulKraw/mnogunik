"""
core/text.py — Обработка текстов объявлений.

Функции:
- replace_synonyms: замена $sin{A || B}$sin на случайный вариант
- replace_vartext: вставка текста из файлов (pastetxt)
- replace_gipotez: подстановка гипотез из CSV
- create_and_process_text: основной пайплайн обработки Description
- create_and_process_unik_text: обработка уникального Description
"""

import os
import random
import re
import sys
from typing import Any, Dict, List, Optional

import pandas as pd

from shared.config import ROOT_DIR
from shared.logger import print_log
from generator.utils.helpers import (
    generate_random_code,
    generate_random_char_code,
    generate_random_hex_color,
)


# ═══════════════════════════════════════════
# СИНОНИМЫ
# ═══════════════════════════════════════════

def replace_synonyms(text: str) -> str:
    """
    Заменяет конструкции $sin{вариант1 || вариант2 || ...}$sin
    на случайно выбранный вариант. Поддерживает вложенность.

    Args:
        text: Текст с синонимами.

    Returns:
        Текст с заменёнными синонимами.
    """
    def _process_block(inner: str) -> str:
        inner = inner[5:-5].strip()
        synonyms = [s.strip() for s in inner.split("||") if s.strip()]
        return random.choice(synonyms) if synonyms else ""

    # Обработка вложенных конструкций (изнутри наружу)
    pattern = r"\$sin\{[^{}]*\s*\$sin\}"
    while re.search(pattern, text, flags=re.DOTALL):
        text = re.sub(
            pattern,
            lambda m: _process_block(m.group(0)),
            text,
            flags=re.DOTALL,
        )

    # Оставшиеся (невложенные)
    final = r"\$sin\{(.*?)\s*\$sin\}"
    while re.search(final, text, flags=re.DOTALL):
        text = re.sub(
            final,
            lambda m: _process_block(m.group(0)),
            text,
            flags=re.DOTALL,
        )

    return text


# ═══════════════════════════════════════════
# ВСТАВКА ТЕКСТА ИЗ ФАЙЛОВ
# ═══════════════════════════════════════════

def replace_vartext(lines: List[str], cl_name: str) -> List[str]:
    """
    Заменяет pastetxt(filename) на содержимое файла var/text/filename.txt.

    Args:
        lines: Список строк шаблона.
        cl_name: Имя клиента (папка).

    Returns:
        Список строк с подставленным содержимым.
    """
    text_dir = os.path.join(ROOT_DIR, cl_name, "var", "text")
    new_lines: List[str] = []
    pattern = re.compile(r"pastetxt\((.*?)\)")

    for line in lines:
        matches = pattern.findall(line)
        if not matches:
            new_lines.append(line)
            continue

        for match in matches:
            full_path = os.path.join(text_dir, f"{match}.txt")
            if os.path.isfile(full_path):
                with open(full_path, encoding="utf-8") as f:
                    content = f.read()
                    if content and not content.endswith(("\n", "\r")):
                        content += "\n"
            else:
                content = ""
            line = line.replace(f"pastetxt({match})", content)

        new_lines.extend(line.splitlines(keepends=True))

    return new_lines


# ═══════════════════════════════════════════
# ГИПОТЕЗЫ
# ═══════════════════════════════════════════

def replace_gipotez(text: str, row: Any, cl_name: str) -> str:
    """
    Заменяет pastegipotez(field) на значение из gipotez.csv по артикулу.

    Args:
        text: Текст с вызовами pastegipotez(...).
        row: Строка DataFrame (dict-like) с полем 'articul'.
        cl_name: Имя клиента.

    Returns:
        Текст с подставленными данными.
    """
    pattern = re.compile(r"pastegipotez\((.*?)\)")
    matches = pattern.findall(text)

    if not matches:
        return text

    articul = row.get("articul")
    if not articul:
        raise ValueError("В строке нет поля 'articul'")

    csv_path = os.path.join(ROOT_DIR, cl_name, "var", "text", "gipotez.csv")
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"Файл не найден: {csv_path}")

    df = pd.read_csv(csv_path)
    filtered = df[df["articul"] == articul]

    if filtered.empty:
        raise ValueError(f"В {csv_path} нет строк с articul={articul}")

    selected = filtered.sample(n=1).iloc[0]

    def _replace(m):
        key = m.group(1).strip()
        if key not in selected:
            return f"[Нет поля {key}]"
        return str(selected[key])

    return pattern.sub(_replace, text)


# ═══════════════════════════════════════════
# ШАБЛОНЫ ПЕРЕМЕННЫХ
# ═══════════════════════════════════════════

def replace_template(match: re.Match) -> str:
    """Обработка {gencode(N)}, {genchar(N)}, {genhex()} в тексте."""
    template = match.group(1)
    parts = template.split("(")
    fn = parts[0]

    if len(parts) > 1:
        args = parts[1].strip(")").split(",")
        if fn == "gencode" and len(args) == 1:
            return generate_random_code(int(args[0]))
        if fn == "genchar" and len(args) == 1:
            return generate_random_char_code(int(args[0]))
        if fn == "genhex":
            return generate_random_hex_color()

    return "{" + template + "}"


def replace_vars(text: str, values: List[str]) -> str:
    """Заменяет {$var_N, prm:..., ...} на значения из списка."""
    pattern = r"\{\$var_(\d+),[^}]*\}"

    def _replace(m):
        idx = int(m.group(1)) - 1
        return values[idx] if idx < len(values) else m.group(0)

    return re.sub(pattern, _replace, text)


def replace_gen_int_with_step(match: re.Match) -> str:
    """Заменяет gen_int(min,max[,step]) на случайное число."""
    nums = list(map(int, re.findall(r"\d+", match.group(0))))
    if len(nums) == 2:
        start, end, step = nums[0], nums[1], 1
    elif len(nums) == 3:
        start, end, step = nums
    else:
        return match.group(0)

    values = list(range(start, end + 1, step))
    return str(random.choice(values)) if values else "0"


def parse_price_field(field_str: str) -> Optional[int]:
    """Парсит поле Price: число или rand(min;max;step)."""
    field_str = field_str.strip()
    m = re.match(r"rand\((\d+);(\d+);(\d+)\)", field_str)
    if m:
        start, end, step = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return random.choice(list(range(start, end + 1, step)))
    try:
        return int(field_str)
    except ValueError:
        return None


# ═══════════════════════════════════════════
# ЗАГРУЗКА СПИСКОВ ДЛЯ $pastevarlists()
# ═══════════════════════════════════════════

def load_lists_from_directory(directory_path: str) -> Dict[str, List[str]]:
    """
    Загружает все .txt файлы из папки в словарь {filename: [строки]}.

    Args:
        directory_path: Путь к папке со списками.

    Returns:
        Словарь с загруженными файлами.
    """
    result: Dict[str, List[str]] = {}
    if not os.path.exists(directory_path):
        return result

    for fn in os.listdir(directory_path):
        if fn.endswith(".txt"):
            path = os.path.join(directory_path, fn)
            with open(path, "r", encoding="utf-8") as f:
                result[fn] = f.readlines()

    return result


# ═══════════════════════════════════════════
# ГЕНЕРАЦИЯ УНИКАЛЬНЫХ ПАРАМЕТРОВ ($var)
# ═══════════════════════════════════════════

def generate_number_array(params: Dict[str, float]) -> List[float]:
    """Генерирует массив чисел на основе prm, mxp, mxm, stp."""
    prm = params["prm"]
    mxp = params["mxp"]
    mxm = params["mxm"]
    stp = params["stp"]

    arr = [prm]
    cur = prm
    while cur + stp <= prm + mxp:
        cur += stp
        arr.append(cur)
    cur = prm
    while cur - stp >= prm - mxm:
        cur -= stp
        arr.append(cur)

    return arr


def generate_rand_unique_objects(cl, text: Optional[str] = None) -> Any:
    """Генерирует массив уникальных вариаций переменных из шаблона."""
    from generator.utils.helpers import smart_format

    pattern = (
        r"\{\$var_(\d+),\s+prm:\s+([\d.]+),\s+mxp:\s+([\d.]+),"
        r"\s+mxm:\s+([\d.]+),\s+stp:\s+([\d.]+)\}"
    )

    if text is None:
        input_path = f"vars/{cl.orig_t}"
        max_combinations = max(cl.num_ads, 1000)
    else:
        input_path = text
        max_combinations = 1

    variants = []

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            m = re.search(pattern, line)
            if m:
                p = {
                    "prm": float(m.group(2)),
                    "mxp": float(m.group(3)),
                    "mxm": float(m.group(4)),
                    "stp": float(m.group(5)),
                }
                variants.append(generate_number_array(p))

    results = []
    formats = [
        "от {min_val} до {max_val} ",
        "от {min_val} до {max_val}",
        "{min_val} - {max_val} ",
        "{min_val} - {max_val}",
        "{min_val}-{max_val}",
        "в пределах {min_val}-{max_val}",
    ]
    probs = [0.1, 0.1, 0.2, 0.2, 0.3, 0.1]

    for _ in range(max_combinations):
        combo = []
        for var in variants:
            pair = random.sample(var, 2)
            min_v = smart_format(min(pair))
            max_v = smart_format(max(pair))
            fmt = random.choices(formats, probs)[0]
            combo.append(fmt.format(min_val=min_v, max_val=max_v))
        results.append(combo)

    return results if text is None else (results[0] if results else [])


# ═══════════════════════════════════════════
# ОСНОВНЫЕ ПАЙПЛАЙНЫ
# ═══════════════════════════════════════════

def create_and_process_text(cl, df: pd.DataFrame, root_dir: str) -> pd.DataFrame:
    """
    Обрабатывает столбец Description: читает шаблон, подставляет
    синонимы, переменные, гипотезы, списки.

    Args:
        cl: ClientParams.
        df: DataFrame прайса.
        root_dir: Корневая директория.

    Returns:
        DataFrame с обработанным Description.
    """
    lists_dir = f"{root_dir}/{cl.name}/var/lists"
    lists_dict = load_lists_from_directory(lists_dir)

    for index, row in df.iterrows():
        # Цена
        if "Price" in row and pd.notna(row["Price"]):
            df.at[index, "Price"] = parse_price_field(str(row["Price"]))

        # Если temp_Description пуст, а Description заполнен — пропускаем
        if (
            "temp_Description" in row
            and (pd.isna(row["temp_Description"]) or row["temp_Description"] == "")
            and pd.notna(row.get("Description"))
        ):
            continue

        # Путь к шаблону
        if "temp_Description" in row and pd.notna(row["temp_Description"]):
            template_path = f"{root_dir}/{cl.name}/var/text/{row['temp_Description']}"
        else:
            template_path = f"{root_dir}/{cl.name}/var/text/{row['Description']}"

        with open(template_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        if index == 3:
            print_log(f"Шаблон текста: {template_path}")

        lines = replace_vartext(lines, cl.name)

        # $perebor — перемешивание блоков
        begp = 0
        for j in range(len(lines)):
            trimmed = lines[j].strip()
            if "$perebor" in trimmed and any(c.isalnum() for c in trimmed):
                begp = j + 1
            if "$/perebor" in trimmed and any(c.isalnum() for c in trimmed):
                endp = j
                block = lines[begp:endp]
                random.shuffle(block)
                lines[begp:endp] = block

        text = "".join(lines).replace("\n", " ")
        text = text.replace("$/perebor", "").replace("$perebor", "")

        text = replace_gipotez(text, row, cl.name)
        text = replace_synonyms(text)

        # $pastetable(column)
        text = re.sub(
            r"\$pastetable\((.*?)\)",
            lambda m: str(row.get(m.group(1), "")),
            text,
        )

        text = replace_vars(text, generate_rand_unique_objects(cl, template_path))
        text = re.sub(r"\{(.*?)\}", replace_template, text)
        text = re.sub(r"gen_int\(\d+,\d+(?:,\d+)?\)", replace_gen_int_with_step, text)

        # $pastevarlists(name)
        def _replace_list(m):
            name = m.group(1) + ".txt"
            return random.choice(lists_dict[name]).strip() if name in lists_dict else ""

        text = re.sub(r"\$pastevarlists\((.*?)\)", _replace_list, text)

        # <art>...</art> → art-gip
        art_match = re.search(r"<art>(.*?)</art>", text)
        if art_match:
            df.at[index, "art-gip"] = art_match.group(1).strip()
            text = re.sub(r"<art>.*?</art>", "", text).strip()

        df.at[index, "Description"] = text

    return df


def create_and_process_unik_text(cl, df: pd.DataFrame, root_dir: str) -> pd.DataFrame:
    """
    Обрабатывает столбец param_unik (уникальные описания) аналогично Description.

    Логика идентична create_and_process_text, но:
    - Читает шаблон из temp_unik_Description
    - Записывает результат в param_unik (не в Description)

    Args:
        cl: ClientParams.
        df: DataFrame прайса.
        root_dir: Корневая директория.

    Returns:
        DataFrame с обработанным param_unik.
    """
    lists_dir = f"{root_dir}/{cl.name}/var/lists"
    lists_dict = load_lists_from_directory(lists_dir)

    for index, row in df.iterrows():
        if "temp_unik_Description" in row and (
            pd.isna(row["temp_unik_Description"]) or row["temp_unik_Description"] == ""
        ):
            continue

        if "temp_unik_Description" in row and pd.notna(row["temp_unik_Description"]):
            template_path = f"{root_dir}/{cl.name}/var/text/{row['temp_unik_Description']}"
        elif "param_unik" in row and pd.notna(row.get("param_unik")):
            template_path = f"{root_dir}/{cl.name}/var/text/{row['param_unik']}"
        else:
            continue

        with open(template_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        lines = replace_vartext(lines, cl.name)

        # $perebor
        begp = 0
        for j in range(len(lines)):
            trimmed = lines[j].strip()
            if "$perebor" in trimmed and any(c.isalnum() for c in trimmed):
                begp = j + 1
            if "$/perebor" in trimmed and any(c.isalnum() for c in trimmed):
                endp = j
                block = lines[begp:endp]
                random.shuffle(block)
                lines[begp:endp] = block

        text = "".join(lines).replace("\n", " ")
        text = text.replace("$/perebor", "").replace("$perebor", "")

        text = replace_gipotez(text, row, cl.name)
        text = replace_synonyms(text)

        text = re.sub(
            r"\$pastetable\((.*?)\)",
            lambda m: str(row.get(m.group(1), "")),
            text,
        )
        text = replace_vars(text, generate_rand_unique_objects(cl, template_path))
        text = re.sub(r"\{(.*?)\}", replace_template, text)
        text = re.sub(r"gen_int\(\d+,\d+(?:,\d+)?\)", replace_gen_int_with_step, text)

        def _replace_list(m):
            name = m.group(1) + ".txt"
            return random.choice(lists_dict[name]).strip() if name in lists_dict else ""

        text = re.sub(r"\$pastevarlists\((.*?)\)", _replace_list, text)

        art_match = re.search(r"<art>(.*?)</art>", text)
        if art_match:
            df.at[index, "art-gip"] = art_match.group(1).strip()
            text = re.sub(r"<art>.*?</art>", "", text).strip()

        df.at[index, "param_unik"] = text

    return df
