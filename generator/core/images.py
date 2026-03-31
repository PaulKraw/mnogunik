"""
core/images.py — Генерация и обработка изображений объявлений.

Объединяет логику из textfun.create_and_process_img_url и imgunik.process_image_row.
Содержит:
- process_image_row: обработка одного изображения (resize, rotate, text overlay)
- apply_modifications: случайные модификации для уникализации
- create_and_process_img_url: основной пайплайн генерации картинок
- Вспомогательные функции (wrap_text, calc_text_width и т.д.)
"""

import ast
import gc
import hashlib
import importlib.util
import json
import os
import random
import re
import sys
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from PIL import Image, ImageDraw, ImageEnhance, ImageFont

from shared.config import ROOT_DIR, ROOT_DIR_OUT, ROOT_URL_OUT, IS_LOCAL
from shared.logger import print_log
from generator.utils.helpers import natural_sort_key


# ═══════════════════════════════════════════
# КЭШИРОВАНИЕ ШРИФТОВ
# ═══════════════════════════════════════════

@lru_cache(maxsize=128)
def get_font_cached(font_path: str, size: int) -> ImageFont.FreeTypeFont:
    """Кэшированная загрузка TTF-шрифта."""
    return ImageFont.truetype(font_path, size)


# ═══════════════════════════════════════════
# ИЗМЕРЕНИЕ ТЕКСТА
# ═══════════════════════════════════════════

def calc_text_width(text: str, font_path: str, font_size: int) -> int:
    """Вычисляет ширину текста в пикселях."""
    font = get_font_cached(font_path, font_size)
    tmp = Image.new("RGBA", (1, 1))
    draw = ImageDraw.Draw(tmp)
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def calculate_text_bbox(
    text: str, font_path: str, font_size: int
) -> Tuple[int, int]:
    """Возвращает (width, height) текста."""
    font = get_font_cached(font_path, font_size)
    tmp = Image.new("RGBA", (1, 1))
    draw = ImageDraw.Draw(tmp)
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def wrap_text_to_width(
    text: str, font_path: str, font_size: int, max_width: int
) -> List[str]:
    """Разбивает текст на строки, чтобы каждая ≤ max_width пикселей."""
    words = text.split(" ")
    lines: List[str] = []
    current = ""

    for word in words:
        test = f"{current} {word}".strip()
        if calc_text_width(test, font_path, font_size) <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines


# ═══════════════════════════════════════════
# МОДИФИКАЦИИ ИЗОБРАЖЕНИЙ
# ═══════════════════════════════════════════

def apply_modifications(
    image: Image.Image,
    rotate_range: Tuple[float, float],
    size_range: Tuple[float, float],
    crop_range: Tuple[float, float],
    contrast_range: float,
) -> Image.Image:
    """
    Применяет случайные модификации к изображению для уникализации.

    Args:
        image: Исходное PIL Image.
        rotate_range: (min, max) угол поворота в градусах.
        size_range: (min%, max%) изменение размера.
        crop_range: (min%, max%) кроп с краёв.
        contrast_range: Амплитуда изменения контраста.

    Returns:
        Модифицированное изображение.
    """
    # Поворот
    angle = random.uniform(rotate_range[0], rotate_range[1])
    rotated = image.rotate(angle)

    # Масштаб
    factor = 1 + random.uniform(size_range[0], size_range[1]) / 100
    new_size = (int(image.width * factor), int(image.height * factor))
    resized = rotated.resize(new_size)

    # Кроп
    crop_box = (
        int(resized.width * crop_range[0] / 100),
        int(resized.height * crop_range[0] / 100),
        int(resized.width * (1 - crop_range[1] / 100)),
        int(resized.height * (1 - crop_range[1] / 100)),
    )
    cropped = resized.crop(crop_box)

    # Контраст
    contrast = 1 + random.uniform(-contrast_range, contrast_range)
    enhanced = ImageEnhance.Contrast(cropped).enhance(contrast)

    # Случайный сдвиг на фоне
    w, h = enhanced.size
    collage = Image.new("RGB", (w, h), (255, 255, 255))
    new_size2 = (w + 10, h + 10)
    enhanced2 = enhanced.resize(new_size2)
    ox = -random.randint(0, 10)
    oy = -random.randint(0, 10)
    collage.paste(enhanced2, (ox, oy))

    # Освобождение памяти
    for im in (rotated, resized, cropped, enhanced, enhanced2):
        try:
            im.close()
        except Exception:
            pass

    return collage


# ═══════════════════════════════════════════
# НАЛОЖЕНИЕ ТЕКСТА
# ═══════════════════════════════════════════

def add_text_to_image(
    image: Image.Image,
    text: str,
    position: Tuple[int, int],
    font_size: int,
    font_color: Tuple[int, ...] = (0, 0, 0),
    bg_color: Tuple[int, ...] = (200, 200, 200, 128),
    outline_color: Tuple[int, ...] = (255, 255, 255, 128),
    font_path: Optional[str] = None,
    outline_width: int = 5,
    center: bool = False,
    trim_left_half_font: bool = False,
) -> Image.Image:
    """
    Рисует текст на изображении с подложкой и обводкой.

    Args:
        image: PIL Image для рисования.
        text: Текст для наложения.
        position: (x, y) координаты.
        font_size: Размер шрифта.
        font_color: Цвет текста (R, G, B).
        bg_color: Цвет фона (R, G, B, A).
        outline_color: Цвет обводки (R, G, B, A).
        font_path: Путь к TTF-файлу.
        outline_width: Ширина обводки в пикселях.
        center: Центрировать по ширине изображения.
        trim_left_half_font: Обрезать подложку слева на полшрифта.

    Returns:
        Изображение с наложенным текстом.
    """
    if not isinstance(text, str):
        text = str(text)

    font = get_font_cached(font_path, font_size) if font_path else ImageFont.load_default()

    text_width, _ = calculate_text_bbox(text, font_path, font_size) if font_path else (100, font_size)
    _, text_height = calculate_text_bbox("Ay", font_path, font_size) if font_path else (100, font_size)

    # Если прозрачный фон и обводка — рисуем напрямую
    if bg_color[-1] == 0 and outline_color[-1] == 0:
        draw = ImageDraw.Draw(image)
        draw.text(position, text, font=font, fill=font_color)
        return image

    padding = int(font_size / 2)
    bg_w = text_width + padding * 2 + outline_width * 2
    bg_h = text_height + padding * 2 + outline_width * 2

    background = Image.new("RGBA", (bg_w, bg_h), (0, 0, 0, 0))
    draw_bg = ImageDraw.Draw(background)

    # Обводка
    draw_bg.rectangle([0, 0, bg_w, bg_h], fill=outline_color)

    # Фон
    draw_bg.rectangle(
        [outline_width, outline_width,
         outline_width + text_width + padding * 2,
         outline_width + text_height + padding * 2],
        fill=bg_color,
    )

    # Текст
    text_pos = (outline_width + padding, outline_width + padding - round(font_size / 3))
    draw_bg.text(text_pos, text, font=font, fill=font_color)

    x, y = position
    if center:
        x = int((image.size[0] - bg_w) / 2)

    bg_to_paste = background
    if trim_left_half_font:
        trim_px = int(font_size / 2)
        if trim_px < background.width:
            bg_to_paste = background.crop((trim_px, 0, background.width, background.height))

    image.paste(bg_to_paste, (x, y), bg_to_paste)

    return image


# ═══════════════════════════════════════════
# ЗАГРУЗЧИК СКРИПТОВ (first_img)
# ═══════════════════════════════════════════

class ScriptLoader:
    """LRU-кэш для динамически загружаемых Python-скриптов генерации картинок."""

    def __init__(self, maxsize: int = 256, func_name: str = "execute_task"):
        self.maxsize = maxsize
        self.func_name = func_name
        self._cache: OrderedDict = OrderedDict()

    def get(self, script_path: str) -> Callable:
        """Загружает и кэширует функцию execute_task из скрипта."""
        abs_path = os.path.abspath(script_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Script not found: {abs_path}")

        mtime = os.stat(abs_path).st_mtime
        key = (abs_path, mtime)

        if key in self._cache:
            func = self._cache.pop(key)
            self._cache[key] = func
            return func

        # Удалить старые версии
        for k in list(self._cache.keys()):
            if k[0] == abs_path:
                self._cache.pop(k, None)

        # Загрузка
        mod_name = "dynmod_" + hashlib.md5(abs_path.encode()).hexdigest()
        spec = importlib.util.spec_from_file_location(mod_name, abs_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load: {abs_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        func = getattr(module, self.func_name, None)
        if not callable(func):
            raise AttributeError(f"{abs_path} has no callable '{self.func_name}'")

        self._cache[key] = func

        # Evict
        while len(self._cache) > self.maxsize:
            self._cache.popitem(last=False)

        return func


# ═══════════════════════════════════════════
# КЭШИРОВАНИЕ JSON
# ═══════════════════════════════════════════

_json_cache: Dict[Tuple[str, float], Any] = {}


def load_json_cached(path: str) -> Optional[Dict]:
    """Загружает JSON с кэшированием по mtime."""
    path = os.path.abspath(path)
    if not os.path.exists(path):
        return None

    mtime = os.path.getmtime(path)
    key = (path, mtime)

    if key in _json_cache:
        return _json_cache[key]

    for k in list(_json_cache.keys()):
        if k[0] == path:
            _json_cache.pop(k, None)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    _json_cache[key] = data
    return data


def load_json(path: str) -> Dict:
    """Загружает JSON без кэширования."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ═══════════════════════════════════════════
# ФАЙЛЫ ПРОГРЕССА И ФЛАГИ
# ═══════════════════════════════════════════

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
START_FILE = os.path.join(BASE_DIR, "..", "start_index.txt")
STOP_FILE = os.path.join(BASE_DIR, "..", "stop.flag")
GENIMG_FILE = os.path.join(BASE_DIR, "..", "genimg.flag")


def read_start_index() -> int:
    """Читает стартовый индекс из файла."""
    try:
        with open(START_FILE, "r") as f:
            s = f.read().strip()
        return int(s) if s else 0
    except Exception:
        return 0


def write_progress(i: int) -> None:
    """Записывает текущий прогресс в файл."""
    try:
        with open(START_FILE, "w") as f:
            f.write(str(i))
    except Exception:
        pass


# ═══════════════════════════════════════════
# ВЫБОР ИЗОБРАЖЕНИЙ
# ═══════════════════════════════════════════

def get_selected_images(cl, row, root_dir: str) -> List[str]:
    """
    Выбирает изображения для объявления (из подпапок или общей папки).

    Args:
        cl: ClientParams.
        row: Строка DataFrame (namedtuple из itertuples).
        root_dir: Корневая директория.

    Returns:
        Список относительных путей к выбранным изображениям.
    """
    input_path = os.path.join(root_dir, cl.name, "var", "img", getattr(row, "images_folder"))
    count = int(getattr(row, "count_img"))
    imgparam = load_json(os.path.join(root_dir, cl.name, "var", "img", getattr(row, "imgpar")))
    use_random = imgparam.get("randomimg", 1)

    numbered = all(
        os.path.isdir(os.path.join(input_path, str(i)))
        for i in range(1, count + 1)
    )

    selected: List[str] = []

    if numbered:
        for i in range(1, count + 1):
            subfolder = os.path.join(input_path, str(i))
            files = [f for f in os.listdir(subfolder) if os.path.isfile(os.path.join(subfolder, f))]
            if files:
                selected.append(os.path.join(str(i), random.choice(files)))

        selected = sorted(selected, key=natural_sort_key)

        # non-title обработка
        non_title_indices = []
        nt_val = getattr(row, "non-title", None) if hasattr(row, "non-title") else None
        if isinstance(nt_val, str) and nt_val.strip().startswith("["):
            try:
                raw = ast.literal_eval(nt_val)
                non_title_indices = [i - 1 for i in raw if isinstance(i, int) and 1 <= i <= len(selected)]
            except Exception:
                pass

        if non_title_indices:
            fixed = [selected[i] for i in non_title_indices]
            rest = [img for i, img in enumerate(selected) if i not in non_title_indices]
            random.shuffle(rest)
            selected = (rest + fixed)[:count]
    else:
        files = [f for f in os.listdir(input_path) if os.path.isfile(os.path.join(input_path, f))]
        if use_random:
            selected = random.sample(files, min(count, len(files)))
        else:
            selected = sorted(files)[:count]

    return selected


# ═══════════════════════════════════════════
# ОБРАБОТКА ОДНОГО ИЗОБРАЖЕНИЯ
# ═══════════════════════════════════════════

def process_image_row(
    original_folder: str,
    imagename: str,
    output_folder: str,
    nameimg: str,
    imgparam: Dict,
    txt_block: Optional[List[Dict]] = None,
    style_txt: Optional[Dict] = None,
    ind: int = 0,
    namecl: str = "svai",
) -> None:
    """
    Обрабатывает одно изображение: модификация + наложение текста + сохранение.

    Args:
        original_folder: Папка с оригинальными изображениями.
        imagename: Имя файла изображения.
        output_folder: Папка для результата (относительно ROOT_DIR_OUT).
        nameimg: Имя выходного файла.
        imgparam: Параметры обработки (rotate, size, crop, contrast).
        txt_block: Список блоков текста для наложения.
        style_txt: Стили текста.
        ind: Индекс (для отладки).
        namecl: Имя клиента.
    """
    if txt_block is None:
        txt_block = []
    if style_txt is None:
        style_txt = {}

    image_path = os.path.join(original_folder, imagename)

    with Image.open(image_path) as src:
        enhanced = apply_modifications(
            src,
            imgparam["rotate_params"],
            imgparam["size_params"],
            imgparam["crop_params"],
            imgparam["contrast_range"],
        )

        if txt_block:
            pos_x = style_txt.get("left", 100)
            pos_y = style_txt.get("top", 100)
            max_w = style_txt.get("width", 800)
            cntr = style_txt.get("center", 0)
            next_x, next_y = pos_x, pos_y

            for block in txt_block:
                text = block.get("text", "")
                tag = block.get("tag")
                tag_style = style_txt.get(tag, {})
                fsize = tag_style.get("font_size", 40)
                fp = tag_style.get("font_path")
                fp = f"{ROOT_DIR}/{namecl}/var/font/{fp}" if fp else f"{ROOT_DIR}/{namecl}/var/font/PTSans-Bold.ttf"
                fcolor = tuple(tag_style.get("color", (0, 0, 0)))

                if not text.strip():
                    bg = (255, 255, 255, 0)
                    outline = (255, 255, 255, 0)
                else:
                    bg = tuple(tag_style.get("background_color", (255, 255, 255, 0)))
                    outline = tuple(tag_style.get("outline_color", (255, 255, 255, 40)))

                if calc_text_width(text, fp, fsize) < max_w:
                    if cntr == 1:
                        next_x = int((enhanced.width - calc_text_width(text, fp, fsize)) / 2)
                    enhanced = add_text_to_image(enhanced, text, (next_x, next_y), fsize, fcolor, bg, outline, fp)
                    next_y += fsize + tag_style.get("margin", 0)
                else:
                    lines = wrap_text_to_width(text, fp, fsize, max_w)
                    for line in lines:
                        if cntr == 1:
                            next_x = int((enhanced.width - calc_text_width(line, fp, fsize)) / 2)
                        enhanced = add_text_to_image(enhanced, line, (next_x, next_y), fsize, fcolor, bg, outline, fp)
                        next_y += round(fsize * 1.65)
                    next_y += tag_style.get("margin", 0)

        output_path = os.path.join(ROOT_DIR_OUT, output_folder)
        os.makedirs(output_path, exist_ok=True)
        enhanced.save(os.path.join(output_path, nameimg), optimize=True)

    del enhanced
    gc.collect()


def load_columns_from_file(file_path: str) -> List[str]:
    """Загружает список колонок из файла."""
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def generate_text_array(file_path: str) -> List[List[Dict[str, str]]]:
    """
    Читает файл с текстовыми блоками для наложения на изображения.

    Формат: блоки разделены строками-числами.
    Каждая строка внутри блока: <tag>текст</tag>.

    Returns:
        Список блоков, каждый блок — список {tag, text}.
    """
    if not os.path.exists(file_path):
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    result: List[List[Dict]] = []
    current_block: List[Dict] = []
    syn_pattern = re.compile(r"\$sin\{(.*?)\}\$sin")

    for line in lines:
        line = line.strip()
        if line.isdigit():
            if current_block:
                result.append(current_block)
                current_block = []
        elif line:
            # Замена синонимов
            replaced = syn_pattern.sub(
                lambda m: random.choice(m.group(1).split(" || ")),
                line,
            )
            # Извлечение тега
            tag_match = re.match(r"<(\w+)>(.+?)</\1>", replaced)
            if tag_match:
                current_block.append({"tag": tag_match.group(1), "text": tag_match.group(2)})

    if current_block:
        result.append(current_block)

    return result


# ═══════════════════════════════════════════
# ОСНОВНОЙ ПАЙПЛАЙН ИЗОБРАЖЕНИЙ
# ═══════════════════════════════════════════

def create_and_process_img_url(
    cl,
    df: pd.DataFrame,
    root_dir: str,
    nofile: bool = False,
) -> pd.DataFrame:
    """
    Генерирует изображения и формирует столбец ImageUrls.

    Для каждой строки прайса:
    1. Выбирает оригинальные изображения (из подпапок или общей папки).
    2. Применяет модификации (rotate, crop, contrast).
    3. Накладывает текстовые блоки (если заданы).
    4. Если есть first_img JSON — запускает пользовательский скрипт.
    5. Формирует URL-строку вида "url1 | url2 | url3".

    Args:
        cl: ClientParams.
        df: DataFrame прайса.
        root_dir: Корневая директория.
        nofile: True = только ссылки без генерации файлов.

    Returns:
        DataFrame с заполненным ImageUrls.
    """
    num_img = 1
    has_first_img = "first_img" in df.columns
    loader = None
    done = 0
    getotkeda = read_start_index()
    date_prefix = datetime.now().strftime("%d%m%y")

    for index, row in enumerate(df.itertuples(index=False)):
        # Пропуск если ImageUrls уже заполнен
        if pd.notna(df.at[index, "ImageUrls"]) and str(df.at[index, "ImageUrls"]).strip():
            continue

        image_names: List[str] = []

        # Определяем: генерировать файлы или только ссылки
        if IS_LOCAL:
            check = index < 10
        else:
            if os.path.exists(GENIMG_FILE):
                check = index >= getotkeda
            else:
                check = False

        local_nofile = not check if not nofile else True

        address_to_append = f"{cl.address_to_append}/{cl.name}/img_{cl.name_csv}/{index}/"
        output_folder = f"{cl.name}/img_{cl.name_csv}/{index}/"

        if not local_nofile:
            output_path_cl = os.path.join(ROOT_DIR_OUT, output_folder)
            if os.path.exists(output_path_cl) and os.path.isdir(output_path_cl):
                for f in os.listdir(output_path_cl):
                    full = os.path.join(output_path_cl, f)
                    if os.path.isfile(full):
                        os.remove(full)

        # ── Основные изображения (count_img) ──
        if (
            "count_img" in df.columns
            and pd.notna(getattr(row, "count_img", None))
            and int(getattr(row, "count_img")) > 0
        ):
            input_path = f"{root_dir}/{cl.name}/var/img/{getattr(row, 'images_folder')}"
            cont_dop = int(getattr(row, "count_img"))
            imgparam = load_json(f"{root_dir}/{cl.name}/var/img/{getattr(row, 'imgpar')}")

            selected_images = get_selected_images(cl, row, root_dir)

            # Текстовые блоки для наложения
            txt_file = f"{root_dir}/{cl.name}/var/text/to_img_text_{cl.name_csv}.txt"
            text_blocks = generate_text_array(txt_file) if os.path.exists(txt_file) else []

            # Стили текста
            style_txt = None
            style_file_csv = f"{root_dir}/{cl.name}/var/img/style_txt_to_img_{cl.name_csv}.json"
            style_file_def = f"{root_dir}/{cl.name}/var/img/style_txt_to_img.json"
            for sf in (style_file_csv, style_file_def):
                if os.path.exists(sf):
                    with open(sf, "r") as f:
                        style_txt = json.load(f)
                    break

            for i in range(cont_dop):
                nameimg = f"{date_prefix}_{getattr(row, 'images_folder')}_{i + 1}_{num_img}.jpg"
                image_names.append(f"{address_to_append}/{nameimg}")

                block = text_blocks[i] if i < len(text_blocks) else []

                if not local_nofile:
                    process_image_row(
                        input_path, selected_images[i], output_folder, nameimg,
                        imgparam, block, style_txt, i, cl.name,
                    )
                num_img += 1

        # ── Дополнительные изображения (count_dop_img) ──
        if (
            "count_dop_img" in df.columns
            and pd.notna(getattr(row, "count_dop_img", None))
            and int(getattr(row, "count_dop_img")) > 0
        ):
            cont_dop = int(getattr(row, "count_dop_img"))
            imgparam = load_json(f"{root_dir}/{cl.name}/var/img/{getattr(row, 'imgpar')}")
            dop_path = f"{root_dir}/{cl.name}/var/img/{getattr(row, 'dop_images_folder')}"
            files = os.listdir(dop_path)
            use_random = imgparam.get("randomimg", 1)

            if use_random:
                sel = random.sample(files, min(cont_dop, len(files)))
            else:
                sel = sorted(files)[:cont_dop]

            for i in range(cont_dop):
                nameimg = f"{date_prefix}_{getattr(row, 'dop_images_folder')}_{i + 1}_{num_img}.jpg"
                image_names.append(f"{address_to_append}/{nameimg}")
                if not local_nofile:
                    process_image_row(dop_path, sel[i], output_folder, nameimg, imgparam)
                num_img += 1

        # ── First image (custom script) ──
        if (
            has_first_img
            and isinstance(getattr(row, "first_img", None), str)
            and getattr(row, "first_img", "").endswith(".json")
        ):
            imgparam = load_json(f"{root_dir}/{cl.name}/var/img/{getattr(row, 'first_img')}")

            # Сбор данных из колонок
            column_data = []
            if "list_col_to_frst_img" in imgparam and imgparam["list_col_to_frst_img"]:
                col_file = f"{root_dir}/{cl.name}/var/img/{imgparam['list_col_to_frst_img']}"
                if os.path.exists(col_file):
                    columns_to_use = load_columns_from_file(col_file)
                    for col in columns_to_use:
                        if col in df.columns:
                            try:
                                val = getattr(row, col)
                            except AttributeError:
                                val = None
                            column_data.append({"name": col, "value": val})

            if cl.name == "sborpk":
                new_name = f"{getattr(row, 'full_art')}_{index}.jpg".replace(" ", "_")
            else:
                new_name = f"1_{date_prefix}_{getattr(row, 'articul')}_{index}.jpg".replace(" ", "_")

            if not local_nofile:
                script_name = imgparam.get("script") or imgparam.get("script_path")
                if script_name:
                    if loader is None:
                        loader = ScriptLoader(maxsize=512)
                    script_path = (
                        script_name if os.path.isabs(script_name)
                        else os.path.join(root_dir, cl.name, "var", "img", script_name)
                    )
                    try:
                        exec_func = loader.get(script_path)
                        exec_func(
                            ROOT_DIR=root_dir,
                            ROOT_DIR_OUT=ROOT_DIR_OUT,
                            new_image_name=new_name,
                            cl=cl,
                            imgparam=imgparam,
                            column_data=column_data,
                            index_n=index,
                        )
                        done += 1
                    except Exception as e:
                        print_log(f"[WARN] first_img script error row {index}: {e}")

            # Вставка первой картинки в начало (кроме stroy/dezi)
            if cl.name not in ("stroy", "dezi"):
                image_names.insert(0, f"{address_to_append}/{new_name}")

        # ── Сохранение URL ──
        df.at[index, "ImageUrls"] = " | ".join(image_names)

        # ── Проверка стоп-флага ──
        if os.path.exists(STOP_FILE):
            write_progress(index)
            print_log("⛔ Стоп-флаг. Выходим.")
            return df

        write_progress(index)

        # Прогресс в консоль
        if IS_LOCAL and index % 25 == 0 and index > 0:
            pct = int(index / cl.num_ads * 100)
            sys.stdout.write(f"\r{index}/{pct}%")
            sys.stdout.flush()

    gc.collect()
    print_log("✅ Картинки сгенерированы")

    return df
