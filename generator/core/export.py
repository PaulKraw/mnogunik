"""
core/export.py — Экспорт данных: HTML-превью, слияние CSV, WordPress.
"""

import os
from typing import Optional

import pandas as pd


def merge_csv_files(
    csv_file1: str, csv_file2: str, output_path: str
) -> None:
    """
    Объединяет два CSV-файла по общим столбцам (outer join).

    Args:
        csv_file1: Путь к первому CSV.
        csv_file2: Путь ко второму CSV.
        output_path: Путь для сохранения результата.
    """
    df1 = pd.read_csv(csv_file1, dtype=str)
    df2 = pd.read_csv(csv_file2, dtype=str)
    common = df1.columns.intersection(df2.columns).tolist()
    merged = pd.merge(df1, df2, on=common, how="outer")
    merged.to_csv(output_path, index=False)


def clean_merged_data(file_path: str) -> None:
    """
    Очищает объединённый файл: удаляет AvitoStatus, пустые колонки.

    Args:
        file_path: Путь к CSV-файлу.
    """
    df = pd.read_csv(file_path, dtype=str)

    if "AvitoStatus" in df.columns:
        df.drop(columns=["AvitoStatus"], inplace=True)

    df.dropna(axis=1, how="all", inplace=True)

    if "Availability" in df.columns:
        df["Availability"] = df["Availability"].fillna("В наличии")

    df.to_csv(file_path, index=False)


def generate_html_from_df(df: pd.DataFrame, output_path: str, titlefile: str) -> None:
    """
    Генерирует HTML-страницу для визуальной проверки объявлений.

    Выбирает до 100 случайных строк и отображает их с картинками.

    Args:
        df: DataFrame прайса.
        output_path: Путь для сохранения HTML.
        titlefile: Файл с заголовком страницы.
    """
    sample = df.sample(n=min(100, len(df)), random_state=1) if len(df) > 100 else df

    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes">
<title>""" + titlefile + """ preview</title>
<style>
* { box-sizing: border-box; }

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    margin: 0;
    background: #f5f5f5;
    padding: 20px;
    color: #222;
    font-size: 16px; /* вместо браузерного умолчания */
}

/* ==== глобальная карусель (sticky сверху) ==== */
.global-carousel {
    position: sticky;
    top: 10px;
    z-index: 100;
    background: #fff;
    padding: 12px 20px;
    border-radius: 12px;
    margin-bottom: 20px;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 20px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.1);
}

.carousel-btn {
    background: #0af;
    color: #fff;
    border: none;
    padding: 10px 18px;
    border-radius: 8px;
    cursor: pointer;
    font-weight: 700;
    font-size: 16px;
    transition: background 0.15s;
}

.carousel-btn:hover { background: #08c; }
.carousel-btn:active { transform: scale(0.96); }

.carousel-counter {
    font-size: 15px;
    color: #333;
    font-weight: 600;
    min-width: 60px;
    text-align: center;
}

/* ==== грид объявлений ==== */
.grid {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    justify-content: flex-start;
}

/* ==== карточка объявления ==== */
.row {
    flex: 1 1 420px;
    max-width: 520px;
    background: #fff;
    border-radius: 12px;
    padding: 0;
    box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    transition: transform 0.15s, box-shadow 0.15s;
}

.row:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(0,0,0,0.12);
}

/* ==== блок картинки ==== */
.image-box {
    width: 100%;
    aspect-ratio: 4 / 4;
    background: #eee;
    overflow: hidden;
    position: relative;
}

.image-box img {
    width: 100%;
    height: 100%;
    object-fit: cover;
    cursor: zoom-in;
    display: block;
}

.image-box .img-badge {
    position: absolute;
    top: 8px;
    right: 8px;
    background: rgba(0,0,0,0.65);
    color: #fff;
    font-size: 12px;
    padding: 4px 8px;
    border-radius: 6px;
    font-weight: 600;
}

.image-box.broken {
    display: flex;
    align-items: center;
    justify-content: center;
    color: #c00;
    font-size: 14px;
    font-weight: 600;
}

/* ==== контент карточки ==== */
.row-body {
    padding: 14px 16px 16px;
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
}

.row-min {
    display: flex;
    flex-direction: column;
    gap: 2px;
}

.column-title {
    font-size: 11px;
    color: #999;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}

.column-value {
    font-size: 14px;
    color: #222;
    line-height: 1.4;
    word-wrap: break-word;
}

/* заголовок */

.row-min.title {
    flex: 1 1 100%;
}
.row-min.title .column-title { display: none; }
.row-min.title .column-value {
    font-size: 19px;
    font-weight: 700;
    line-height: 1.3;
    color: #111;
}

/* цена */
.row-min.price .column-title { display: none; }
.row-min.price .column-value {
    font-size: 22px;
    font-weight: 800;
    color: #111;
}

/* описание */
.column-value p { margin: 4px 0; }
.column-value ul { padding-left: 18px; margin: 4px 0; }

/* тех. поля — мельче, отделены */
.row-min.meta {
    border-top: 1px solid #f0f0f0;
    padding-top: 8px;
    flex: 1 1 20%;
}

.row-min.meta.Description {
    flex: 1 1 100%;
}


.row-min.meta .column-value {
    font-size: 13px;
    color: #555;
}

/* адаптив */
@media (max-width: 900px) {
    body { padding: 10px; }
    .row { flex: 1 1 100%; max-width: 100%; }
}


/* Адаптив для телефонов (ширина до 768px) */
@media (max-width: 768px) {
    body {
        padding: 8px;
        font-size: 16px;
    }

    .row {
        border-radius: 16px;
        margin-bottom: 12px;
    }

    .row-body {
        padding: 12px 14px 16px;
        gap: 12px;
    }

    /* Заголовок объявления */
    .row-min.title .column-value {
        font-size: 22px;
        line-height: 1.3;
    }

    /* Цена */
    .row-min.price .column-value {
        font-size: 26px;
    }

    /* Обычные поля (мета, описание) */
    .column-value {
        font-size: 16px;
        line-height: 1.45;
    }

    /* Мелкие мета-поля (автор, дата и т.п.) */
    .row-min.meta .column-value {
        font-size: 14px;
    }

    /* Бейдж с номером картинки – чуть крупнее */
    .image-box .img-badge {
        font-size: 14px;
        padding: 5px 10px;
        top: 10px;
        right: 10px;
    }
}

/* Для совсем узких экранов (до 480px) */
@media (max-width: 480px) {
    .row-min.title .column-value {
        font-size: 20px;
    }
    .row-min.price .column-value {
        font-size: 24px;
    }
    .carousel-btn {
        padding: 10px 16px;
        font-size: 16px;
        min-width: auto;
    }
    .global-carousel {
        gap: 8px;
    }
}

</style>
</head>
<body>

<div class="global-carousel">
    <button class="carousel-btn" onclick="prevImage()">← Назад</button>
    <div class="carousel-counter" id="carouselCounter">1 / 1</div>
    <button class="carousel-btn" onclick="nextImage()">Вперёд →</button>
</div>

<div class="grid">
"""

    # поля, которые считаем "ключевыми" (показываются крупно)
    KEY_FIELDS = {"Title", "Price"}

    for _, row in sample.iterrows():
        # сначала находим картинки, чтобы вывести их вверху карточки
        image_urls = []
        for col in df.columns:
            val = row[col]
            if isinstance(val, str) and "http" in val and ".jpg" in val.lower():
                image_urls = [l.strip() for l in val.split("|") if l.strip().lower().endswith(".jpg")]
                if image_urls:
                    break

        html += '<div class="row">\n'

        # блок с картинкой (один <img>, все урлы в data-images)
        if image_urls:
            data_attr = "|".join(image_urls)
            html += (
                '<div class="image-box">'
                f'<img class="preview-image" data-images="{data_attr}" '
                f'src="{image_urls[0]}" '
                'onerror="this.parentElement.classList.add(\'broken\');this.style.display=\'none\';this.parentElement.innerHTML=\'⚠ картинка не загрузилась\';">'
                f'<div class="img-badge"><span class="img-current">1</span>/{len(image_urls)}</div>'
                '</div>\n'
            )
        else:
            html += '<div class="image-box broken">нет картинки</div>\n'

        # тело карточки
        html += '<div class="row-body">\n'

        # сначала Title и Price
        for key_col in ["Title", "Price"]:
            if key_col in df.columns:
                val = row[key_col]
                if pd.isna(val) or val == "":
                    continue
                cls = " title" if key_col == "Title" else " price"
                html += f'<div class="row-min{cls}">'
                html += f'<div class="column-title">{key_col}:</div>'
                html += f'<div class="column-value">{val}</div>'
                html += '</div>\n'

        # потом остальные поля (кроме картинок и уже выведенных)
        for col in df.columns:
            if col in KEY_FIELDS:
                continue
            val = row[col]
            if pd.isna(val) or val == "":
                continue
            # пропускаем поле с картинками
            if isinstance(val, str) and "http" in val and ".jpg" in val.lower():
                continue

            html += f'<div class="row-min meta {col}">'
            html += f'<div class="column-title">{col}</div>'
            html += f'<div class="column-value">{val}</div>'
            html += '</div>\n'

        html += '</div>\n'  # /row-body
        html += '</div>\n'  # /row

    html += """
</div>

<script>
let currentIndex = 0;
const images = document.querySelectorAll('.preview-image');

let maxImages = 1;
images.forEach(img => {
    const arr = (img.dataset.images || '').split('|').filter(Boolean);
    if (arr.length > maxImages) maxImages = arr.length;
});

function updateImages() {
    images.forEach(img => {
        const arr = (img.dataset.images || '').split('|').filter(Boolean);
        if (!arr.length) return;
        const idx = currentIndex % arr.length;
        img.src = arr[idx];
        const badge = img.parentElement.querySelector('.img-current');
        if (badge) badge.textContent = (idx + 1);
    });
    document.getElementById('carouselCounter').textContent =
        ((currentIndex % maxImages) + 1) + ' / ' + maxImages;
}

function nextImage() {
    currentIndex = (currentIndex + 1) % maxImages;
    updateImages();
}

function prevImage() {
    currentIndex = (currentIndex - 1 + maxImages) % maxImages;
    updateImages();
}

/* клик по картинке → открыть оригинал */
images.forEach(img => {
    img.addEventListener('click', () => window.open(img.src, '_blank'));
});

/* стрелки клавиатуры */
document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowLeft') prevImage();
    if (e.key === 'ArrowRight') nextImage();
});

updateImages();
</script>
</body></html>"""

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def read_columns_to_delete(file_path: str) -> list:
    """Читает список столбцов для удаления из текстового файла."""
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def delete_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Удаляет указанные столбцы из DataFrame."""
    return df.drop(columns=[c for c in columns if c in df.columns])
