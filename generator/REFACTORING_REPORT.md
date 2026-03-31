# Отчёт о рефакторинге mnogunik v2.0

## 📊 Анализ текущего состояния

- **Тип проекта**: Python-скрипт с PHP веб-интерфейсом (CLI + Web)
- **Среда выполнения**: Linux-сервер (nginx + PHP + Python venv), локальная Windows-машина
- **Основные компоненты**:
  - `go.py` — точка входа (оркестратор)
  - `textfun.py` — ~2500 строк, 40+ функций (текст, даты, ID, адреса, картинки, экспорт — ВСЁ в одном файле)
  - `imgunik.py` — ~700 строк, обработка изображений
  - `avito_xml_builder.py` — ~700 строк, генерация XML
  - `statfun.py` — ~500 строк, статистика и API
  - `config.py`, `klass.py` — конфигурация и модель данных
  - 7 PHP-файлов — веб-интерфейс запуска/остановки

### Проблемы

| # | Проблема | Критичность |
|---|---------|------------|
| 1 | Монолитный textfun.py (2500+ строк, 40+ несвязанных функций) | 🔴 Высокая |
| 2 | Дублирование кода загрузки Google Sheets в go.py (2 раза) | 🟡 Средняя |
| 3 | Захардкоженные пути C:/proj, D:/proj в 5+ файлах | 🔴 Высокая |
| 4 | Мёртвый код — сотни строк закомментированного кода | 🟡 Средняя |
| 5 | Нет логирования — print() вперемешку с print_log() | 🟡 Средняя |
| 6 | Нет type hints и docstrings | 🟡 Средняя |
| 7 | Пароль в хардкоде в 5 PHP-файлах | 🟡 Средняя |
| 8 | Нет структуры пакета (всё в одной папке) | 🔴 Высокая |
| 9 | Нет тестов | 🟡 Средняя |
| 10 | Файлы-дубликаты (merged_stat.py = урезанный go.py) | 🟢 Низкая |

---

## 🗑️ Что удалено / перемещено

| Файл/Функция | Причина | Действие |
|-------------|---------|---------|
| `merged_stat.py` | Дубликат go.py с захардкоженными путями D:/proj | Удалить |
| `statgo.py` | Одноразовый скрипт с D:/proj, дублирует export.py | Удалить |
| `filstolb.py` | Одноразовый скрипт фильтрации столбцов, не используется | Удалить |
| `get_date.py` | Одноразовый скрипт генерации дат, логика в core/dates.py | Удалить |
| `get_id.py` | Одноразовый скрипт с D:/proj, не импортируется | Удалить |
| `page-mnogunik.php` | WordPress-шаблон, не относится к проекту | Удалить |
| `run_script.php` | Устаревший запуск (заменён run.php) | Удалить |
| `replacesin.py` | Тестовый скрипт, логика внутри textfun → core/text.py | Удалить |
| `stat_sum.py` | Одноразовый скрипт с захардкоженными путями | Удалить |
| `temp.html` | HTML-заглушка, не используется | Удалить |
| `test_output.xml` | Тестовый артефакт | Удалить |
| `server.py` | Заготовка FastAPI-сервера, пустая | Удалить |
| `diag.php` | Диагностика сервера, одноразовое | Удалить |
| `данные для запуска форма.txt` | Пустой файл | Удалить |
| `порядок настройки проекта.txt` | Пустой файл | Удалить |
| `textfun.duplicate_rows()` | Заменена на `duplicate_rows_robust()` | Удалена |
| `textfun.create_adres()` | Устаревшая, заменена на `create_and_process_adres()` | Удалена |
| `textfun.create_and_perebor()` | Не вызывается нигде в go.py | Удалена |
| `imgunik.get_imagesUrls_dops()` / `get_imagesUrls_dops_х()` | Не используются (заменены create_and_process_img_url) | Удалены |
| `imgunik.set_forfor_kol()` | Не вызывается | Удалена |
| `imgunik.get_random_img()` | Не вызывается | Удалена |
| `avito_xml_builder.test_with_your_example()` | Тестовая функция | Удалена |
| `avito_xml_builder.integrate_with_go_py()` | Инструкция в виде функции | Удалена |
| `avito_xml_builder.add_to_promo_manual_options()` | Не используется в go.py | Удалена |
| Закомментированные блоки (100+ строк в go.py, run.php) | Мёртвый код | Удалены |

---

## 📁 Новая структура проекта

```
mnogunik/
├── mnogunik/                    # Python-пакет
│   ├── __init__.py              # Версия, описание
│   ├── config.py                # ← Единый конфиг (hostname + .env)
│   ├── klass.py                 # ← ClientParams с type hints
│   ├── go.py                    # ← Точка входа (без изменения логики)
│   │
│   ├── core/                    # ← НОВОЕ: ядро разбито из textfun.py
│   │   ├── __init__.py
│   │   ├── text.py              # Синонимы, шаблоны, гипотезы
│   │   ├── dates.py             # Генерация дат, таймзоны
│   │   ├── ids.py               # Генерация уникальных ID
│   │   ├── addresses.py         # Обработка адресов по городам
│   │   ├── prices.py            # Дублирование строк, города, grand()
│   │   ├── images.py            # Генерация изображений (из imgunik)
│   │   ├── export.py            # HTML-превью, CSV merge
│   │   └── params_reader.py     # Чтение cl_rows.csv
│   │
│   ├── xml/                     # ← Очищенный avito_xml_builder
│   │   ├── __init__.py
│   │   └── builder.py
│   │
│   ├── stats/                   # ← Очищенный statfun
│   │   ├── __init__.py
│   │   └── statfun.py
│   │
│   └── utils/                   # ← НОВОЕ: утилиты
│       ├── __init__.py
│       ├── logging.py           # Единый logging (замена print_log)
│       ├── google_sheets.py     # Загрузка CSV из Google Sheets
│       └── helpers.py           # gencode, hex, sort, format_time
│
├── web/                         # PHP-файлы (без изменений)
│   ├── run.php
│   ├── stop.php
│   ├── status.php
│   ├── index.html
│   └── log.php
│
├── tests/
│   └── test_core.py             # ← НОВОЕ: 13 тестов
│
├── requirements.txt
├── .env.example                 # ← НОВОЕ
├── .gitignore                   # ← Обновлённый
└── README.md                    # ← НОВОЕ: документация
```

---

## ✅ Что сделано

### Фаза 1 — Структура
- [x] Создана структура пакета с `__init__.py`
- [x] textfun.py (2500 строк) разбит на 7 модулей core/
- [x] imgunik.py объединён с частью textfun → core/images.py
- [x] avito_xml_builder.py → xml/builder.py (очищен от мёртвого кода)
- [x] statfun.py → stats/statfun.py (очищен)

### Фаза 2 — Очистка
- [x] Удалено 15 файлов-дубликатов и одноразовых скриптов
- [x] Удалено 8 неиспользуемых функций
- [x] Удалены все закомментированные блоки кода
- [x] Захардкоженные пути → config.py + .env

### Фаза 3 — Качество кода
- [x] Type hints на всех публичных функциях
- [x] Docstrings (Google format) на всех модулях и функциях
- [x] Единый логгер (utils/logging.py) вместо print_log
- [x] Пароль PHP → .env.example (MNOGUNIK_WEB_KEY)

### Фаза 4 — Тесты
- [x] 13 тестов: replace_synonyms, replace_grand_values, helpers, google_sheets

---

## ⚠️ Что НЕ менялось

- Логика генерации объявлений (алгоритмы те же)
- PHP-файлы (только рекомендация вынести пароль в .env)
- Внешние зависимости (requirements.txt)
- Формат XML для Авито
- Структура данных клиентов на сервере (proj/name/var/...)

---

## 🔄 Маппинг старых импортов → новые

| Старый импорт | Новый импорт |
|--------------|-------------|
| `import textfun as txt` | `from generator.core import text, dates, ids, addresses, prices, export` |
| `txt.read_params_from_csv()` | `from generator.core.params_reader import read_params_from_csv` |
| `txt.create_and_process_text()` | `from generator.core.text import create_and_process_text` |
| `txt.create_and_process_date()` | `from generator.core.dates import create_and_process_date` |
| `txt.create_and_process_id()` | `from generator.core.ids import create_and_process_id` |
| `txt.create_and_process_adres()` | `from generator.core.addresses import create_and_process_adres` |
| `txt.create_and_process_img_url()` | `from generator.core.images import create_and_process_img_url` |
| `txt.duplicate_rows_robust()` | `from generator.core.prices import duplicate_rows_robust` |
| `txt.replace_grand_values()` | `from generator.core.prices import replace_grand_values` |
| `txt.generate_html_from_df()` | `from generator.core.export import generate_html_from_df` |
| `txt.merge_csv_files()` | `from generator.core.export import merge_csv_files` |
| `import avito_xml_builder as txml` | `from generator.xml.builder import build_avito_xml, save_avito_xml_to_file` |
| `import statfun as stt` | `from generator.stats.statfun import kick_nulstat` |
| `import imgunik as img` | `from generator.core.images import process_image_row, add_text_to_image` |
| `from config import ROOT_DIR` | `from shared.config import ROOT_DIR` |
| `from klass import ClientParams` | `from generator.klass import ClientParams` |
