# mnogunik

Система автоматической генерации и управления объявлениями на Авито.

## Что делает

1. **Читает прайс** из Google Sheets / CSV / Excel
2. **Дублирует строки** по распределению городов
3. **Генерирует уникальные тексты** — синонимы, гипотезы, шаблоны
4. **Создаёт уникальные изображения** — ротация, кроп, текстовые оверлеи
5. **Строит XML** для загрузки на Авито (формат autoload v3)
6. **Собирает статистику** через API Авито
7. **Оптимизирует ставки** на основе аналитики

## Структура проекта

```
mnogunik/
├── mnogunik/               # Python-пакет
│   ├── __init__.py
│   ├── config.py           # Конфигурация (пути, среда)
│   ├── klass.py            # ClientParams — модель данных
│   ├── go.py               # Точка входа (оркестратор)
│   ├── core/               # Ядро генерации
│   │   ├── text.py         # Обработка текстов (синонимы, шаблоны)
│   │   ├── dates.py        # Генерация дат публикации
│   │   ├── ids.py          # Генерация уникальных ID
│   │   ├── addresses.py    # Обработка адресов
│   │   ├── prices.py       # Дублирование строк, города
│   │   ├── images.py       # Генерация изображений
│   │   ├── export.py       # HTML-превью, CSV merge
│   │   └── params_reader.py# Чтение параметров из CSV
│   ├── xml/
│   │   └── builder.py      # Построение Avito XML
│   ├── stats/
│   │   └── statfun.py      # Статистика и аналитика
│   └── utils/
│       ├── logging.py      # Единый логгер
│       ├── google_sheets.py# Загрузка из Google Sheets
│       └── helpers.py      # Мелкие утилиты
├── tests/
│   └── test_core.py        # Тесты
├── requirements.txt
├── .env.example
└── .gitignore
```

## Установка

```bash
# Клонируем
git clone <repo-url>
cd mnogunik

# Виртуальное окружение
python3 -m venv .venv
source .venv/bin/activate

# Зависимости
pip install -r requirements.txt

# Конфигурация
cp .env.example .env
# Отредактировать .env под свою среду
```

## Запуск

```bash
# Основной скрипт генерации
python -m mnogunik.go

# Тесты
python -m pytest tests/ -v
```

## Конфигурация

Переменные окружения (`.env`):

| Переменная | Описание | По умолчанию |
|-----------|----------|-------------|
| `MNOGUNIK_ROOT_DIR` | Корневая папка проекта | Автодетект по hostname |
| `MNOGUNIK_ROOT_DIR_OUT` | Папка для выходных файлов | `ROOT_DIR/outfile` |
| `MNOGUNIK_ROOT_URL_OUT` | URL для ссылок на картинки | `http://mnogunik.ru/outfile` |
| `MNOGUNIK_WEB_KEY` | Пароль для PHP-эндпоинтов | `super123Lisa` |

## Технологии

- Python 3.8+
- pandas, Pillow, requests
- SQLite (статистика)
- PHP + nginx (веб-интерфейс)
- Avito API v2

## Статус

Активная разработка. v2.0 — рефакторинг кодовой базы.
