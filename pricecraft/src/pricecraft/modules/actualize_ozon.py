#!/usr/bin/env python3
# modules/actualize_ozon.py
import pandas as pd
import os, json, time, traceback
from datetime import datetime
import sys
import requests
from typing import List, Dict, Set

current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from config import sheets
from config import settings
import get_config

PROJECT_ROOT = settings.PROJECT_ROOT
RUNNERS_DIR = os.path.join(PROJECT_ROOT, 'runners')
LOG_FILE = os.path.join(RUNNERS_DIR, 'log.txt')
STATUS_FILE = os.path.join(RUNNERS_DIR, 'status.json')

# Используем те же ключи, что и в update_ozon_prices.py
HEADERS = {
    "Client-Id": "1456803",  # Ваш Client-Id из update_ozon_prices.py
    "Api-Key": "29a88933-3390-4bed-aaec-1220052c268d",  # Ваш Api-Key
    "Content-Type": "application/json"
}

def write_log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{ts} | {msg}\n")
    print(msg)

def write_status(status, message='', module='actualize_ozon'):
    data = {
        "module": module,
        "status": status,
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_active_ozon_articles() -> List[str]:
    """
    Получаем все активные (не архивные) артикулы с Ozon.
    Используем v3/product/list - это правильный эндпоинт.
    """
    url = "https://api-seller.ozon.ru/v3/product/list"
     
    all_articles = []
    last_product_id = ""
    
    write_log("📥 Начинаем получение товаров с Ozon...")
    cheki = 0
    while True:
        write_log(f"cheki {cheki}")
        cheki = cheki + 1

        # ВАЖНО: Ozon v3 API использует другой формат запроса
        payload = {
            "filter": {
                "visibility": "ALL"  # ALL = все товары (включая архивные)
            },
            "limit": 1000,
        }
        
        if last_product_id:
            payload["last_id"] = last_product_id
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, verify=False, timeout=30)
            
            if response.status_code != 200:
                write_log(f"❌ Ошибка API (код {response.status_code}): {response.text[:200]}")
                break
            
            data = response.json()
            
            if "result" not in data:
                write_log(f"❌ Некорректный ответ API: {data}")
                break
            
            items = data.get("result", {}).get("items", [])
            
            if not items:
                write_log("ℹ️ Получены все товары")
                break
            
            # Собираем только НЕ архивные товары
            for item in items:
                # Проверяем статус товара
                state = item.get("status", {}).get("state", "")
                is_archived = item.get("is_archived", False)
                
                # Берем только активные товары (не в архиве)
                if not is_archived and state not in ["failed", "rejected"]:
                    offer_id = item.get("offer_id", "")
                    if offer_id:
                        all_articles.append(offer_id)
            
            # Пагинация
            last_product_id = data.get("result", {}).get("last_id", "")
            
            write_log(f"📊 Получено: {len(all_articles)} активных товаров...")
            
            if not last_product_id:
                break
                
            time.sleep(0.3)  # Задержка для соблюдения rate limits
            
        except requests.exceptions.Timeout:
            write_log("⏱️ Таймаут запроса к Ozon API")
            break
        except Exception as e:
            write_log(f"❌ Ошибка запроса: {e}")
            break
    
    write_log(f"✅ Всего активных товаров на Ozon: {len(all_articles)}")
    return all_articles

def get_archived_ozon_articles() -> List[str]:
    """
    Получаем архивные товары для отладки (опционально)
    """
    url = "https://api-seller.ozon.ru/v3/product/info/status"
    
    archived_articles = []
    last_product_id = ""
    
    write_log("📥 Получаем архивные товары для проверки...")
    
    while True:
        payload = {
            "filter": {
                "visibility": "ALL"
            },
            "limit": 1000,
        }
        
        if last_product_id:
            payload["last_id"] = last_product_id
        
        response = requests.post(url, headers=HEADERS, json=payload, verify=False)
        
        if response.status_code != 200:
            break
            
        data = response.json()
        items = data.get("result", {}).get("items", [])
        
        if not items:
            break
        
        for item in items:
            if item.get("is_archived", False):
                offer_id = item.get("offer_id", "")
                if offer_id:
                    archived_articles.append(offer_id)
        
        last_product_id = data.get("result", {}).get("last_id", "")
        if not last_product_id:
            break
        
        time.sleep(0.3)
    
    write_log(f"📊 Архивных товаров на Ozon: {len(archived_articles)}")
    return archived_articles

def archive_products(articles: List[str]) -> Dict:
    """
    Архивируем товары на Ozon.
    Используем v2/product/archive.
    """
    if not articles:
        write_log("ℹ️ Нет товаров для архивации")
        return {"archived": 0, "errors": []}
    
    url = "https://api-seller.ozon.ru/v1/product/archive"
    
    BATCH_SIZE = 100  # Ozon рекомендует до 100 товаров за запрос
    archived = 0
    errors = []
    
    write_log(f"🗄️ Начинаем архивацию {len(articles)} товаров...")
    
    for i in range(0, len(articles), BATCH_SIZE):
        batch = articles[i:i + BATCH_SIZE]
        
        # ВАЖНО: v2/product/archive требует product_id, но часто работают с offer_id
        # Пробуем оба варианта, или используем v1/product/import-by-sku
        
        # Вариант 1: Если API принимает offer_id напрямую
        payload = {
            "product_id": batch  # или "offer_id": batch в зависимости от API
        }
        
        # Если не работает, попробуйте этот вариант:
        # payload = {"products": [{"offer_id": offer_id} for offer_id in batch]}
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, verify=False, timeout=30)
            
            if response.status_code == 200:
                archived += len(batch)
                write_log(f"✅ Пакет {i//BATCH_SIZE + 1}: архивировано {len(batch)} товаров")
            else:
                write_log(f"❌ Ошибка пакета {i//BATCH_SIZE + 1}: {response.text[:200]}")
                errors.extend(batch)
                
                # Попробуем альтернативный метод для этого пакета
                alt_result = archive_products_alternative(batch)
                if alt_result.get("archived", 0) > 0:
                    archived += alt_result["archived"]
                    write_log(f"🔄 Альтернативный метод: +{alt_result['archived']} товаров")
            
            time.sleep(1)  # Задержка между запросами
            
        except Exception as e:
            write_log(f"❌ Ошибка запроса архивации: {e}")
            errors.extend(batch)
    
    write_log(f"📊 Итог: успешно архивировано {archived} из {len(articles)} товаров")
    return {"archived": archived, "errors": errors}

def archive_products_alternative(articles: List[str]) -> Dict:
    """
    Альтернативный метод архивации через другой эндпоинт
    """
    if not articles:
        return {"archived": 0, "errors": []}
    
    url = "https://api-seller.ozon.ru/v1/product/archive"
    archived = 0
    
    for offer_id in articles:
        payload = {"product_id": [offer_id]}  # Некоторые API ждут массив
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, verify=False)
            if response.status_code == 200:
                archived += 1
            else:
                write_log(f"❌ Ошибка архивации {offer_id}: {response.text[:100]}")
        except Exception as e:
            write_log(f"❌ Ошибка запроса для {offer_id}: {e}")
        
        time.sleep(0.5)
    
    return {"archived": archived, "errors": []}

def get_our_articles() -> List[str]:
    """Получаем наши актуальные артикулы из конфигов"""
    try:
        # Используем тот же лист, что и для генерации Ozon файла
        client = sheets.get_client()
        sh = client.open_by_key(settings.SPREADSHEET_ID)
        ws = sh.worksheet("generate_ozon_file")
        
        data = ws.get_all_values()
        if len(data) < 2:  # Только заголовок
            write_log("⚠️ Таблица generate_ozon_file пуста или содержит только заголовки")
            return []
        
        df = get_config.init_conf_df(pd.DataFrame(data))
        
        # Проверяем наличие нужных колонок
        if 'Артикул*' in df.columns:
            articles = df['Артикул*'].dropna().astype(str).str.strip().tolist()
            write_log(f"📊 Найдено наших артикулов: {len(articles)}")
            return articles
        elif 'Артикул продавца' in df.columns:
            articles = df['Артикул продавца'].dropna().astype(str).str.strip().tolist()
            write_log(f"📊 Найдено артикулов (из 'Артикул продавца'): {len(articles)}")
            return articles
        else:
            write_log("⚠️ Не найден столбец с артикулами (искал 'Артикул*' и 'Артикул продавца')")
            write_log(f"Доступные колонки: {list(df.columns)}")
            return []
            
    except Exception as e:
        write_log(f"❌ Ошибка получения наших артикулов: {e}")
        return []

def compare_articles(ozon_articles: List[str], our_articles: List[str]) -> List[str]:
    """Сравниваем списки и находим лишние товары на Ozon"""
    # Преобразуем в множества для быстрого сравнения
    ozon_set = set(ozon_articles)
    our_set = set(our_articles)
    
    write_log(f"📊 Ozon товаров: {len(ozon_set)}, наших товаров: {len(our_set)}")
    
    # Что есть на Ozon, но нет у нас
    to_archive = list(ozon_set - our_set)
    
    # Что есть у нас, но нет на Ozon (новые товары)
    new_articles = list(our_set - ozon_set)
    
    write_log(f"📊 Найдено для архивации: {len(to_archive)} товаров")
    write_log(f"📊 Новых товаров (еще не на Ozon): {len(new_articles)}")
    
    if to_archive and len(to_archive) <= 20:
        write_log(f"📝 Примеры для архивации: {to_archive[:10]}")
    
    if new_articles and len(new_articles) <= 20:
        write_log(f"📝 Примеры новых товаров: {new_articles[:10]}")
    
    return to_archive

def save_report(our_count: int, ozon_count: int, archived_count: int, 
                archived_list: List[str], errors: List[str]):
    """Сохраняем отчет в Google Sheets"""
    try:
        client = sheets.get_client()
        sh = client.open_by_key(settings.SPREADSHEET_ID)
        
        # Пробуем найти или создать лист для отчета
        try:
            ws = sh.worksheet("ozon_actualize_report")
        except Exception:
            ws = sh.add_worksheet(title="ozon_actualize_report", rows=1000, cols=10)
        
        # Очищаем лист
        ws.clear()
        
        # Создаем DataFrame с основной статистикой
        stats_data = {
            "Дата актуализации": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            "Наших артикулов в базе": [our_count],
            "Товаров на Ozon (активных)": [ozon_count],
            "Отправлено в архив": [archived_count],
            "Ошибок архивации": [len(errors)],
            "Статус": ["✅ Успешно" if len(errors) == 0 else "⚠️ С ошибками"]
        }
        
        stats_df = pd.DataFrame(stats_data)
        
        # Подготавливаем список архивных товаров
        archived_df = pd.DataFrame({
            "Артикулы отправленные в архив": archived_list if archived_list else ["Нет товаров для архивации"]
        })
        
        # Подготавливаем список ошибок
        errors_df = pd.DataFrame({
            "Артикулы с ошибкой архивации": errors if errors else ["Ошибок нет"]
        })
        
        # Записываем статистику
        ws.update('A1', [stats_df.columns.tolist()], value_input_option="USER_ENTERED")
        ws.update('A2', stats_df.values.tolist(), value_input_option="USER_ENTERED")
        
        # Записываем архивные товары
        ws.update('A5', [archived_df.columns.tolist()], value_input_option="USER_ENTERED")
        ws.update('A6', archived_df.values.tolist(), value_input_option="USER_ENTERED")
        
        # Записываем ошибки
        start_row = len(archived_list) + 8 if archived_list else 8
        ws.update(f'A{start_row}', [errors_df.columns.tolist()], value_input_option="USER_ENTERED")
        ws.update(f'A{start_row + 1}', errors_df.values.tolist(), value_input_option="USER_ENTERED")
        
        write_log("📊 Отчет сохранен в Google Sheets (лист 'ozon_actualize_report')")
        
    except Exception as e:
        write_log(f"⚠️ Не удалось сохранить отчет: {e}")


# Сначала получаем активные offer_id с Ozon (у вас уже есть это в get_active_ozon_articles())
def get_active_ozon_articles_with_product_ids() -> Dict[str, int]:
    """
    Получаем ВСЕ товары с Ozon и собираем mapping: offer_id -> product_id
    Возвращает словарь {offer_id: product_id}
    """
    url = "https://api-seller.ozon.ru/v3/product/list"
    
    result = {}
    last_product_id = ""
    total_items_processed = 0

    
    write_log("📥 Начинаем получение товаров с Ozon (с product_id)...")
    
    while True:
        payload = {
            "filter": {"visibility": "ALL"},
            "limit": 1000,
        }
        
        if last_product_id:
            payload["last_id"] = last_product_id

        write_log(f"📤 Отправляем запрос с payload: {payload}")

        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, verify=False, timeout=30)
            
            write_log(f"📥 Получен ответ. Код: {response.status_code}")

            if response.status_code != 200:
                write_log(f"❌ Ошибка API: {response.status_code}")
                write_log(f"❌ Текст ошибки: {response.text[:500]}")
                break
            
            data = response.json()
# Детальный вывод структуры ответа для отладки
            write_log(f"📊 Структура ответа: {list(data.keys())}")
            
            if "result" not in data:
                write_log(f"⚠️ Нет ключа 'result' в ответе. Полный ответ: {json.dumps(data, ensure_ascii=False)[:500]}")
                break
            
            result_data = data.get("result", {})
            write_log(f"📊 Структура result: {list(result_data.keys())}")
            
            items = result_data.get("items", [])
            total = result_data.get("total", 0)
            last_id = result_data.get("last_id", "")
            
            write_log(f"📊 Получено items: {len(items)}, total: {total}, last_id: {last_id}")
            
            if not items:
                write_log("ℹ️ Нет товаров в ответе")
                break
            
            # Детальный вывод ПЕРВОГО товара для анализа структуры
            if total_items_processed == 0 and items:
                first_item = items[0]
                write_log("=" * 60)
                write_log("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ПЕРВОГО ТОВАРА:")
                write_log("=" * 60)
                write_log(f"Полная структура товара: {json.dumps(first_item, ensure_ascii=False, indent=2)}")
                write_log("=" * 60)
                
                write_log("📋 Ключи в объекте товара:")
                for key in first_item.keys():
                    value = first_item.get(key)
                    write_log(f"  '{key}': {type(value).__name__} = {str(value)[:100]}")
                
                # Проверяем различные возможные пути к ID
                possible_id_keys = ['id', 'product_id', 'item_id', 'sku_id', 'productID']
                for key in possible_id_keys:
                    if key in first_item:
                        write_log(f"  ⚡ Найден потенциальный ID в ключе '{key}': {first_item[key]}")
                
                # Проверяем offer_id
                possible_offer_keys = ['offer_id', 'offerId', 'offer', 'sku', 'article']
                for key in possible_offer_keys:
                    if key in first_item:
                        write_log(f"  ⚡ Найден потенциальный offer_id в ключе '{key}': {first_item[key]}")
                
                write_log("=" * 60)
            
            # Обрабатываем все товары
            active_count = 0
            archived_count = 0
            for idx, item in enumerate(items):
                total_items_processed += 1
                
                # Пробуем разные варианты ключей для offer_id
                offer_id = None
                offer_keys = ['offer_id', 'offerId', 'offer', 'sku']
                for key in offer_keys:
                    if key in item and item[key]:
                        offer_id = str(item[key]).strip()
                        break
                
                # Пробуем разные варианты ключей для product_id
                product_id = None
                product_keys = ['id', 'product_id', 'productId', 'item_id']
                for key in product_keys:
                    if key in item and item[key]:
                        try:
                            product_id = int(item[key])
                            break
                        except (ValueError, TypeError):
                            continue
                
                # Проверяем статус архивации
                is_archived = item.get("is_archived", False)
                state = item.get("status", {}).get("state", "")
                state_name = item.get("status", {}).get("state_name", "")
                
                # Логируем первый товар каждого пакета для отслеживания
                if idx == 0:
                    write_log(f"🔍 Пример товара: offer_id={offer_id}, product_id={product_id}, "
                             f"is_archived={is_archived}, state={state}")
                
                if offer_id and product_id and not is_archived:
                    result[offer_id] = product_id
                    active_count += 1
                    
                    # Логируем первые 5 товаров
                    if len(result) <= 5:
                        write_log(f"  ✅ Активный: {offer_id} → {product_id}")
                else:
                    archived_count += 1
                    if not offer_id or not product_id:
                        write_log(f"  ⚠️ Пропущен: нет offer_id или product_id")
                    elif is_archived:
                        write_log(f"  📦 Архивный: {offer_id} → {product_id}")
            
            write_log(f"📊 В этом пакете: активных {active_count}, архивных {archived_count}")
            write_log(f"📊 Всего собрано: {len(result)} активных товаров")
            
            # Пагинация
            last_product_id = last_id
            
            if not last_product_id:
                write_log("ℹ️ Достигнут конец списка товаров")
                break
                
            write_log(f"🔄 Переходим к следующей странице, last_id: {last_product_id}")
            time.sleep(0.5)
            
        except requests.exceptions.Timeout:
            write_log("⏱️ Таймаут запроса к Ozon API")
            break
        except json.JSONDecodeError as e:
            write_log(f"❌ Ошибка декодирования JSON: {e}")
            write_log(f"❌ Ответ сервера: {response.text[:500]}")
            break
        except Exception as e:
            write_log(f"❌ Неожиданная ошибка: {e}")
            write_log(f"❌ Traceback: {traceback.format_exc()}")
            break
    
    # Итоговый отчет
    write_log("=" * 60)
    write_log("📊 ИТОГОВЫЙ ОТЧЕТ:")
    write_log(f"✅ Всего обработано товаров: {total_items_processed}")
    write_log(f"✅ Активных товаров найдено: {len(result)}")
    write_log("=" * 60)
    
    # Выводим первые 10 записей для проверки
    if result:
        write_log("🔍 ПЕРВЫЕ 10 СООТВЕТСТВИЙ offer_id → product_id:")
        for i, (offer_id, product_id) in enumerate(list(result.items())[:10]):
            write_log(f"  {i+1:2d}. {offer_id:30} → {product_id}")
    
    # Сохраняем mapping в файл для отладки
    debug_file = os.path.join(RUNNERS_DIR, "ozon_mapping_debug.json")
    try:
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        write_log(f"💾 Отладочный файл сохранен: {debug_file}")
    except Exception as e:
        write_log(f"⚠️ Не удалось сохранить отладочный файл: {e}")
    
    return result


# Или альтернативный метод для получения product_id (если v3 не дает id):

def get_product_id_mapping(offer_ids: List[str]) -> Dict[str, int]:
    """
    Получаем product_id для списка offer_ids через v2/product/info/list
    """
    url = "https://api-seller.ozon.ru/v2/product/info/list"
    
    mapping = {}
    BATCH_SIZE = 100  # Максимальный размер пачки для Ozon
    
    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]
        
        payload = {
            "offer_id": batch,
            "product_id": [],
            "visibility": "ALL"
        }
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("result", {}).get("items", [])
                
                for item in items:
                    offer_id = item.get("offer_id", "")
                    product_id = item.get("id")
                    
                    if offer_id and product_id:
                        mapping[offer_id] = product_id
                        write_log(f"   🔗 {offer_id} → {product_id}")
                        
                write_log(f"📋 Пакет {i//BATCH_SIZE + 1}: получено {len(items)} ID")
                
            else:
                write_log(f"⚠️ Ошибка пакета {i//BATCH_SIZE + 1}: {response.text[:100]}")
                
        except Exception as e:
            write_log(f"⚠️ Исключение в пакете {i//BATCH_SIZE + 1}: {e}")
        
        time.sleep(0.5)
    
    write_log(f"✅ Получено product_id для {len(mapping)} из {len(offer_ids)} offer_id")
    return mapping


# Обновленная функция сравнения:
def compare_articles_and_get_product_ids(
    ozon_articles_mapping: Dict[str, int], 
    our_articles: List[str]
) -> List[int]:
    """
    Сравниваем списки и возвращаем product_id для архивации
    """

    ozon_offer_ids = set(ozon_articles_mapping.keys())
    our_offer_ids = set(our_articles)
    
    write_log(f"📊 Ozon товаров: {len(ozon_offer_ids)}, наших товаров: {len(our_offer_ids)}")
    
    # Что есть на Ozon, но нет у нас
    to_archive_offer_ids = list(ozon_offer_ids - our_offer_ids)
    
    # Получаем product_id для этих offer_id
    to_archive_product_ids = []
    
    for offer_id in to_archive_offer_ids:
        product_id = ozon_articles_mapping.get(offer_id)
        if product_id:
            to_archive_product_ids.append(product_id)
            # write_log(f"📝 Для архивации: {offer_id} → {product_id}")
        else:
            write_log(f"⚠️ Не найден product_id для {offer_id}")
    
    write_log(f"📊 Найдено для архивации: {len(to_archive_product_ids)} товаров (product_id)")
    
    return to_archive_product_ids

def debug_ozon_api_response():
    """
    Функция для отладки - получает один товар и показывает полную структуру ответа
    """
    url = "https://api-seller.ozon.ru/v3/product/list"
    
    payload = {
        "filter": {"visibility": "ALL"},
        "limit": 1,  # Только один товар для анализа
    }
    
    write_log("🔍 ЗАПУСК ОТЛАДКИ API OZON")
    write_log(f"URL: {url}")
    write_log(f"Headers: {HEADERS}")
    write_log(f"Payload: {payload}")
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload, verify=False, timeout=30)
        
        write_log(f"Статус код: {response.status_code}")
        write_log(f"Заголовки ответа: {dict(response.headers)}")
        
        # Сохраняем полный ответ в файл
        debug_file = os.path.join(RUNNERS_DIR, "ozon_api_debug.json")
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=2)
        
        write_log(f"💾 Полный ответ сохранен в: {debug_file}")
        
        # Анализируем структуру
        data = response.json()
        write_log("\n🔍 АНАЛИЗ СТРУКТУРЫ ОТВЕТА:")
        
        def analyze_structure(obj, indent=0, path=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    if isinstance(value, (dict, list)):
                        write_log(f"{'  ' * indent}{key}: {type(value).__name__}")
                        analyze_structure(value, indent + 1, new_path)
                    else:
                        write_log(f"{'  ' * indent}{key}: {type(value).__name__} = {str(value)[:50]}")
            elif isinstance(obj, list):
                if obj:
                    write_log(f"{'  ' * indent}[list] length: {len(obj)}")
                    if len(obj) > 0:
                        write_log(f"{'  ' * indent}Первый элемент:")
                        analyze_structure(obj[0], indent + 1, f"{path}[0]")
        
        analyze_structure(data)
        
    except Exception as e:
        write_log(f"❌ Ошибка при отладке API: {e}")
        write_log(f"❌ Traceback: {traceback.format_exc()}")

def archive_products_by_product_ids(product_ids: List[int]) -> Dict:
    """
    Архивируем товары на Ozon по их числовым product_id.
    """
    if not product_ids:
        return {"archived": 0, "errors": []}
    
    url = "https://api-seller.ozon.ru/v1/product/archive"
    
    BATCH_SIZE = 100
    archived = 0
    errors = []
    
    for i in range(0, len(product_ids), BATCH_SIZE):
        batch = product_ids[i:i + BATCH_SIZE]
        
        # Проверяем, что все элементы - числа
        for pid in batch:
            if not isinstance(pid, (int, float)):
                write_log(f"⚠️ Предупреждение: product_id {pid} не число (тип: {type(pid)})")
        
        payload = {
            "product_id": batch
        }
        
        write_log(f"📦 Пакет {i//BATCH_SIZE + 1}: отправляем {len(batch)} product_id")
        write_log(f"   Пример: {batch[:3]}...")
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            
            if response.status_code == 200:
                archived += len(batch)
                write_log(f"✅ Пакет {i//BATCH_SIZE + 1}: архивировано {len(batch)} товаров")
            else:
                error_msg = response.text[:200]
                write_log(f"❌ Ошибка пакета {i//BATCH_SIZE + 1}: {error_msg}")
                errors.extend([str(pid) for pid in batch])
            
            time.sleep(1)
            
        except Exception as e:
            write_log(f"❌ Ошибка запроса архивации: {e}")
            errors.extend([str(pid) for pid in batch])
    
    return {"archived": archived, "errors": errors}

def main():
    try:
        write_log("=" * 60)
        write_log("🔄 ЗАПУСК АКТУАЛИЗАЦИИ OZON")
        write_log("=" * 60)
        
        write_status("running", "Начинаем актуализацию...")
        
        # 1. Получаем наши артикулы
        write_log("\n📥 Шаг 1: Получаем наши артикулы...")
        our_articles = get_our_articles()
        
        if not our_articles:
            write_log("❌ Нет наших артикулов для сравнения!")
            write_status("error", "Нет наших артикулов в таблице")
            return
        
# 2. Получаем товары с Ozon с product_id
        write_log("\n📥 Шаг 2: Получаем товары с Ozon (сопоставляем offer_id → product_id)...")
        ozon_mapping = get_active_ozon_articles_with_product_ids()
        
        if not ozon_mapping:
            write_log("⚠️ Не удалось получить товары с Ozon")
            ozon_mapping = {}
        
        # 3. Сравниваем и получаем product_id для архивации
        write_log("\n📊 Шаг 3: Сравниваем списки...")
        to_archive_product_ids = compare_articles_and_get_product_ids(ozon_mapping, our_articles)
        
        if not to_archive_product_ids:
            write_log("🎉 Нет лишних товаров для архивации!")
            write_status("finished", "Нет товаров для архивации")
            save_report(
                our_count=len(our_articles),
                ozon_count=len(ozon_mapping),
                archived_count=0,
                archived_list=[],
                errors=[]
            )
            return
        
        # 4. Архивируем по product_id
        write_log(f"\n⚠️ Найдено {len(to_archive_product_ids)} товаров для архивации")
        write_log("Начинаем архивацию через 3 секунды...")
        time.sleep(3)
        
        write_log("\n🗄️ Шаг 4: Архивируем товары по product_id...")
        write_status("running", f"Архивация {len(to_archive_product_ids)} товаров...")
        
        archive_result = archive_products_by_product_ids(to_archive_product_ids)
        
        # 5. Сохраняем отчет
        write_log("\n📊 Шаг 5: Формируем отчет...")
        
        # Получаем offer_id для отчета
        to_archive_offer_ids = []
        for offer_id, product_id in ozon_mapping.items():
            if product_id in to_archive_product_ids:
                to_archive_offer_ids.append(offer_id)
        
        save_report(
            our_count=len(our_articles),
            ozon_count=len(ozon_mapping),
            archived_count=archive_result["archived"],
            archived_list=to_archive_offer_ids,
            errors=archive_result["errors"]
        )
        
        # 6. Итог
        write_log("\n" + "=" * 60)
        write_log("✅ АКТУАЛИЗАЦИЯ ЗАВЕРШЕНА")
        write_log(f"📊 Наших товаров: {len(our_articles)}")
        write_log(f"📊 Товаров на Ozon: {len(ozon_mapping)}")
        write_log(f"📊 Отправлено в архив: {archive_result['archived']}")
        write_log(f"📊 Ошибок архивации: {len(archive_result['errors'])}")
        write_log("=" * 60)
        
        if archive_result['errors']:
            write_status("finished", f"Архивировано {archive_result['archived']} товаров (с ошибками)")
        else:
            write_status("finished", f"Успешно архивировано {archive_result['archived']} товаров")
            






        # # 2. Получаем артикулы с Ozon
        # write_log("\n📥 Шаг 2: Получаем товары с Ozon...")
        # ozon_articles = get_active_ozon_articles()
        
        # if not ozon_articles:
        #     write_log("⚠️ Не удалось получить товары с Ozon")
        #     # Продолжаем, возможно на Ozon нет товаров
            
        # # 3. Сравниваем
        # write_log("\n📊 Шаг 3: Сравниваем списки...")
        # to_archive = compare_articles(ozon_articles, our_articles)
        
        # if not to_archive:
        #     write_log("🎉 Нет лишних товаров для архивации!")
        #     write_status("finished", "Нет товаров для архивации")
        #     save_report(
        #         our_count=len(our_articles),
        #         ozon_count=len(ozon_articles),
        #         archived_count=0,
        #         archived_list=[],
        #         errors=[]
        #     )
        #     return
        
        # # 4. Подтверждение (можно добавить позже)
        # write_log(f"\n⚠️ Найдено {len(to_archive)} товаров для архивации")
        # write_log("Начинаем архивацию через 3 секунды...")
        # time.sleep(3)
        
        # # 5. Архивируем
        # write_log("\n🗄️ Шаг 4: Архивируем товары...")
        # write_status("running", f"Архивация {len(to_archive)} товаров...")
        
        # archive_result = archive_products(to_archive)
        
        # # 6. Сохраняем отчет
        # write_log("\n📊 Шаг 5: Формируем отчет...")
        # save_report(
        #     our_count=len(our_articles),
        #     ozon_count=len(ozon_articles),
        #     archived_count=archive_result["archived"],
        #     archived_list=to_archive,
        #     errors=archive_result["errors"]
        # )
        
        # # 7. Итог
        # write_log("\n" + "=" * 60)
        # write_log("✅ АКТУАЛИЗАЦИЯ ЗАВЕРШЕНА")
        # write_log(f"📊 Наших товаров: {len(our_articles)}")
        # write_log(f"📊 Товаров на Ozon: {len(ozon_articles)}")
        # write_log(f"📊 Отправлено в архив: {archive_result['archived']}")
        # write_log(f"📊 Ошибок архивации: {len(archive_result['errors'])}")
        # write_log("=" * 60)
        
        # if archive_result['errors']:
        #     write_status("finished", f"Архивировано {archive_result['archived']} товаров (с ошибками)")
        # else:
        #     write_status("finished", f"Успешно архивировано {archive_result['archived']} товаров")
            
        get_config.finish("actualize_ozon")
        
    except Exception as e:
        tb = traceback.format_exc()
        write_log(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        write_log(tb)
        write_status("error", str(e)[:100], "actualize_ozon")
        
        # Сохраняем ошибку в лог
        error_log_path = os.path.join(RUNNERS_DIR, "actualize_error.log")
        with open(error_log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n{datetime.now()} - ERROR:\n")
            f.write(tb)
            f.write("\n" + "="*60 + "\n")

if __name__ == "__main__":
    main()