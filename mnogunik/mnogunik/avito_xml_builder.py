# -*- coding: utf-8 -*-
"""
avito_xml_builder_fixed.py
Использует прямой подход для создания XML с CDATA
"""

import pandas as pd
import os
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import xml.etree.ElementTree as ET
from config import ROOT_DIR_OUT, ROOT_DIR, ROOT_URL_OUT, nout
def remove_empty_tags(xml_string):
    root = ET.fromstring(xml_string)

    for parent in root.iter():
        for child in list(parent):

            # ❗ НЕ трогаем Image
            if child.tag == 'Image':
                continue

            if (
                (child.text is None or child.text.strip() == "")
                and len(child) == 0
                and not child.attrib
            ):
                parent.remove(child)

    return ET.tostring(root, encoding="unicode")

# ============================================================================
# 1. КОНФИГУРАЦИЯ СООТВЕТСТВИЙ ПОЛЕЙ
# ============================================================================

FIELD_MAPPING = {
    'Id': 'Id',
    'DateBegin': 'DateBegin',
    'DateEnd': 'DateEnd',
    'ListingFee': 'ListingFee',
    'AdStatus': 'AdStatus',
    'AvitoId': 'AvitoId',
    'ManagerName': 'ManagerName',
    'ContactPhone': 'ContactPhone',
    'Address': 'Address',
    'Latitude': 'Latitude',
    'Longitude': 'Longitude',
    'SellerAddressID': 'SellerAddressID',
    'Title': 'Title',
    'Description': 'Description',
    'Price': 'Price',
    'ContactMethod': 'ContactMethod',
    'Promo': 'Promo',
    'PromoAutoOptions': None,
    'PromoManualOptions': None,
    'Category': 'Category',
    'PackagingType': 'PackagingType',
    'MinSaleQuantity': 'MinSaleQuantity',
    'PriceFor': 'PriceFor',
    'InternetCalls': 'InternetCalls',
    'CallsDevices': None,
    'Delivery': None,
    'WeightForDelivery': 'WeightForDelivery',
    'LengthForDelivery': 'LengthForDelivery',
    'HeightForDelivery': 'HeightForDelivery',
    'WidthForDelivery': 'WidthForDelivery',
    'ReturnPolicy': 'ReturnPolicy',
    'DeliverySubsidy': 'DeliverySubsidy',
    'GoodsType': 'GoodsType',
    'AdType': 'AdType',
    'Condition': 'Condition',
    'Availability': 'Availability',
    'FenceType': 'FenceType',
    'FenceSubType': 'FenceSubType',
    'MetalFenceType': 'MetalFenceType',
    'Height': 'Height',
    'SectionWidth': 'SectionWidth',
    'MetalThickness': 'MetalThickness',
    'LamellaWidth': 'LamellaWidth',
    'MetalProfileType': 'MetalProfileType',
    'Width': 'Width',
    'Color': None,
    'TargetAudience': 'TargetAudience',
    'ImageUrls': None,
    'ImageNames': None,
    'Material': 'Material',
    'StringerLength': 'StringerLength',
    'SectionLength': 'SectionLength',
    'CoveringMaterial': 'CoveringMaterial',
    'ProductType': 'ProductType',
    'RoofType': 'RoofType',
    'MeshType': 'MeshType',
    'RodThickness': 'RodThickness',
    'GateSubType': 'GateSubType',
    'SlidingGatesBrand': 'SlidingGatesBrand',
    'AutomaticGateControl': 'AutomaticGateControl',
    'Wicket': 'Wicket',
    'SlidingGateMaterial': 'SlidingGateMaterial',
    'GateAutomationBrand': 'GateAutomationBrand',
    'GateAutomationGateConstruction': 'GateAutomationGateConstruction',
    'GateAutomationType': 'GateAutomationType',
    'PileMaterial': 'PileMaterial',
    'PileType': 'PileType',
    'PurposeFor': 'PurposeFor',
    'SheetThickness': 'SheetThickness',
    'ProfileType': 'ProfileType',
    'RALColor': 'RALColor',
    'CustomOrder': 'CustomOrder',
    'Weight': 'Weight',
    'Diameter': 'Diameter',
    'Length': 'Length',
    'PipeWallThickness': 'PipeWallThickness',
    'GoodsSubType': 'GoodsSubType',
    'ServiceType': 'ServiceType',
    'ServiceSubtype': 'ServiceSubtype',
    'Specialty': 'Specialty',
    'FoundationWorks': None,
    'Foundation': 'Foundation',
    'Pile': 'Pile',
    'FreeMeasurementVisit': 'FreeMeasurementVisit',
    'WorkExperience': 'WorkExperience',
    'TeamSize': None,
    'WorkDays': None,
    'WorkTimeFrom': 'WorkTimeFrom',
    'WorkTimeTo': 'WorkTimeTo',
    'WorkWithContract': 'WorkWithContract',
    'Guarantee': 'Guarantee',
    'MinimumOrderAmount': 'MinimumOrderAmount',
    'MaterialPurchase': 'MaterialPurchase',
    'PriceList': None,

    
}

# Поля, требующие специальной обработки
SPECIAL_FIELDS = {
    'multiple_pipe': ['Color', 'Delivery', 'CallsDevices', 'WorkDays', 'FoundationWorks','TeamSize'],
    'images': ['ImageUrls', 'ImageNames'],
    'promo': ['PromoAutoOptions', 'PromoManualOptions'],
    'html_text': ['Description'],
    'priceist': ['PriceList'],
}

# ============================================================================
# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def load_cities_from_file(file_path: str) -> List[str]:
    """
    Загружает список городов из текстового файла.
    
    Формат файла:
    - Первая строка: заголовки "город", "число" (пропускается)
    - Последующие строки: название города, число (через запятую)
    
    Пример:
    город,число
    Санкт-Петербург,100
    "Кронштадт", 50
    Ломоносов,30
    """
    cities = []
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
        
        # Пропускаем первую строку с заголовками
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
                
            # Разбиваем по запятой
            parts = line.split(',')
            if not parts:
                continue
            
            # Первая часть - название города (очищаем от кавычек и пробелов)
            city_name = parts[0].strip().strip('"').strip("'").strip()
            
            # Добавляем город, если он не пустой
            if city_name:
                cities.append(city_name)
        
        # print(f"✅ Загружено {len(cities)} городов из файла: {file_path}")
        return cities
        
    except FileNotFoundError:
        print(f"⚠️ Файл не найден: {file_path}. Используется список по умолчанию.")
        return get_default_cities()
    except Exception as e:
        print(f"⚠️ Ошибка при чтении файла: {e}. Используется список по умолчанию.")
        return get_default_cities()


def get_default_cities() -> List[str]:
    """Возвращает список городов по умолчанию (на случай ошибки)"""
    return [
        "Санкт-Петербург",
        "Кронштадт",
        "Ломоносов",
        "Пушкин",
        "Сестрорецк"
    ]

# Глобальная переменная для хранения списка городов
CITIES_LIST = None

def set_cities_file(file_path: str):
    """Устанавливает файл с городами и загружает список"""
    global CITIES_LIST
    CITIES_LIST = load_cities_from_file(file_path)
    
def get_cities_list() -> List[str]:
    """Возвращает текущий список городов"""
    global CITIES_LIST
    if CITIES_LIST is None:
        CITIES_LIST = get_default_cities()
    return CITIES_LIST


def escape_xml_text(text: str) -> str:
    """Экранирует XML-специальные символы."""
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    # Экранируем основные XML-символы
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&apos;')
    
    return text


def needs_cdata(text: str) -> bool:
    """Проверяет, нужно ли оборачивать текст в CDATA."""
    if not isinstance(text, str):
        return False
    
    # Проверяем наличие разрешенных HTML-тегов Авито
    html_pattern = r'<(p|br|strong|em|ul|ol|li)(\s[^>]*)?>'
    if re.search(html_pattern, text, re.IGNORECASE):
        return True
    
    # Проверяем наличие незакрытых тегов или спецсимволов
    if re.search(r'<[^>]*$', text) or '&' in text or '<' in text or '>' in text:
        return True
    
    return False


def create_cdata_section(text: str) -> str:
    """Создает корректную секцию CDATA."""
    # В CDATA нельзя включать "]]>", нужно разбивать
    if "]]>" in text:
        parts = text.split("]]>")
        cdata_parts = []
        for part in parts:
            cdata_parts.append(f"<![CDATA[{part}]]>")
        return "".join(cdata_parts)
    else:
        return f"<![CDATA[{text}]]>"


def process_multiple_values(value: Any, separator: str = ' | ') -> List[str]:
    """Обрабатывает значения с разделителями."""
    if pd.isna(value) or value is None:
        return []
    
    value_str = str(value).strip()
    if not value_str:
        return []
    
    parts = [part.strip() for part in value_str.split(separator)]
    return [part for part in parts if part]


def process_images_field(value: Any, field_name: str) -> List[Dict[str, str]]:
    """Обрабатывает поля с изображениями."""
    if pd.isna(value) or value is None:
        return []
    
    value_str = str(value).strip()
    if not value_str:
        return []
    
    images = []
    image_parts = process_multiple_values(value_str, ' | ')
    
    for img in image_parts:
        img_dict = {}
        
        if field_name == 'ImageUrls':
            if img.startswith(('http://', 'https://')):
                img_dict['url'] = img
            else:
                img_dict['name'] = img
        elif field_name == 'ImageNames':
            img_dict['name'] = img
        
        if img_dict:
            images.append(img_dict)
    
    return images


def process_promo_auto_options(value: Any) -> List[Dict[str, str]]:
    """Обрабатывает PromoAutoOptions."""
    if pd.isna(value) or value is None:
        return []
    
    value_str = str(value).strip()
    if not value_str:
        return []
    
    items = []
    lines = [line.strip() for line in value_str.split('\n') if line.strip()]
    
    for line in lines:
        parts = [part.strip() for part in line.split('|')]
        
        item_dict = {}
        if len(parts) == 2:
            region, budget = parts
            if region:
                item_dict['Region'] = region
            if budget:
                item_dict['Budget'] = budget
        
        if item_dict:
            items.append(item_dict)
    
    return items


def process_promo_manual_options(value: Any) -> List[Dict[str, str]]:
    """Обрабатывает PromoManualOptions."""
    if pd.isna(value) or value is None:
        return []
    
    value_str = str(value).strip()
    if not value_str:
        return []
    
    items = []
    lines = [line.strip() for line in value_str.split('\n') if line.strip()]
    
    for line in lines:
        parts = [part.strip() for part in line.split('|')]
        
        item_dict = {}
        if len(parts) == 3:
            region, bid, limit = parts
            
            if region:
                item_dict['Region'] = region
            if bid:
                item_dict['Bid'] = bid
            if limit:
                item_dict['DailyLimit'] = limit
        
        if item_dict:
            items.append(item_dict)
    
    return items


# ============================================================================
# 3. ОСНОВНАЯ ФУНКЦИЯ СОЗДАНИЯ XML (ПРЯМОЙ ПОДХОД)
# ============================================================================
def extract_city_from_row(row: pd.Series) -> str:
    """
    Извлекает название города из строки DataFrame.
    Проверяет столбец 'город' (в разных вариантах написания).
    """
    # Возможные названия столбца с городом
    possible_columns = ['город', 'Город', 'CITY', 'city', 'города', 'Города']
    
    for col in possible_columns:
        if col in row.index and not pd.isna(row[col]):
            city = str(row[col]).strip()
            if city:
                return city
    
    return None

def create_xml_directly(dataframe: pd.DataFrame, params=None) -> str:
    """
    Создает XML строку напрямую, обходя проблемы с экранированием в ElementTree.
    """

    CITIES_FILE_PATH = f"{ROOT_DIR}/{params.name}/{params.k_gorod}"
    # Укажите путь к файлу с городами
    # CITIES_FILE_PATH = "cities.txt"  # или полный путь к файлу

    # Загружаем города при старте
    set_cities_file(CITIES_FILE_PATH)

    lines = []
    
    # XML декларация
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    
    # Открывающий тег Ads с атрибутами
    lines.append('<Ads formatVersion="3" target="Avito.ru">')
    
    # Для каждой строки DataFrame создаем элемент Ad
    for idx, row in dataframe.iterrows():
        try:
            ad_lines = []
            ad_lines.append('  <Ad>')


             # Получаем город из текущей строки (для добавления в промо)
            current_city = extract_city_from_row(row)
            # if current_city:
                # print(f"🏙️ Объявление {idx+1}: город из прайса - {current_city}")
            
            # 1. Простые поля
            for df_col, xml_tag in FIELD_MAPPING.items():
                if df_col not in row.index or xml_tag is None:
                    continue
                
                value = row[df_col]
                if pd.isna(value) or value is None:
                    continue
                
                value_str = str(value).strip()
                if not value_str:
                    continue
                
                # ОСОБАЯ ОБРАБОТКА ДЛЯ Description
                if df_col == 'Description':
                    if needs_cdata(value_str):
                        # Используем CDATA
                        cdata_content = create_cdata_section(value_str)
                        ad_lines.append(f'    <Description>{cdata_content}</Description>')
                    else:
                        # Экранируем обычный текст
                        escaped_text = escape_xml_text(value_str)
                        ad_lines.append(f'    <Description>{escaped_text}</Description>')
                else:
                    # Все остальные поля
                    escaped_text = escape_xml_text(value_str)
                    ad_lines.append(f'    <{xml_tag}>{escaped_text}</{xml_tag}>')

                
            
            # 2. Множественные значения (Color, Delivery, CallsDevices)
            for field in SPECIAL_FIELDS['multiple_pipe']:
                if field not in row.index:
                    continue
                
                value = row[field]
                if pd.isna(value) or value is None:
                    continue
                
                values_list = process_multiple_values(value)
                if not values_list:
                    continue
                
                xml_tag = FIELD_MAPPING.get(field, field)
                if xml_tag is None:
                    xml_tag = field
                
                ad_lines.append(f'    <{xml_tag}>')
                for val in values_list:
                    escaped_val = escape_xml_text(val)
                    ad_lines.append(f'      <Option>{escaped_val}</Option>')
                ad_lines.append(f'    </{xml_tag}>')
            
            # 3. Изображения
            for field in SPECIAL_FIELDS['images']:
                if field not in row.index:
                    continue
                
                value = row[field]
                if pd.isna(value) or value is None:
                    continue
                images = process_images_field(value, field)
                # print(images)

                if not images:
                    continue
                
                ad_lines.append('    <Images>')
                for img_dict in images:

                    if 'url' in img_dict:
                        escaped_url = escape_xml_text(img_dict['url'])
                        ad_lines.append(f'      <Image url="{escaped_url}" />')
                    elif 'name' in img_dict:
                        escaped_name = escape_xml_text(img_dict['name'])
                        ad_lines.append(f'      <Image name="{escaped_name}" />')
                ad_lines.append('    </Images>')
            
            # 4. Настройки продвижения
            # PromoAutoOptions
            if 'PromoAutoOptions' in row.index and not pd.isna(row['PromoAutoOptions']):
                promo_items = process_promo_auto_options(row['PromoAutoOptions'])
                if promo_items:
                    ad_lines.append('    <PromoAutoOptions>')
                    for item_dict in promo_items:
                        ad_lines.append('      <Item>')
                        for key, value in item_dict.items():
                            escaped_value = escape_xml_text(value)
                            ad_lines.append(f'        <{key}>{escaped_value}</{key}>')
                        ad_lines.append('      </Item>')
                    ad_lines.append('    </PromoAutoOptions>')
            

            # PromoManualOptions
            if 'PromoManualOptions' in row.index and not pd.isna(row['PromoManualOptions']):
                # Получаем список городов для добавления
                


                if row['Category']==  "Предложение услуг" :
                    promo_items = process_promo_manual_options_serv_add(row['PromoManualOptions'])

                    
                    
                else:
                    cities_to_process = get_cities_list().copy() 

                # Если есть город из прайса, добавляем его в список (если его там нет)
                    if current_city and current_city not in cities_to_process:
                        cities_to_process.append(current_city)
                        # print(f"➕ Добавлен город из прайса: {current_city}")


                    promo_items = process_promo_manual_options_add(
                        row['PromoManualOptions'], 
                        cities_to_process
                    )

                if promo_items:
                    ad_lines.append('    <PromoManualOptions>')
                    for item_dict in promo_items:
                        ad_lines.append('      <Item>')
                        for key, value in item_dict.items():
                            escaped_value = escape_xml_text(value)
                            ad_lines.append(f'        <{key}>{escaped_value}</{key}>')
                        ad_lines.append('      </Item>')
                    ad_lines.append('    </PromoManualOptions>')


            # PromoManualOptions
            if 'PriceList' in row.index and not pd.isna(row['PriceList']):
                # Получаем список городов для добавления
                
                
                pricelist_items = process_pricelist_add(row['PriceList'])

                if pricelist_items:
                    ad_lines.append('    <PriceList>')
                    for item_dict in pricelist_items:
                        ad_lines.append('      <Service>')
                        for key, value in item_dict.items():
                            escaped_value = escape_xml_text(value)
                            ad_lines.append(f'        <{key}>{escaped_value}</{key}>')
                        ad_lines.append('      </Service>')
                    ad_lines.append('    </PriceList>')


            
            ad_lines.append('  </Ad>')
            lines.extend(ad_lines)
            
        except Exception as e:
            print(f"⚠️ Ошибка при обработке объявления {idx + 1}: {e}")
            # Добавляем пустой Ad чтобы не сломать структуру
            lines.append('  <Ad>')
            lines.append(f'    <Id>error_{idx + 1}</Id>')
            lines.append('  </Ad>')
    
    # Закрывающий тег
    lines.append('</Ads>')
    
    return '\n'.join(lines)



def process_promo_manual_options_add(promo_str, cities_to_add=None):
    """
    Обрабатывает PromoManualOptions: дублирует существующие Item'ы для каждого города.
    
    Args:
        promo_str (str): Строка с существующими настройками промо
        cities_to_add (list): Список городов для добавления, если None - использует все города
    
    Returns:
        list: Список словарей с Item'ами для каждого города
    """
    
    
    items = []
    
    if pd.isna(promo_str) or not promo_str:
        # Если промо пустое, создаем базовый Item для каждого города
        for city in cities_to_add:
            items.append({
                'Region': city,
                'Bid': 10,  # значение по умолчанию
                'DailyLimit': 500  # значение по умолчанию
            })
    else:
        # Парсим существующие промо-настройки
        # Предполагаем формат: "Москва|500|5000;СПб|600|4000"
        existing_items = []
        
        # Разбиваем по точкам с запятой
        for item_str in str(promo_str).split(';'):
            if not item_str.strip():
                continue
                
            # Разбиваем по слешу
            parts = item_str.split('|')
            if len(parts) >= 3:
                city = parts[0].strip()
                try:
                    bid = int(parts[1].strip())
                except:
                    bid = 5
                try:
                    daily_limit = int(parts[2].strip())
                except:
                    daily_limit = 500




                existing_items.append({
                    'Region': city,
                    'Bid': bid,
                    'DailyLimit': daily_limit
                })


        
        if existing_items:
            # Используем bid и dailyLimit из первого существующего Item'а для всех городов
            first_item = existing_items[0]
            base_bid = first_item.get('Bid', 5)
            base_daily_limit = first_item.get('DailyLimit', 500)
            
            # Собираем все города (существующие + новые)
            all_cities_set = set()
            result_items = []
            
            # Сначала добавляем существующие
            for item in existing_items:
                city = item['Region']
                all_cities_set.add(city)
                result_items.append(item)
            
            # Затем добавляем новые города с теми же bid и dailyLimit
            for city in cities_to_add:
                if city not in all_cities_set:
                    result_items.append({
                        'Region': city,
                        'Bid': base_bid,
                        'DailyLimit': base_daily_limit
                    })
                    all_cities_set.add(city)
            
            items = result_items
        else:
            # Если не удалось распарсить, создаем для всех городов с дефолтными значениями
            for city in cities_to_add:
                items.append({
                    'Region': city,
                    'Bid': 10,
                    'DailyLimit': 500
                })
    
    return items



def process_promo_manual_options_serv_add(promo_str):
    """
    Обрабатывает PromoManualOptions: дублирует существующие Item'ы для каждого города.
    
    Args:
        promo_str (str): Строка с существующими настройками промо
        cities_to_add (list): Список городов для добавления, если None - использует все города
    
    Returns:
        list: Список словарей с Item'ами для каждого города
    """
    
    
    

    # Парсим существующие промо-настройки
    # Предполагаем формат: "Москва|500|5000;СПб|600|4000"
    existing_items = []

    # Разбиваем по слешу
    parts = promo_str.split('|')


    try:
        bid = int(parts[0].strip())
    except:
        bid = 5
    try:
        daily_limit = int(parts[1].strip())
    except:
        daily_limit = 500




    existing_items.append({
        'Bid': bid,
        'DailyLimit': daily_limit
    })


    
    
    return existing_items



def process_pricelist_add(pricelist):
    """
    Обрабатывает pricelist: дублирует существующие Item'ы для каждой услуги.
    
    Args:
        PriceList (str): Строка с существующими настройками промо
    
    Returns:
        list: Список словарей с Item'ами для каждой услуги
    """
    
    items = []
    
    if pd.isna(pricelist) or not pricelist:
        # Если pricelist пустое, создаем пустоту
        return ""
    else:
        # Парсим существующие промо-настройки
        # Предполагаем формат: "Москва|500|5000;СПб|600|4000"
        existing_items = []
        
        # Разбиваем по точкам с запятой
        for item_str in str(pricelist).splitlines():
            if not item_str.strip():
                continue
                
            # Разбиваем по слешу
            parts = item_str.split('|')

            # если больше 3 занч то труе и будет 1
            chsv = int(len(parts) > 4)
            chsv = 0 if (parts[1].strip()=="") else 1
                


            try:
                serviceprice = int(parts[2].strip())
            except:
                serviceprice = 500

            servicestartingprice = parts[3].strip()
            servicepricetype = parts[4].strip()

            if chsv == 1:
                existing_items.append({
                    'ServiceName': "Своя услуга",
                    'ServiceTitle': parts[1].strip(),
                    'ServicePrice': serviceprice,
                    'ServiceStartingPrice': servicestartingprice,
                    'ServicePriceType': servicepricetype
                })
            else:
                existing_items.append({
                    'ServiceName': parts[0].strip(),
                    'ServicePrice': serviceprice,
                    'ServiceStartingPrice': servicestartingprice,
                    'ServicePriceType': servicepricetype
                })




        
        if not existing_items:
            return ""
    
    return existing_items
# ============================================================================
# 4. ФУНКЦИИ ДЛЯ ИНТЕГРАЦИИ
# ============================================================================

def build_avito_xml(dataframe: pd.DataFrame, params=None) -> str:
    """
    Основная функция: преобразует DataFrame в XML для Авито.
    Использует прямой подход для правильной обработки CDATA.
    """
    return create_xml_directly(dataframe, params)


def save_avito_xml_to_file(dataframe: pd.DataFrame, params, 
                          base_output_path: str = None) -> str:
    """
    Сохраняет DataFrame как XML-файл для Авито.
    """
    if base_output_path is None:
        output_dir = getattr(params, 'output_dir', '.')
        xml_filename = f"{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.xml"
        output_path = f"{output_dir}/{xml_filename}"
    else:
        output_path = base_output_path.replace('.csv', '.xml')
    
    try:
        # Создаем директорию если не существует
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Генерируем XML
        xml_content = build_avito_xml(dataframe, params)

        clean_xml = remove_empty_tags(xml_content)
        
        # Сохраняем в файл
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(clean_xml)
        
        print(f"✅ XML-файл успешно сохранён: {output_path}")
        print(f"📊 Размер файла: {os.path.getsize(output_path)} байт")
        
        # Проверяем правильность CDATA
        check_cdata_in_file(output_path)
        
        return output_path
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении XML: {e}")
        raise


def check_cdata_in_file(file_path: str, max_lines: int = 20):
    """Проверяет наличие правильных CDATA секций в файле."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print("\n🔍 Проверка CDATA в файле:")
        
        cdata_count = 0
        for i, line in enumerate(lines[:max_lines]):
            if '<Description>' in line and 'CDATA' in line:
                cdata_count += 1
                print(f"  Строка {i+1}: Найдена CDATA секция")
                # Показываем фрагмент
                start = max(0, i-2)
                end = min(len(lines), i+3)
                print(f"    Контекст:")
                for j in range(start, end):
                    prefix = ">>> " if j == i else "    "
                    print(f"{prefix}{lines[j].rstrip()}")
        
        if cdata_count == 0:
            print("  ⚠️ CDATA секции не найдены")
            # Поиск любых Description
            for i, line in enumerate(lines[:max_lines]):
                if '<Description>' in line:
                    print(f"  Строка {i+1}: Description без CDATA: {line.strip()[:100]}...")
    
    except Exception as e:
        print(f"  ⚠️ Ошибка при проверке файла: {e}")


# ============================================================================
# 5. ТЕСТИРОВАНИЕ НА ВАШЕМ ПРИМЕРЕ
# ============================================================================

def test_with_your_example():
    """Тестирует на примере из вашего сообщения."""
    print("=" * 80)
    print("ТЕСТ НА ВАШЕМ ПРИМЕРЕ DESCRIPTION")
    print("=" * 80)
    
    # Ваш пример Description
    example_description = """<p>🎨 Практичный и красивый металлический штакетник — никаких забот.</p><p>М-форма, 1.8 м высота, двойное полимерное покрытие — служит долго, не выгорает и не гниёт.</p><p>Установка простая: без сварки и бетона.</p><p>Отличный вариант для дачи, двора или фасада.</p>    <p> Стоимость указана за  <strong> 1 штуку штакетник</strong>а </p> высотой 1,8м /  полимерное покрытие по двум сторонам  /  толщина 0,4 мм  /  ширина плашки 125 мм  /  М или П – обр. </p>  <p><strong>В наличии на складе</strong><br>     ✔️ Ширина: 125 мм<br> ✔️ Высота: 1,8 / 2 м<br> ✔️ Профиль: М / П-образный<br>  ✨ Красивый вид с любой стороны благодаря двухсторонней покраске<br>  ✔️ Толщина: 0,4 мм<br>  </p>  <p></p> <p>🎨 <strong>Цветовая палитра</strong><br>   ✅ RAL8017 — 🟫 коричневый<br> ✅ RAL6005 — 🟩 зелёный<br> ✅ RAL7024 — ⬛ графит<br>  ✅ RAL7004 — ⬛ серый<br>   ✅ RAL3005 — 🟪 красный<br>  </p>  <p></p> <p>🛠️<strong>Под заказ — от 3 дней</strong><br>  ✔️ Профиль П или М<br> ✔️ 0,45 мм по толщине <br> ✔️ Длина по запросу<br> ✔️ Под заказ — толщина 0,45 мм, закруглённый край<br> ✔️ Профиль округлой формы  <br> ✔️ На индивидуальную длину  <br> ✔️ Постоянно в наличии — быстрое изготовление<br>   ✔️ Длина по запросу<br>  ✔️ 0,45 мм по толщине<br> ✔️ По заданной длине<br>  ✔️ Профиль с полукруглой формой<br>  </p>   <p>-----------------------</p> <p>🚚 <strong>Как работаем</strong><br>  ✅ Самовывоз из поселка Тайцы <br>   🧾 Никаких авансов — всё честно <br>  🚛 Работаем с частными лицами и организациями.<br> 🤝 Помогаем с расчетами и погрузкой<br>   ✅ Быстрая доставка по СПб и Ленобласти <br>  </p>  <p></p>  <p><strong>➕ В наличии дополнительные товары </strong><br>  ✔️ Комплектующие: заглушки, фурнитура, крепления<br> ✔️ Сетка 3D Gitter<br> ✔️ Калитки, ворота (распашные, откатные)<br>  ✔️ Столбы, поперечины, винтовые сваи<br> ✔️ Профлист С8<br>   </p>   <p></p>  <p>📌 <strong>Почему с нами удобно и выгодно:</strong><br>   ⏱️ Отправка в срок — без промедлений<br>  🕒 Всегда готовы помочь — консультация без оплаты<br> ✅ Гарантии на все изделия<br> 💼 Подход, который учитывает все нюансы<br>    🔧 Всё делаем сами — вы не переплачиваете<br> ✅ Вся продукция на складе — без ожидания<br>  ✅ Индивидуальный заказ - от 3 дней<br>  </p>  <p>-----------------------</p>   <ul> <li>Протяжённость рейки: 163 - 169см</li> <li>Размер по ширине: в пределах 112-125мм</li> <li>Общая высота рейки: 16-19мм</li> <li>Расчетный период службы: 14-31мес</li> <li>Максимальная ширина ворот: от 380 до 480см</li> <li>В комплекте может быть разное количество штук: от 7 до 19шт</li> <li>Площадь покрытия изделия: 0.16 - 0.20кв/м</li> </ul>"""
    
    print(f"Длина описания: {len(example_description)} символов")
    print(f"Содержит HTML теги: {needs_cdata(example_description)}")
    
    # Создаем тестовый DataFrame
    test_data = {
        'Id': ['test_001'],
        'Title': ['Металлический штакетник евро'],
        'Description': [example_description],
        'Price': ['25000'],
        'ImageUrls': ['img1.jpg | img2.jpg'],
        'Color': ['Коричневый | Зеленый | Графит'],
        'Category': ['Ремонт и строительство'],
        'Condition': ['Новое'],
    }
    
    df = pd.DataFrame(test_data)
    
    class MockParams:
        name = 'test_client'
        name_csv = 'zabor'
        date_f = datetime.now().strftime('%Y%m%d')
        num_ads = 1
    
    params = MockParams()
    
    # Генерируем XML
    xml_content = build_avito_xml(df, params)
    
    # Сохраняем для проверки
    test_file = 'test_fixed_output.xml'
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    
    print(f"\n✅ Тестовый XML сохранен в: {test_file}")
    
    # Показываем фрагмент с Description
    print("\n📄 Фрагмент Description из XML:")
    lines = xml_content.split('\n')
    for line in lines:
        if '<Description>' in line:
            # Показываем начало и конец
            print(f"Начало: {line[:100]}...")
            # Ищем закрывающий тег в этой или следующих строках
            for i, l in enumerate(lines[lines.index(line):lines.index(line)+3]):
                if '</Description>' in l:
                    print(f"Конец: ...{l[-100:]}")
                    break
    
    return xml_content


# ============================================================================
# 6. ИНТЕГРАЦИЯ В GO.PY
# ============================================================================

def integrate_with_go_py():
    """
    Инструкция по интеграции в go.py:
    
    1. Импортируйте модуль в go.py:
       import avito_xml_builder_fixed as xml_builder
    
    2. Найдите в go.py строку сохранения CSV:
       output_file_path = f"{ROOT_DIR}/{params.name}/{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv"
       extended_price_df.to_csv(output_file_path, index=False)
    
    3. Добавьте ПОД НЕЙ создание XML:
       try:
           xml_path = xml_builder.save_avito_xml_to_file(
               extended_price_df, 
               params, 
               output_file_path  # Автоматически заменит .csv на .xml
           )
           print_log(f"✅ XML-файл создан: {xml_path}")
       except Exception as e:
           print_log(f"❌ Ошибка при создании XML: {e}")
    
    4. Удалите старый импорт avito_xml_builder если был
    """
    print("Инструкция по интеграции сохранена в функции integrate_with_go_py()")


# ============================================================================
# ЗАПУСК ТЕСТА
# ============================================================================

if __name__ == "__main__":
    print("Запуск теста модуля avito_xml_builder_fixed...")
    
    # Тест на вашем примере
    xml_result = test_with_your_example()
    
    # Проверяем результат
    if "<![CDATA[" in xml_result and "]]>" in xml_result:
        print("\n🎉 УСПЕХ! CDATA корректно обработаны!")
    else:
        print("\n⚠️ ВНИМАНИЕ: CDATA не найдены в результате!")
    
    print("\nГотово к интеграции в go.py")
    integrate_with_go_py()



def add_to_promo_manual_options(promo_data, cities_to_add, base_bid=500, daily_limit=5000):
    """
    Добавляет новые города в PromoManualOptions с проверкой на дублирование.
    
    Args:
        promo_data (list): Текущий список PromoManualOptions
        cities_to_add (list): Список городов для добавления
        base_bid (int): Ставка для новых записей
        daily_limit (int): Дневной лимит для новых записей
        
    Returns:
        list: Обновленный список PromoManualOptions
    """

    
    # Определяем список доступных городов (можно вынести в константу)
    # AVAILABLE_CITIES = [
    #     "Санкт-Петербург",
    #     "Кронштадт",
    #     "Ломоносов",
    #     "Пушкин",
    #     "Сестрорецк",
    #     "Красное Село",
    #     "Петергоф",
    #     "Колпино",
    #     "Кировск",
    #     "Шлиссельбург",
    #     "Янино-1",
    #     "Лесколово",
    #     "Пикалево",
    #     "Толмачево",
    #     "Щеглово",
    #     "Горбунки",
    #     "Тельмана",
    #     "Бокситогорск",
    #     "Волосово",
    #     "Гатчина",
    #     "Красный Бор",
    #     "Кудрово",
    #     "Морозова",
    #     "Сиверский",
    #     "Павлово",
    #     "Тайцы",
    #     "Мга",
    #     "Агалатово",
    #     "Коммунар",
    #     "Токсово",
    #     "Аннино",
    #     "Новое Девяткино",
    #     "Сосново",
    #     "Вырица",
    #     "Кингисепп",
    #     "Рощино",
    #     "Сосновый Бор",
    #     "Тосно",
    #     "Всеволожск",
    #     "Кузьмоловский",
    #     "Мурино",
    #     "Ульяновка",
    #     "Усть-Луга",
    #     "Гостилицы",
    #     "Кипень",
    #     "Сертолово",
    #     "Федоровское",
    #     "Свердлова",
    #     "Бугры",
    #     "Колтуши",
    #     "Романовка"
    # ]

    # Определяем список доступных городов (можно вынести в константу)
    AVAILABLE_CITIES = [
        "Санкт-Петербург",
        "Кронштадт",
        "Ломоносов",
        "Пушкин"
    ]
    
    # Если promo_data не существует, создаем пустой список
    if promo_data is None:
        promo_data = []
    
    # Собираем существующие города для проверки дублей
    existing_cities = set()
    for item in promo_data:
        if isinstance(item, dict) and 'Region' in item:
            existing_cities.add(item['Region'])
    
    # Определяем какие города добавлять
    cities_to_process = []
    if cities_to_add == "all":
        cities_to_process = AVAILABLE_CITIES
    elif isinstance(cities_to_add, list):
        for city_name in cities_to_add:
            if city_name in AVAILABLE_CITIES:
                cities_to_process.append(city_name)
            else:
                print(f"Город '{city_name}' не найден в списке доступных городов")
    
    # Добавляем новые города
    added_count = 0
    for city_name in cities_to_process:
        if city_name not in existing_cities:
            new_item = {
                'Region': city_name,
                'Bid': base_bid,
                'DailyLimit': daily_limit
            }
            promo_data.append(new_item)
            existing_cities.add(city_name)
            added_count += 1
            print(f"Добавлен город: {city_name}")
        else:
            print(f"Город '{city_name}' уже существует, пропускаем")
    
    print(f"Добавлено новых городов: {added_count}")
    print(f"Всего городов: {len(promo_data)}")
    
    return promo_data


# # Пример использования:
# if __name__ == "__main__":
#     # Пример данных (как они реально выглядят)
#     sample_promo = [
#         {'city': 'Москва', 'bid': 500, 'dailyLimit': 5000},
#         {'city': 'Санкт-Петербург', 'bid': 450, 'dailyLimit': 4000}
#     ]
    
#     # Добавляем новые города
#     updated = add_to_promo_manual_options(
#         sample_promo,
#         cities_to_add=["Москва", "Калуга", "Владимир"],  # Москва уже есть
#         base_bid=600,
#         daily_limit=8000
#     )
    
#     print("\nИтоговый список:")
#     for item in updated:
#         print(f"  {item}")