"""
xml/builder.py — Построение XML-файла для Авито из DataFrame.

Использует прямой подход (строковая конкатенация) вместо ElementTree
для корректной обработки CDATA-секций в Description.
"""

import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd
import xml.etree.ElementTree as ET

from shared.config import ROOT_DIR


# ═══════════════════════════════════════════
# МАППИНГ ПОЛЕЙ DataFrame → XML
# ═══════════════════════════════════════════

FIELD_MAPPING: Dict[str, Optional[str]] = {
    "Id": "Id", "DateBegin": "DateBegin", "DateEnd": "DateEnd",
    "ListingFee": "ListingFee", "AdStatus": "AdStatus", "AvitoId": "AvitoId",
    "ManagerName": "ManagerName", "ContactPhone": "ContactPhone",
    "Address": "Address", "Latitude": "Latitude", "Longitude": "Longitude",
    "SellerAddressID": "SellerAddressID", "Title": "Title",
    "Description": "Description", "Price": "Price",
    "ContactMethod": "ContactMethod", "Promo": "Promo",
    "PromoAutoOptions": None, "PromoManualOptions": None,
    "Category": "Category", "PackagingType": "PackagingType",
    "MinSaleQuantity": "MinSaleQuantity", "PriceFor": "PriceFor",
    "InternetCalls": "InternetCalls", "CallsDevices": None,
    "Delivery": None, "WeightForDelivery": "WeightForDelivery",
    "LengthForDelivery": "LengthForDelivery",
    "HeightForDelivery": "HeightForDelivery",
    "WidthForDelivery": "WidthForDelivery",
    "ReturnPolicy": "ReturnPolicy", "DeliverySubsidy": "DeliverySubsidy",
    "GoodsType": "GoodsType", "AdType": "AdType",
    "Condition": "Condition", "Availability": "Availability",
    "FenceType": "FenceType", "FenceSubType": "FenceSubType",
    "MetalFenceType": "MetalFenceType", "Height": "Height",
    "SectionWidth": "SectionWidth", "MetalThickness": "MetalThickness",
    "LamellaWidth": "LamellaWidth", "MetalProfileType": "MetalProfileType",
    "Width": "Width", "Color": None, "TargetAudience": "TargetAudience",
    "ImageUrls": None, "ImageNames": None,
    "Material": "Material", "StringerLength": "StringerLength",
    "SectionLength": "SectionLength", "CoveringMaterial": "CoveringMaterial",
    "ProductType": "ProductType", "RoofType": "RoofType",
    "MeshType": "MeshType", "RodThickness": "RodThickness",
    "GateSubType": "GateSubType", "SlidingGatesBrand": "SlidingGatesBrand",
    "AutomaticGateControl": "AutomaticGateControl", "Wicket": "Wicket",
    "SlidingGateMaterial": "SlidingGateMaterial",
    "GateAutomationBrand": "GateAutomationBrand",
    "GateAutomationGateConstruction": "GateAutomationGateConstruction",
    "GateAutomationType": "GateAutomationType",
    "PileMaterial": "PileMaterial", "PileType": "PileType",
    "PurposeFor": "PurposeFor", "SheetThickness": "SheetThickness",
    "ProfileType": "ProfileType", "RALColor": "RALColor",
    "CustomOrder": "CustomOrder", "Weight": "Weight",
    "Diameter": "Diameter", "Length": "Length",
    "PipeWallThickness": "PipeWallThickness",
    "GoodsSubType": "GoodsSubType", "ServiceType": "ServiceType",
    "ServiceSubtype": "ServiceSubtype", "Specialty": "Specialty",
    "FoundationWorks": None, "Foundation": "Foundation", "Pile": "Pile",
    "FreeMeasurementVisit": "FreeMeasurementVisit",
    "WorkExperience": "WorkExperience", "TeamSize": None,
    "WorkDays": None, "WorkTimeFrom": "WorkTimeFrom",
    "WorkTimeTo": "WorkTimeTo", "WorkWithContract": "WorkWithContract",
    "Guarantee": "Guarantee", "MinimumOrderAmount": "MinimumOrderAmount",
    "MaterialPurchase": "MaterialPurchase", "PriceList": None,
}

# Поля с множественными значениями (через " | ")
MULTI_VALUE_FIELDS = [
    "Color", "Delivery", "CallsDevices", "WorkDays", "FoundationWorks", "TeamSize"
]

IMAGE_FIELDS = ["ImageUrls", "ImageNames"]


# ═══════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════

def escape_xml(text: str) -> str:
    """Экранирует XML-спецсимволы."""
    if pd.isna(text) or text is None:
        return ""
    text = str(text)
    for old, new in [("&", "&amp;"), ("<", "&lt;"), (">", "&gt;"),
                     ('"', "&quot;"), ("'", "&apos;")]:
        text = text.replace(old, new)
    return text


def needs_cdata(text: str) -> bool:
    """Проверяет, нужна ли CDATA-обёртка для текста."""
    if not isinstance(text, str):
        return False
    if re.search(r"<(p|br|strong|em|ul|ol|li)(\s[^>]*)?>", text, re.IGNORECASE):
        return True
    return bool(re.search(r"<[^>]*$|[&<>]", text))


def make_cdata(text: str) -> str:
    """Создаёт CDATA-секцию, корректно обрабатывая ']]>'."""
    if "]]>" in text:
        parts = text.split("]]>")
        return "".join(f"<![CDATA[{p}]]>" for p in parts)
    return f"<![CDATA[{text}]]>"


def split_pipe(value: Any, sep: str = " | ") -> List[str]:
    """Разбивает значение по разделителю."""
    if pd.isna(value) or value is None:
        return []
    parts = [p.strip() for p in str(value).strip().split(sep)]
    return [p for p in parts if p]


def load_cities_from_file(file_path: str) -> List[str]:
    """Загружает список городов из CSV (город, число)."""
    cities: List[str] = []
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            for line in f.readlines()[1:]:  # пропуск заголовка
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                city = parts[0].strip().strip('"').strip("'").strip()
                if city:
                    cities.append(city)
    except Exception as e:
        print(f"⚠️ Ошибка чтения городов: {e}")
        cities = ["Санкт-Петербург", "Кронштадт", "Ломоносов", "Пушкин", "Сестрорецк"]
    return cities


def remove_empty_tags(xml_string: str) -> str:
    """Удаляет пустые теги из XML (кроме Image)."""
    root = ET.fromstring(xml_string)
    for parent in root.iter():
        for child in list(parent):
            if child.tag == "Image":
                continue
            if (child.text is None or child.text.strip() == "") and len(child) == 0 and not child.attrib:
                parent.remove(child)
    return ET.tostring(root, encoding="unicode")


# ═══════════════════════════════════════════
# PROMO / PRICELIST ОБРАБОТКА
# ═══════════════════════════════════════════

def process_promo_manual_options(promo_str: str, cities: List[str]) -> List[Dict]:
    """Парсит PromoManualOptions и дублирует для всех городов."""
    items: List[Dict] = []
    if pd.isna(promo_str) or not promo_str:
        for city in cities:
            items.append({"Region": city, "Bid": 10, "DailyLimit": 500})
        return items

    existing: List[Dict] = []
    for chunk in str(promo_str).split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split("|")
        if len(parts) >= 3:
            try:
                bid = int(parts[1].strip())
            except ValueError:
                bid = 5
            try:
                limit = int(parts[2].strip())
            except ValueError:
                limit = 500
            existing.append({"Region": parts[0].strip(), "Bid": bid, "DailyLimit": limit})

    if not existing:
        for city in cities:
            items.append({"Region": city, "Bid": 10, "DailyLimit": 500})
        return items

    base_bid = existing[0].get("Bid", 5)
    base_limit = existing[0].get("DailyLimit", 500)
    seen = set()
    result: List[Dict] = []

    for item in existing:
        seen.add(item["Region"])
        result.append(item)

    for city in cities:
        if city not in seen:
            result.append({"Region": city, "Bid": base_bid, "DailyLimit": base_limit})
            seen.add(city)

    return result


def process_promo_service(promo_str: str) -> List[Dict]:
    """Парсит PromoManualOptions для услуг (bid|limit)."""
    parts = str(promo_str).split("|")
    try:
        bid = int(parts[0].strip())
    except (ValueError, IndexError):
        bid = 5
    try:
        limit = int(parts[1].strip())
    except (ValueError, IndexError):
        limit = 500
    return [{"Bid": bid, "DailyLimit": limit}]


def process_pricelist(pricelist_str: str) -> List[Dict]:
    """Парсит PriceList для услуг."""
    if pd.isna(pricelist_str) or not pricelist_str:
        return []

    items: List[Dict] = []
    for line in str(pricelist_str).splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 5:
            continue

        is_custom = bool(parts[1].strip())
        try:
            price = int(parts[2].strip())
        except ValueError:
            price = 500

        entry: Dict[str, Any] = {}
        if is_custom:
            entry["ServiceName"] = "Своя услуга"
            entry["ServiceTitle"] = parts[1].strip()
        else:
            entry["ServiceName"] = parts[0].strip()
        entry["ServicePrice"] = price
        entry["ServiceStartingPrice"] = parts[3].strip()
        entry["ServicePriceType"] = parts[4].strip()
        items.append(entry)

    return items


# ═══════════════════════════════════════════
# ОСНОВНАЯ ГЕНЕРАЦИЯ XML
# ═══════════════════════════════════════════

def build_avito_xml(dataframe: pd.DataFrame, params=None) -> str:
    """
    Преобразует DataFrame в XML-строку для Авито.

    Args:
        dataframe: DataFrame с данными объявлений.
        params: ClientParams (нужен для чтения файла городов).

    Returns:
        XML-строка.
    """
    # Загрузка городов
    cities: List[str] = []
    if params:
        cities_file = f"{ROOT_DIR}/{params.name}/{params.k_gorod}"
        cities = load_cities_from_file(cities_file)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<Ads formatVersion="3" target="Avito.ru">',
    ]

    for idx, row in dataframe.iterrows():
        try:
            ad = ["  <Ad>"]

            # Город из строки (для promo)
            current_city = None
            for col in ("город", "Город", "city", "CITY"):
                if col in row.index and pd.notna(row[col]):
                    current_city = str(row[col]).strip()
                    if current_city:
                        break

            # 1) Простые поля
            for df_col, xml_tag in FIELD_MAPPING.items():
                if df_col not in row.index or xml_tag is None:
                    continue
                val = row[df_col]
                if pd.isna(val) or val is None:
                    continue
                val_str = str(val).strip()
                if not val_str:
                    continue

                if df_col == "Description" and needs_cdata(val_str):
                    ad.append(f"    <Description>{make_cdata(val_str)}</Description>")
                else:
                    ad.append(f"    <{xml_tag}>{escape_xml(val_str)}</{xml_tag}>")

            # 2) Множественные значения
            for field in MULTI_VALUE_FIELDS:
                if field not in row.index:
                    continue
                values = split_pipe(row[field])
                if not values:
                    continue
                tag = FIELD_MAPPING.get(field, field) or field
                ad.append(f"    <{tag}>")
                for v in values:
                    ad.append(f"      <Option>{escape_xml(v)}</Option>")
                ad.append(f"    </{tag}>")

            # 3) Изображения
            for field in IMAGE_FIELDS:
                if field not in row.index or pd.isna(row[field]):
                    continue
                imgs = split_pipe(row[field])
                if not imgs:
                    continue
                ad.append("    <Images>")
                for img in imgs:
                    if img.startswith(("http://", "https://")):
                        ad.append(f'      <Image url="{escape_xml(img)}" />')
                    else:
                        ad.append(f'      <Image name="{escape_xml(img)}" />')
                ad.append("    </Images>")

            # 4) PromoManualOptions
            if "PromoManualOptions" in row.index and pd.notna(row["PromoManualOptions"]):
                if row.get("Category") == "Предложение услуг":
                    promo_items = process_promo_service(row["PromoManualOptions"])
                else:
                    city_list = cities.copy()
                    if current_city and current_city not in city_list:
                        city_list.append(current_city)
                    promo_items = process_promo_manual_options(row["PromoManualOptions"], city_list)

                if promo_items:
                    ad.append("    <PromoManualOptions>")
                    for item in promo_items:
                        ad.append("      <Item>")
                        for k, v in item.items():
                            ad.append(f"        <{k}>{escape_xml(str(v))}</{k}>")
                        ad.append("      </Item>")
                    ad.append("    </PromoManualOptions>")

            # 5) PriceList
            if "PriceList" in row.index and pd.notna(row["PriceList"]):
                pl_items = process_pricelist(row["PriceList"])
                if pl_items:
                    ad.append("    <PriceList>")
                    for item in pl_items:
                        ad.append("      <Service>")
                        for k, v in item.items():
                            ad.append(f"        <{k}>{escape_xml(str(v))}</{k}>")
                        ad.append("      </Service>")
                    ad.append("    </PriceList>")

            ad.append("  </Ad>")
            lines.extend(ad)

        except Exception as e:
            print(f"⚠️ Ошибка объявления {idx + 1}: {e}")
            lines.extend(["  <Ad>", f"    <Id>error_{idx + 1}</Id>", "  </Ad>"])

    lines.append("</Ads>")
    return "\n".join(lines)


def save_avito_xml_to_file(
    dataframe: pd.DataFrame,
    params,
    base_output_path: Optional[str] = None,
) -> str:
    """
    Генерирует и сохраняет XML-файл для Авито.

    Args:
        dataframe: DataFrame с объявлениями.
        params: ClientParams.
        base_output_path: Базовый путь (CSV, будет заменён на .xml).

    Returns:
        Путь к сохранённому XML-файлу.
    """
    if base_output_path:
        output_path = base_output_path.replace(".csv", ".xml")
    else:
        output_dir = getattr(params, "output_dir", ".")
        xml_name = f"{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.xml"
        output_path = f"{output_dir}/{xml_name}"

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    xml_content = build_avito_xml(dataframe, params)
    clean_xml = remove_empty_tags(xml_content)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(clean_xml)

    print(f"✅ XML сохранён: {output_path} ({os.path.getsize(output_path)} байт)")
    return output_path
