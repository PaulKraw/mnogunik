"""
klass.py — Класс параметров клиента.

Содержит ClientParams — единственную модель данных проекта.
Все параметры генерации объявлений передаются через этот объект.
"""

from typing import Any, Dict, List, Optional, Tuple


class ClientParams:
    """
    Параметры клиента / аккаунта для генерации объявлений.

    Attributes:
        name: Имя клиента (= имя папки в proj/).
        name_csv: Идентификатор аккаунта внутри клиента.
        k_gorod: Путь к файлу распределения городов (относительно proj/name/).
        num_ads: Целевое количество объявлений.
        file_price: Имя файла прайса или URL Google Sheets.
        date_f: Дата начала размещения (YYYY-MM-DD).
        end_date: Срок жизни объявления в днях.
        num_days: Количество дней для генерации дат.
        periods: Список кортежей (кол-во_в_день, час_от, час_до).
        imgparam: Словарь параметров обработки изображений.
        address_to_append: Базовый URL для ссылок на картинки.
        info_dict: Контактные данные (CompanyName, ContactPhone и т.д.).
    """

    def __init__(
        self,
        name_csv: str = "alx",
        name: str = "svai",
        k_gorod: str = "k_gorod.csv",
        num_ads: int = 1000,
        orig_t: str = "text.txt",
        file_price: str = "file_price.csv",
        date_f: str = "2023-10-30",
        end_date: int = 2,
        num_days: int = 14,
        periods: Optional[List[Tuple[int, int, int]]] = None,
        shuffle_list: bool = True,
        imgparam: Optional[Dict[str, Any]] = None,
        address_to_append: str = "https://paulkraw.ru/",
        CompanyName: Optional[str] = None,
        ContactPhone: Optional[str] = None,
        ManagerName: Optional[str] = None,
        cat_wp: Optional[str] = None,
        info_dict: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name_csv = name_csv
        self.name = name
        self.cat_wp = cat_wp
        self.CompanyName = CompanyName
        self.ManagerName = ManagerName
        self.ContactPhone = ContactPhone
        self.k_gorod = k_gorod
        self.num_ads = num_ads
        self.orig_t = orig_t
        self.date_f = date_f
        self.end_date = end_date
        self.num_days = num_days
        self.periods = periods
        self.imgparam = imgparam
        self.file_price = file_price
        self.address_to_append = address_to_append
        self.info_dict = info_dict or {}
