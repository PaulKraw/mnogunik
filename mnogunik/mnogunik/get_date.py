import sys
import csv
import pandas as pd
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance, ImageFont
import random
import itertools
import os
import string
import datetime
from collections import defaultdict
import re

import imgunik as img
import textfun as txt
import statfun as stt

class ClientParams:
    def __init__(self, name_csv='alx', name='svai', k_gorod='k_gorod.csv', num_ads=1000, orig_t='text_alx.txt', date_f="2023-10-30", end_date=2, num_days=14, periods=None, shuffle_list=True, imgparam=None, info_dict=None):
        self.name_csv = name_csv
        self.name = name
        self.k_gorod = k_gorod
        self.num_ads = num_ads #количесвто строк в файле
        self.orig_t = orig_t
        self.date_f = date_f
        self.end_date = end_date
        self.num_days = num_days
        self.periods = periods
        self.shuffle_list = shuffle_list
        self.imgparam = imgparam
        self.info_dict = info_dict



date = ClientParams(
    name_csv='dddd',
    name='date',
    k_gorod='k_gorod.csv',
    num_ads=1100,
    date_f="2025-01-18",
    num_days=25,
    end_date=30,
    periods=[(44,7,20)] ,
    shuffle_list=True,


)




cl = date


res_date = txt.create_date_list(cl)
adsShort = {
    "DateBegin": res_date, # это и есть res_date
    # "DateEnd": txt.create_enddate_list(res_date,cl.end_date), # как вставить сюда массив что создан строчкой выше
    }


# Имя файла, в который будет сохранен CSV
csv_filename_s = f"{cl.name}_{cl.name_csv}_d.csv"
    
# Получаем количество строк в adsShort


# Создаем список значений из info_dict, повторенных для каждой строки в adsShort


# Открываем файл CSV для записи
with open(csv_filename_s, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)

    # Записываем заголовки (ключи словаря) в файл CSV
    csv_writer.writerow(list(adsShort.keys()) )

    # Записываем данные из adsShort и повторенные значения из info_dict в файл CSV
    for row_values in zip(*adsShort.values()):
        csv_writer.writerow(row_values)

print("новый файл создан")


if __name__ == "__main__":
    print("Конец.")
else:
    print("my_module.py has been imported.")

