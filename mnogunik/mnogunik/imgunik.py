# -*- coding: utf-8 -*-

# import sys
# import csv
# import pandas as pd
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance, ImageFont
import random
import itertools
import os
import string
# import datetime
from collections import defaultdict
import re


from concurrent.futures import ThreadPoolExecutor

from config import ROOT_DIR_OUT, ROOT_DIR, ROOT_URL_OUT

from functools import lru_cache
import gc

@lru_cache(maxsize=64)
def get_font(font_path: str, size: int):
    # один и тот же (path, size) не открываем заново
    return ImageFont.truetype(font_path, size)

@lru_cache(maxsize=128)
def get_font_cached(font_path: str, size: int) -> ImageFont.FreeTypeFont:
    # Открываем TTF один раз на (path, size)
    return ImageFont.truetype(font_path, size)

# ROOT_DIR = 'C:/proj/'
# ROOT_DIR_OUT = 'C:/proj/outfile/'


# def create_and_process_img_url(params, extended_price_df, ROOT_DIR):
    

def get_imagesUrls_dops_х(cl):
    # вспомогательные данные
    fold = "C:"

    # выходные данные
    file_name = file_path = os.path.join(ROOT_DIR_OUT, f'{cl.name_csv}/{cl.name}', 'file.txt') 

    original_image_folder = f"img\\orig{cl.name_csv}"
    output_texturl_folder = "output_file"
    address_to_append = cl.imgparam['address_to_append']
    type_kol = cl.imgparam['type_kol']
    scalem = cl.imgparam['scalem']
    png_spacing = cl.imgparam['png_spacing']
    x_cor_png = cl.imgparam['x_cor_png']
    y_cor_png = cl.imgparam['y_cor_png']
    perspective_scale_percent = cl.imgparam['perspective_scale_percent']
    position_txt = cl.imgparam['position_txt']
    font_color_txt = cl.imgparam['font_color_txt']
    images_per_ad = cl.imgparam['images_per_ad']
    maximg_ads = cl.imgparam['maximg_ads']
    rotate_params = cl.imgparam['rotate_params']
    size_params = cl.imgparam['size_params']
    crop_params = cl.imgparam['crop_params']
    contrast_range = cl.imgparam['contrast_range']

    output_folder = f"output_file/img_{cl.name_csv}"
    os.makedirs(output_folder, exist_ok=True)

    font_path = cl.imgparam['font_path']
    font_size = cl.imgparam['font_size']
    if font_path is None:
        font = ImageFont.load_default()
    else:
        font = ImageFont.truetype(font_path, font_size)

    image_namesjpg = []
    image_games_folder = f"img/photo1" # Папка с изображениями
    image_games = [os.path.join(image_games_folder, filename) for filename in os.listdir(image_games_folder) if filename.lower().endswith(".jpg")]
    # image_dim_folder = "img/dim"# Папка с изображениями
    # image_dim = [os.path.join(image_dim_folder, filename) for filename in os.listdir(image_dim_folder) if filename.lower().endswith(".png")]
    obloj = Image.open("img/obloj.png").convert("RGBA")
    # input_folder = f"img/imgpng/{cl.name_csv}"
    # image_paths = [os.path.join(input_folder, filename) for filename in os.listdir(input_folder) if filename.lower().endswith(".png")]
    # images = [Image.open(path).convert("RGBA") for path in image_paths]
    preim = []
    with open('vars/preim.txt', 'r', encoding='utf-8') as file:
        for line in file:
            # Удаляем лишние пробелы в начале и в конце строки
            line = line.strip()
            # Проверяем длину строки
            if len(line) <= 40:
                preim.append(line)

    # random_lists = generate_random_lists(len(images), cl.num_ads*10 )


    for ii in range(cl.num_ads):
        image_namesjpg.append(f"101_{ii+1}.jpg")

    for i in range(cl.num_ads):
        scalem = cl.imgparam['scalem']
        # Выбор случайной уникальной перестановки
        # selected_permutation = get_random_img(i, random_lists, images)
        collage = Image.new("RGB", (1200, 800), (255, 255, 255))
        random_image = Image.open(random.choice(image_games))# Открываем выбранное изображение
        random_image.thumbnail(collage.size, Image.LANCZOS)  # Изменяем размер изображения, чтобы оно вместились в коллаж

        # random_dim = Image.open(random.choice(image_dim)).convert("RGBA") # Открываем выбранное изображение

        # Вставляем изображение в коллаж
        collage.paste(random_image, (0, 0))
        # collage.paste(random_dim, (0, 0), random_dim)
        collage.paste(obloj, (0, 0), obloj)
        # xysq = []
        
        # add_image_info(type_kol, png_spacing, xysq, selected_permutation[0], scalem, x_cor_png, y_cor_png)
        # for y in range(1, 7):
        #     scalem += perspective_scale_percent * scalem


        #     if type_kol == "top":
        #         add_image_info(type_kol, png_spacing, xysq, selected_permutation[y], scalem, xysq[y - 1]["x"], y_cor_png)
        #     elif type_kol == "left" or type_kol == "right":
        #         add_image_info(type_kol, png_spacing, xysq, selected_permutation[y], scalem, x_cor_png, xysq[y - 1]["y"])
        #     else:
        #         add_image_info(type_kol, png_spacing, xysq, selected_permutation[y], scalem, xysq[y - 1]["x"], y_cor_png)


        # Отрисовываем картинки в обратном порядке
        # for y in reversed(xysq):
        #     collage.paste(y["img"], (y["x"], y["y"]), y["img"])

        # Переберите элементы строки и названия полей
        text = random.choice(preim)
        
        collage = img.add_text_to_image(collage, text, position_txt, font_size=font_size, font_color=font_color_txt, font_path=font_path, encoding="utf-8")

        img.apply_modifications(collage, rotate_params, size_params, crop_params, contrast_range)

        collage.save(os.path.join(output_folder, f"101_{i+1}.jpg"))
        print(f"{cl.name_csv}/ картинка колаж номер {i+1} создана")

    print(f" первые картинки сгенерированы ")
    
    
    generate_images(original_image_folder,  output_folder, maximg_ads, images_per_ad, rotate_params, size_params, crop_params, contrast_range)

    generate_text_file(output_texturl_folder, cl.num_ads, images_per_ad, maximg_ads)

    array_img = generate_text_array(output_texturl_folder, cl.num_ads, images_per_ad, maximg_ads)

    print("Доп картинки созданы.")  

    imgurl_list = [' | '.join(pair) for pair in zip(image_namesjpg, array_img)]

    
    
    imagesUrls = []
    for line in imgurl_list:
        image_names = line.strip().split(" | ")  # Разделяем строку на отдельные названия картинок
        imgurl_images = [f"{address_to_append}{image_name}" for image_name in image_names]
        imagesUrls.append(" | ".join(imgurl_images)) 
    
    return imagesUrls


def process_image_row(original_image_folder, imagename, output_folder, nameimg, imgparam, txt_block=[], style_txt=None, ind=0, namecl="svai"):
    if txt_block is None:
        txt_block = []
    if style_txt is None:
        style_txt = {}
    
    rotate_params = imgparam['rotate_params']
    size_params = imgparam['size_params']
    crop_params = imgparam['crop_params']
    contrast_range = imgparam['contrast_range']
    # print(f"{original_image_folder} - {imagename}")
    image_path = os.path.join(original_image_folder, imagename)

    with Image.open(image_path) as src_img:
        # image = Image.open(image_path)

        enhanced_image = apply_modifications(src_img, rotate_params, size_params, crop_params, contrast_range)
        # print(txt_block)
    
        if txt_block:

            pos_x = style_txt.get('left', 100)
            pos_y = style_txt.get('top', 100)
            max_w_txt = style_txt.get('width', 800)

            next_x, next_y = pos_x, pos_y

            cntr = style_txt.get('center', 0)

            for block in txt_block:
            
                text = f"{block.get('text', '')}"
                tag = block.get("tag")
                fsize = style_txt.get(tag, {}).get('font_size', 40)
                font_path = style_txt.get(tag, {}).get('font_path')
                font_path = f"{ROOT_DIR}/{namecl}/var/font/{font_path}" if font_path else f"{ROOT_DIR}/{namecl}/var/font/PTSans-Bold.ttf"



                font_color = tuple(style_txt.get(tag, {}).get('color', (0, 0, 0)))

                if not text.strip():
                    bg_color_txt = (255, 255, 255, 0)
                    outline_color = (255, 255, 255, 0)
                else:
                    bg_color_txt = tuple(style_txt.get(tag, {}).get('background_color', (255, 255, 255, 0)))
                    outline_color = tuple(style_txt.get(tag, {}).get('outline_color', (255, 255, 255, 40)))

            # if text=""


                if calc_text_width(text, font_path, fsize) < max_w_txt:
                    if cntr == 1:
                        next_x = int((enhanced_image.width - img.calc_text_width(text, font_path, fsize)) / 2)
                    position = (next_x, next_y)
                    enhanced_image = add_text_to_image(
                        enhanced_image, text, position, fsize,
                        font_color, bg_color_txt, outline_color, font_path
                    )
                    next_y += fsize + style_txt.get(tag, {}).get('margin', 0)
                else:
                    lines = wrap_text_to_width(text, font_path, fsize, max_w_txt)
                    for line in lines:
                        if cntr == 1:
                            next_x = int((enhanced_image.width - calc_text_width(line, font_path, fsize)) / 2)
                        position = (next_x, next_y)
                        enhanced_image = add_text_to_image(
                            enhanced_image, line, position, fsize,
                            font_color, bg_color_txt, outline_color, font_path
                        )
                        next_y += round(fsize * 1.65)
                    next_y += style_txt.get(tag, {}).get('margin', 0)

        # запись
        output_path = os.path.join(ROOT_DIR_OUT, output_folder)
        os.makedirs(output_path, exist_ok=True)
        out_file = os.path.join(output_path, nameimg)
        enhanced_image.save(out_file, optimize=True)

    # очистка ссылок и сборка мусора
    del enhanced_image
    gc.collect()

def wrap_text_to_width(text, font_path, font_size, max_width):
    """Разбивает текст на строки, чтобы каждая не превышала max_width."""
    # Создание объекта шрифта
    # print(f"type(font_path): {type(font_path)}")
    # font = ImageFont.truetype(font_path, font_size)

    words = text.split(' ')
    lines = []
    current_line = ""
    for word in words:
        # Проверяем ширину строки с добавлением нового слова
        test_line = f"{current_line} {word}".strip()
        # test_width = font.getsize(test_line)[0]  # Измеряем ширину
        test_width = calc_text_width(test_line, font_path, font_size)
# calc_text_width

    # pile_diameter_width, pile_diameter_height = img.calculate_text_bbox(pile_diameter, font_path, fz)


        if test_width <= max_width:
            current_line = test_line  # Добавляем слово в текущую строку
        else:
            lines.append(current_line)  # Сохраняем текущую строку
            current_line = word  # Начинаем новую строку
    lines.append(current_line)  # Добавляем последнюю строку
    return lines


def get_imagesUrls_dops(cl):
    # вспомогательные данные
    fold = "D:"

def load_system_font(font_name, font_size):
    try:
        system_font_path = get_system_fonts_path()  # Получаем путь к системным шрифтам
        if system_font_path:
            arial_font_path = os.path.join(system_font_path, font_name + ".ttf")
            font = ImageFont.truetype(arial_font_path, font_size)
            return font
        else:
            print("Не удалось получить путь к системным шрифтам.")
            return None
    except Exception as e:
        print(f"Ошибка при загрузке шрифта: {e}")
        return None


def generate_images(original_image_folder, output_folder, num_ads, images_per_ad, rotate_params, size_params, crop_params, contrast_range):
    files = sorted(os.listdir(original_image_folder))
    original_files = files.copy()

    with ThreadPoolExecutor() as executor:
        for ad_num in range(1, num_ads + 1):
            # print(f"Создаем {ad_num} допкартинки")
            tasks = []
            for img_num in range(1, images_per_ad + 1):
                if not original_files:
                    original_files = files.copy()

                current_original = original_files.pop(0)
                task = executor.submit(process_image, original_image_folder, output_folder, current_original, ad_num, img_num, images_per_ad, rotate_params, size_params, crop_params, contrast_range)
                tasks.append(task)

            for task in tasks:
                task.result()  # ждем завершения всех задач

def process_image(original_image_folder, output_folder, current_original, ad_num, img_num, images_per_ad, rotate_params, size_params, crop_params, contrast_range):
    current_image = Image.open(os.path.join(original_image_folder, current_original))
    modified_image = apply_modifications(current_image, rotate_params, size_params, crop_params, contrast_range)
    new_filename = f'dop_{(ad_num - 1) * images_per_ad + img_num}.jpg'
    modified_image = resize_image_randomly(modified_image)
    modified_image.save(os.path.join(output_folder, new_filename))

def resize_image_randomly(image, min_delta=-30, max_delta=30):
    width, height = image.size
    delta = random.randint(min_delta, max_delta)
    new_width = width + delta
    new_height = int((new_width / width) * height)
    return image.resize((new_width, new_height), Image.Resampling.LANCZOS)

def apply_modifications(image, rotate_range, size_range, crop_range, contrast_range):
    rotate_amount = random.uniform(rotate_range[0], rotate_range[1])
    rotated = image.rotate(rotate_amount)

    new_size = (
        int(image.width * (1 + random.uniform(size_range[0], size_range[1]) / 100)),
        int(image.height * (1 + random.uniform(size_range[0], size_range[1]) / 100))
    )
    resized = rotated.resize(new_size)

    crop_box = (
        int(resized.width * crop_range[0] / 100),
        int(resized.height * crop_range[0] / 100),
        int(resized.width * (1 - crop_range[1] / 100)),
        int(resized.height * (1 - crop_range[1] / 100))
    )
    cropped = resized.crop(crop_box)

    contrast_factor = 1 + random.uniform(-contrast_range, contrast_range)
    enhanced = ImageEnhance.Contrast(cropped).enhance(contrast_factor)

    w_main, h_main = enhanced.size 

    collage = Image.new("RGB", (w_main, h_main), (255, 255, 255))

    new_size2 = (int(w_main+10), int(h_main+10))
    enhanced2 = enhanced.resize(new_size2)
    obloj_x = 0 - randomik(0,10,1)
    obloj_y = 0 - randomik(0,10,1)

    # collage = collage.convert("RGBA")  # если вдруг не RGBA

    collage.paste(enhanced2, (obloj_x, obloj_y))

    # освобождаем временные PIL-объекты, чтобы не копились
    for im in (rotated, resized, cropped, enhanced, enhanced2):
        try:
            im.close()
        except Exception:
            pass


    return collage


def generate_text_file(output_folder, num_ads, images_per_ad, maximg_ads):
    text_file_path = os.path.join(output_folder, 'url_img.txt')
    with open(text_file_path, 'w') as txt_file:
        num_ads_i = 0
        while num_ads>=num_ads_i:
            
            for ad_num in range(1, maximg_ads + 1):
                images_line = ' | '.join([f'{(ad_num - 1) * images_per_ad + img_num}.jpg' for img_num in range(1, images_per_ad + 1)])
                txt_file.write(images_line + '\n')
                num_ads_i += 1
                if num_ads<=num_ads_i:
                    break

def generate_text_array(output_folder, num_ads, images_per_ad, maximg_ads):
    text_data = []
    num_ads_i = 0
    while num_ads>=num_ads_i:
        for ad_num in range(1, maximg_ads + 1):
            images_line = ' | '.join([f'dop_{(ad_num - 1) * images_per_ad + img_num}.jpg' for img_num in range(1, images_per_ad + 1)])
            text_data.append(images_line)
            num_ads_i += 1
            if num_ads<=num_ads_i:
                return text_data
        
    return text_data

# Функция для создания информации о картинке и добавления ее в список
def add_image_info(type_kol, png_spacing, xysq, img, scale, x, y):

    scaled_image = img.resize((int(img.width * scale), int(img.height * scale)), Image.NEAREST)

    if type_kol == "top":
        w = img.width if not xysq else img.width + 10
        new_x = x + (w - int(w * png_spacing)) if xysq else x
        new_y = 0
    elif type_kol == "left":
        h = 0 if not xysq else img.height + 10
        new_y = y + (h - int(h * png_spacing)) if xysq else y
        new_x = 0
    elif type_kol == "right":
        h = 0 if not xysq else img.height + 10
        new_y = y + (h - int(h * png_spacing)) if xysq else y
        new_x = 1200-round(img.width*scale)
    else:

        w = img.width if not xysq else img.width + 100

        new_x = x + (w - int(w * png_spacing)) if xysq else 500
        new_y = 0
        new_y = int(y - scaled_image.height - ((img.height - scaled_image.height) / 4))


    # new_y = int(collage.height - scaled_image.height - ((img.height - scaled_image.height) / 4))
    xysq.append({"x": new_x, "y": new_y, "w": scaled_image.width, "img": scaled_image})





def get_random_img(num_png, images):
    
    max = num_png * 10
    length = len(images) 

    random_numbers = []  # Изначально пустой массив

    # for _ in range(10):  # Цикл повторяет 10 раз
    #     random_numbers.extend(list(range(0, length)))  # Добавляем массив в конец

    random_numbers.extend(list(range(0, length)))  # Добавляем массив в конец
    
    random.shuffle(random_numbers)
    
    indices = random_numbers[:num_png]
    result_images = [images[i] for i in indices]
    return result_images


def add_text_to_image(image, text, position, font_size, font_color=(0, 0, 0),
                      bg_color_txt=(200, 200, 200, 128), outline_color=(255, 255, 255, 128), 
                      font_path=None, encoding="utf-8", outline_width=5, center=False, trim_left_half_font=False):
    
    """
    Рисует текст на image, при необходимости с подложкой и обводкой.
    - Безопасно для больших серий: кеширует шрифты, не «залипает» на файловых дескрипторах.
    - Корректная альфа: конвертируем в RGBA только копию для пасты, исходник не портим.
    - Центрирование — по реальной ширине изображения, без хардкода 1200.
    """


    # 1) НЕ перекодируем текст: в Python строки уже Unicode. Удаляем encode/decode.
    if not isinstance(text, str):
        text = str(text)


    # 2) Шрифт: кешируем, чтобы не открывать TTF на каждую картинку
    if font_path:
        font = get_font_cached(font_path, font_size)
    else:
        font = ImageFont.load_default()

    # text_width, text_height = calculate_text_bbox(text, font_path, font_size)

    # Убедитесь, что текст закодирован в указанной кодировке
    text = text.encode(encoding).decode(encoding)

    # Создаем объект для рисования текста на исходном изображении
    # draw = ImageDraw.Draw(image)

    text_width, hhh = calculate_text_bbox(text, font_path, font_size)
    www, text_height = calculate_text_bbox("text", font_path, font_size)

    if bg_color_txt[-1] == 0 and outline_color[-1] == 0:
            draw = ImageDraw.Draw(image)
            draw.text(position, text, font=font, fill=font_color)
            return image

    padding = int(font_size / 2)
    background_width = text_width + padding * 2  # Паддинг с каждой стороны
    background_height = text_height + padding * 2  # Паддинг с каждой стороны

    bg_w = background_width + outline_width * 2
    bg_h = background_height + outline_width * 2
    background = Image.new("RGBA", (bg_w, bg_h), (0, 0, 0, 0))
    draw_background = ImageDraw.Draw(background)

    # Небольшой декоративный уголок 
    draw_background.rectangle(
        [0, 0, int(background_width * 0.1), int(background_height * 0.1)],
        fill=bg_color_txt
    )

    # Обводка
    draw_background.rectangle(
        [0, 0, bg_w, bg_h],
        fill=outline_color
    )

    # Основной фон
    draw_background.rectangle(
        [outline_width, outline_width, outline_width + background_width, outline_width + background_height],
        fill=bg_color_txt
    )


    # Текст на подложке: сдвигаем чуть вверх, чтобы визуально центрировать по высоте
    text_position = (outline_width + padding, outline_width + padding - round(font_size / 3))
    draw_background.text(text_position, text, font=font, fill=font_color)

     # 6) Центрирование по ширине текущего изображения (если просили)
    x, y = position  # не мутируем исходный кортеж
    if center:
        img_w, _ = image.size
        x = int((img_w - bg_w) / 2)

    # 7) Паста с альфой: убедимся, что у «приёмника» есть альфа-канал
    #    (на самом деле pillow умеет клеить RGBA на RGB с маской, но так надёжнее)
    needs_convert_back = False
    if image.mode != "RGBA":
        image = image.convert("RGBA")
        needs_convert_back = True

    # Если исходник был RGB — вернём обратно
    if needs_convert_back:
        image = image.convert("RGB")

    bg_to_paste = background
    if trim_left_half_font:
        trim_px = int(font_size / 2)
        if trim_px < background.width:
            bg_to_paste = background.crop((trim_px, 0, background.width, background.height))

    # image.paste(background, (x, y), background)
    image.paste(bg_to_paste, (x, y), bg_to_paste)

    

    #     # Рисуем полупрозрачный прямоугольник на подложке
    #     draw_background.rectangle([outline_width, outline_width, 
    #                             background_width + outline_width, 
    #                             background_height + outline_width], fill=bg_color_txt)

    #     # Позиционируем текст на подложке
    #     text_position = (padding, padding - round(font_size/3))
    #     draw_background.text(text_position, text, font=font, fill=font_color)
    #     #в прошлом комите короче

    #     if center==True:
    #         background_width, background_height = background.size
    #         position[0] = int(1200 / 2) - int(background_width / 2)
        
    #     # Вставляем подложку с текстом на исходное изображение
    #     image.paste(background, position, background)

    return image



def add_rotated_text_to_image(image, text, position, font_size, font_color=(0, 0, 0),
                              bg_color_txt=(200, 200, 200, 128), outline_color=(255, 255, 255, 128),
                              font_path=None, encoding="utf-8", outline_width=5, center=False):
    # Если font_path не указан, используем шрифт по умолчанию
    font = ImageFont.truetype(font_path, size=font_size) if font_path else ImageFont.load_default()

    # Убедитесь, что текст закодирован в указанной кодировке
    text = text.encode(encoding).decode(encoding)

    # Создаем объект для рисования на слое
    draw = ImageDraw.Draw(image)


    # Создаем слой для текста
    # text_width, text_height = font.getsize(text)

    text_bbox = draw.textbbox(position, text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]


    padding = int(font_size / 2)
    layer_width = text_width + padding * 2 + outline_width * 2
    layer_height = text_height + padding * 2 + outline_width * 2
    text_layer = Image.new("RGBA", (layer_width, layer_height), (0, 0, 0, 0))

    draw = ImageDraw.Draw(text_layer)

    # Рисуем обводку (если указана)
    if outline_color[-1] > 0:
        draw.rectangle(
            [outline_width, outline_width, layer_width - outline_width, layer_height - outline_width],
            fill=outline_color
        )

    # Рисуем фон текста (если указан)
    if bg_color_txt[-1] > 0:
        draw.rectangle(
            [outline_width + padding, outline_width + padding,
             layer_width - outline_width - padding, layer_height - outline_width - padding],
            fill=bg_color_txt
        )

    # Рисуем текст
    text_position = (outline_width + padding, outline_width + padding)
    draw.text(text_position, text, font=font, fill=font_color)

    # Поворачиваем текстовый слой
    rotated_text_layer = text_layer.rotate(90, resample=Image.BICUBIC, expand=True)

    # Если включен центрированный режим, корректируем позицию
    if center:
        image_width, image_height = image.size
        layer_width, layer_height = rotated_text_layer.size
        position = (
            int(image_width / 2 - layer_width / 2),
            int(image_height / 2 - layer_height / 2)
        )

    # Накладываем повернутый текст на исходное изображение
    image.paste(rotated_text_layer, position, rotated_text_layer)

    return image



def lighten_color(color, factor):
    """
    Функция для осветления цвета на заданный коэффициент.
    
    :param color: Цвет в формате (R, G, B, A), например, (255, 255, 255, 128)
    :param factor: Коэффициент осветления, например, 1.2 (увеличит яркость на 20%)
    :return: Новый цвет в формате (R, G, B, A) после осветления
    """
    r, g, b, a = color

    # Осветление каждого канала (красный, зелёный, синий)
    r = min(int(r * factor), 255)
    g = min(int(g * factor), 255)
    b = min(int(b * factor), 255)
    
    return (r, g, b, a)



def set_forfor_kol(image_paths,collage):

    random_images = random.sample(image_paths, 4)

    for j, image_path in enumerate(random_images):
        image = Image.open(image_path)
        image.thumbnail((600, 400))
        x = (j % 2) * 600
        y = (j // 2) * 400
        collage.paste(image, (x, y))





def randomik(x, y, z):
    return (int(random.random() * ((x - y) / z)))*(-z) + x


def calculate_text_bbox(text, font_path, font_size):
    # Загружаем шрифт
    font = ImageFont.truetype(font_path, font_size)
    
    # Создаём временное изображение для расчётов
    temp_image = Image.new("RGBA", (1, 1))
    draw = ImageDraw.Draw(temp_image)
    
    # Вычисляем размеры бокса текста
    bbox = draw.textbbox((0, 0), text, font=font)
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    
    return width, height

def calc_text_width(text, font_path, font_size):
    # Загружаем шрифт
    font = ImageFont.truetype(font_path, font_size)
    
    # Создаём временное изображение для расчётов
    temp_image = Image.new("RGBA", (1, 1))
    draw = ImageDraw.Draw(temp_image)
    
    # Вычисляем размеры бокса текста
    bbox = draw.textbbox((0, 0), text, font=font)
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    
    return width

# def calc_text_width(text, font_or_path, font_size):
#     if isinstance(font_or_path, str):
#         font = ImageFont.truetype(font_or_path, font_size)
#     else:
#         font = font_or_path  # уже готовый шрифт
#     if hasattr(font, 'getlength'):
#         return font.getlength(text)
#     else:
#         return font.getsize(text)[0]
#     return font.getlength(text) if hasattr(font, 'getlength') else font.getsize(text)[0]
#     # Загружаем шрифт
#     font = ImageFont.truetype(font_path, font_size)
    
#     # Создаём временное изображение для расчётов
#     temp_image = Image.new("RGBA", (1, 1))
#     draw = ImageDraw.Draw(temp_image)
    
#     # Вычисляем размеры бокса текста
#     bbox = draw.textbbox((0, 0), text, font=font)
#     width = bbox[2] - bbox[0]
#     height = bbox[3] - bbox[1]
    
#     return width

# Устанавливаем уровень прозрачности (0 - полностью прозрачно, 255 - полностью непрозрачно)
def change_transparency(image, alpha):
    # Разделяем изображение на RGB и альфа-канал
    r, g, b, a = image.split()
    
    # Применяем новый уровень прозрачности
    a = a.point(lambda p: alpha)
    
    # Собираем обратно изображение с изменённой прозрачностью
    return Image.merge("RGBA", (r, g, b, a))

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r'(\d+)', s)]



def get_text_for_description(cl_name, art_gip_value, preim, root_dir="ROOT_PATH"):
    """
    Возвращает текст для вставки: либо из gipotez.csv (title-img), либо случайный из preim

    :param cl_name: имя проекта
    :param art_gip_value: значение art-gip
    :param preim: список альтернативных текстов
    :param root_dir: путь к корню
    :return: строка текста
    """
    input_file_path = os.path.join(root_dir, cl_name, "var", "text", "gipotez.csv")
    # print(input_file_path)
    # print(art_gip_value)

    if art_gip_value:  # Если не пусто — пробуем взять из файла
        input_file_path = os.path.join(root_dir, cl_name, "var", "text", "gipotez.csv")
        # print(input_file_path)
        if os.path.exists(input_file_path):
            try:
                df = pd.read_csv(input_file_path)
                
                matched_row = df[df['art-gip'] == art_gip_value]

                if not matched_row.empty:
                    return matched_row.iloc[0]['title-img']
            except Exception as e:
                print(f"Ошибка при чтении gipotez.csv: {e}")

    # Если не найдено или пустой art_gip_value — случайный текст
    print('берем рандом')
    return random.choice(preim)



def wrap_text_to_width__(text, font, max_width, draw):
    words = text.split()
    lines = []
    current_line = ""

    for word in words:
        test_line = f"{current_line} {word}".strip()
        width = draw.textlength(test_line, font=font)
        if width <= max_width:
            current_line = test_line
        else:
            lines.append(current_line)
            current_line = word
    if current_line:
        lines.append(current_line)
    return lines

def draw_text_in_box(config: dict, image):
    draw = ImageDraw.Draw(image)

    text = config["text"]
    x = config["x"]
    y = config["y"]
    box_width = config["box_width"]
    box_height = config["box_height"]
    font_path = config["font_path"]
    font_size = config["font_size"]
    font_color = tuple(config.get("font_color", (0, 0, 0)))
    align = config.get("align", "left")

    font = ImageFont.truetype(font_path, font_size)

    # Перенос строк
    lines = wrap_text_to_width(text, font_path, font_size, box_width)

    total_text_height = len(lines) * int(font_size * 1.2)  # чуть больше высоты шрифта
    start_y = y + (box_height - total_text_height) // 2

    for line in lines:
        line_width = draw.textlength(line, font=font)

        if align == "center":
            text_x = x + (box_width - line_width) // 2
        elif align == "right":
            text_x = x + (box_width - line_width)
        else:
            text_x = x

        draw.text((text_x, start_y), line, font=font, fill=font_color)
        start_y += int(font_size * 1.2)

    return image






def paste_images_with_offset(image_data_list, collage, root_dir, cl_name, temp_cat):
    """
    Вставляет несколько изображений с разбросом по координатам.

    image_data_list: список из подсписков вида [имя_файла, x, dx, y, dy]
    collage: объект Image, на который вставляются изображения
    root_dir: корневая директория
    cl_name: имя подпапки (например, cl.name)
    """
    for data in image_data_list:
        image_name, x, dx, y, dy = data
        offset_x = x + random.randint(0, dx)
        offset_y = y + random.randint(0, dy)
        image_path = f"{root_dir}/{cl_name}/var/img/{temp_cat}/{image_name}"
        image = Image.open(image_path).convert("RGBA")
        collage.paste(image, (offset_x, offset_y), image)
