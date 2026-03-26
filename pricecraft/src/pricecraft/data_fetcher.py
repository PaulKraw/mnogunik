



def down_respons(url_batya, gideon, name_table):
    name_table = f"csv/{name_table}"
    url = f"{url_batya}/export?format=csv&gid={gideon}"

    response = requests.get(url, verify=False)
    response.raise_for_status() 

    with open(name_table, 'wb') as file:
        file.write(response.content)

    # df = pd.read_csv(url) #болеее

    # Или для всех столбцов
    

    df = pd.read_csv(name_table)

    # df = df.applymap(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x)
    df = df.apply(lambda col: col.map(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x))


    return df

def down_respons_main(url_batya, gideon, name_table, strok, stolb, rgb_cell):
    url = f"{url_batya}/export?format=xlsx&gid={gideon}"

    # Загружаем Excel файл по URL
    response = requests.get(url, verify=False)
    response.raise_for_status()

    # Сохраняем файл на диск
    with open(name_table, 'wb') as file:
        file.write(response.content)

    # Загружаем Excel файл с помощью openpyxl
    wb = openpyxl.load_workbook(name_table, data_only=True)
    sheet = wb.active  # Получаем активный лист

    data = []

    # Итерируем по строкам, начиная с 13-й (индекс 12) и столбцам, начиная с 5-го (индекс 4)
    for row in sheet.iter_rows(min_row=strok, min_col=stolb):
        row_data = []
        for cell in row:
            # Проверяем цвет ячейки
            # print(f" cell.value {cell.value} cell.fill {cell.fill}")
            
            fill = cell.fill

            color = fill.start_color.rgb

            # color2 = None

            # if cell.value == 77250:
                # print(f"{color} {cell.value}")
                # print(type(color))
                # print(repr(color))
            
            # color2 = color

            # коричневый FFE599, голубой 00FFFF
            # print(color)
            
             #по умолчанию в сравнении глюк, лучше обрезать альфа канал у обоих
            if isinstance(color, str):
                # Обрезаем альфа-канал, если он есть
                rgbcolor = color[2:]
            else:
                # Устанавливаем значение по умолчанию (например, белый)
                rgbcolor = "FFFFFF"  # или None, если так логика требует
            # rgb_cell
            
            # Проверяем, является ли значение числом и ячейка голубая
            # if rgbcolor == '00FFFF':  # Голубой цвет в формате RGBA
            if rgbcolor == rgb_cell:  # Голубой цвет в формате RGBA
                row_data.append(cell.value)  # Если цвет голубой, записываем пустую ячейку
            else:
                if isinstance(cell.value, (int, float)) or cell.value=="#REF!":
                    row_data.append(None)
                else:
                    row_data.append(cell.value)


            # Если цвет ячейки голубой (проверка цвета, например для голубого)

            # row_data.append(cell.value)

            # if color == 'FF00FFFF':  # Пример для голубого (просто для примера, может быть другой код)
            #     row_data.append(None)  # Пропускаем, если цвет не голубой
            # else:
            #     row_data.append(cell.value)


        data.append(row_data)
    
    # Преобразуем отфильтрованные данные в DataFrame для дальнейшей обработки, если нужно
    import pandas as pd
    df = pd.DataFrame(data)
    # print(df)

     # Сохраняем DataFrame в CSV файл
    # df.to_csv("csv/df_check.csv", index=False)

    # Сохраняем DataFrame в новый Excel файл
    # df.to_excel("xls/df_check.xlsx", index=False, header=False)


    return df
