import pandas as pd

def format_execution_time(execution_time):
        # Рассчитываем минуты и секунды
        minutes = int(execution_time // 60)
        seconds = execution_time % 60
        
        # Функция для определения правильного окончания слова "минута"
        def get_minutes_word(minutes):
            if 11 <= minutes % 100 <= 19:
                return "минут"
            elif minutes % 10 == 1:
                return "минута"
            elif 2 <= minutes % 10 <= 4:
                return "минуты"
            else:
                return "минут"

        # Получаем правильное окончание
        minutes_word = get_minutes_word(minutes)

        return f"Время выполнения программы: {minutes} {minutes_word} ({seconds:.2f} секунд)"


def getHashTable(df):
        print(len(df.columns))  # 28 столбцов + 1 первый
        print(len(df))  # 36 строк + 1 первая


        len_stolb = len(df.columns) 
        len_strok = len(df) 

        data = []

        numstrok = 0

        for i in range(1, len_strok):

            for j in range(1, len_stolb):
                # if j < 5:
                #     print(f" i - {i} / j - {j}")
                # if 
                # print(f"i {i}, j {j}, ")
                value = df.iloc[i, j]
                # if value != "" and pd.notna(value):
                if pd.notna(value):
                    numstrok += 1
                    stolb_name = df.columns[j]
                    # print(f" stolb_name = - {df.columns[j] } ")

                    # if j < 5:
                    #     print(f" 1 stolb_name = - {df.columns[j] } ")
                    # stolb_name = df.iloc[0, j] # Берем имя видеокарты из первой строки

                    
                    # stolb_name = df.iloc[-1, j]
                    # strok_name = df.iloc[i, 0]
                    strok_name = df.iloc[i, 0]
                    # if j < 5:
                    #     print(f" strok_name = - {df.iloc[i, 0]} ")

                    
                    # print(f"numstrok {numstrok} / i {i}, j {j}, stolb {stolb_name} / strok {strok_name}  val: {value}")

                    fixed_hash = get_fixed_hash(f"{strok_name}_{stolb_name}", length=8)

                    data.append({"conf":fixed_hash, "Процессор_ar":strok_name,"Видеокарта_ar":stolb_name,"Сумма":value})
            
        df_out = pd.DataFrame(data)

        return df_out
    
    
    # приводит в порядок таблицу
def init_df(df):
    # df = df.iloc[]
    df = df.dropna(how='all') 
    df = df.dropna(axis=1, how='all') 

    print(df) 
    # df_new = df.iloc[3:]
    # df = df.iloc[strok-1:, stolb-1:]  # строки с 1 до 3, столбцы с 0 до 2

    df = df.drop(1, axis=0).drop(df.columns[[1,2]], axis=1).reset_index(drop=True)
    df.columns = range(df.shape[1]) 

    df.columns = df.iloc[0]  # Устанавливаем первую строку как названия столбцов
    df = df[1:].reset_index(drop=True)  # Удаляем первую строку и сбрасываем индексы
    return df