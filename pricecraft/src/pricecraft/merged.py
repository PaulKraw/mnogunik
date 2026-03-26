
import pandas as pd


def merge_oz_param(df_orig, name_table):
    
    df_merg = get_df(f"unique_types/{name_table}.csv")

    # df_merged_param = df_orig
    
    df_merged_param = df_orig.merge(df_merg, left_on=f"{name_table}_ar", right_on='Имя в базе', how='left',suffixes=(f"", f"_{name_table}"))

    # прибавляем цену к общей цене "Сумма" прибавляем цену "цена" комлектующего 
    # суммирование происходит в следующей переборке строк, где формируется артикул общий и хеш артикула общего

    df_merged_param.to_csv(f"csv/hashed_data_psu_oz_par_after_{name_table}.csv", index=False, encoding="utf-8")

    # отдаем таблицу с одабвлеными параметрами
    return df_merged_param


def get_df(name_table):
    
 

    df = pd.read_csv(name_table)

    # df = df.applymap(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x)
    df = df.apply(lambda col: col.map(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x))


    return df