"""
Скрипт компиляции набора данных о заболеваемости COVID-19 в регионах России.

Скрипт обрабатывает помесячные данные статистики заболеваемости COVID-19
и формирует единый набор данных в виде файла covid_russia.xlsx

Использование:

    - клонировать репозиторий в какой-либо каталог

    - установить необходимые пакеты: 
        pip install -r requirements.txt

    - загрузить исходные данные с сайта Росстат в каталог data/

    - запустить скрипт: 
        python compile_data.py

Исходные данные можно найти на сайте Росстат https://rosstat.gov.ru поиском 
формы "ЕСТЕСТВЕННОЕ ДВИЖЕНИЕ НАСЕЛЕНИЯ" или по прямой ссылке 
(данные на октябрь 2021 доступны тут: https://rosstat.gov.ru/storage/mediabank/2021_edn10.htm)

Исходные данные используются as is, без модификаций или изменений. Автор не несет какой-либо ответственности 
за интерпретацию исходных данных или обработанных результатов.

Обновление данных будет производиться по мере необходимости и наличия свободного времени.
"""

import pandas as pd
import re
import json

from pathlib import Path
from functools import reduce, cache

@cache
def get_reg_map():
    with open('reg_map.json', 'rt', encoding='utf-8') as fp:
        js_data = json.load(fp)
        df_reg = pd.DataFrame(
            data=[v for v in js_data['regions'].values()],
            columns=list(js_data['regions']['01'].keys()),
        )
        df_reg['name_key'] = df_reg['reg_name'].str.replace(' ','').str.upper()

        df_fo = pd.DataFrame(
            data=[v for v in js_data['fo'].values()],
            columns=list(js_data['fo']['01'].keys()),
        )
        df_fo['name_key'] = df_fo['fo_name'].str.replace(' ','').str.upper()

        return df_reg, df_fo


def process_file(file_name):
    print(f'Обработка файла {file_name}...')

    # Определяем период по имени файла
    period = [int(x) for x in re.findall(r'\d+', file_name)]
    if len(period) < 2:
        print('--> Не могу определить период из имени файла, пропускаю файл')
        return None

    if period[0] >= 2020:
        # Перепутаны периоды
        period = [x for x in reversed(period)]
    print(f'--> Период {period[0]:02d}.{period[1]:04d}')

    # Ищем закладку со статистикой COVID (форма 5.1)
    all_sheets = pd.read_excel(file_name, sheet_name=None)
    keys = [key for key in all_sheets.keys() if '5.1' in key or '5_1' in key]
    if not keys:
        print('--> Статистика по COVID не найдена, пропускаю файл')
        return None
    df_data = all_sheets[keys[0]]

    # Ищем начало таблицы (строка Российская Федерация)
    start_idx = df_data[ df_data[df_data.columns[0]].str.contains('Российская Федерация') == True ].index
    if not start_idx.empty:
        n_start = start_idx[0]
    else:
        print('--> Не найдено начало таблицы, пропускаю файл')
        return None

    # Ищем конец таблицы (сноска Информация)
    end_idx = df_data[ df_data[df_data.columns[0]].str.contains('Информация') == True ].index
    n_end = df_data.shape[0]+1 if end_idx.empty else end_idx[0]

    # Формируем набор
    # До 05.2021 файл включает следующие колонки:
    #   - Субъект/регион
    #   - Всего смертей
    #   - COVID-19, вирус идентифицирован
    #   - возможно, COVID-19, вирус не идентифицирован
    # После 05.2021 структура следующая:
    #   - Субъект/регион
    #   - Всего	2021 г.
    #   - Всего	2020 г.
    #   - 2021 в % к 2020 г.
    # 	- COVID-19, вирус идентифицирован 2021 г.
    # 	- COVID-19, вирус идентифицирован 2020 г.
    #   - 2021 в % к 2020 г.
    #   - возможно, COVID-19, вирус не идентифицирован 2021 г.
    #   - возможно, COVID-19, вирус не идентифицирован 2020 г.
    #   - 2021 в % к 2020 г.
    cols = list(range(4)) if period[0] < 5 or period[1] < 2021 else \
           [0, 1, 4, 7]
    df = df_data.iloc[n_start:n_end, cols]
    df.columns = ['subject', 'total_deaths', 'confirmed_covid_deaths', 'possible_covid_deaths']
    df['name_key'] = df['subject'].str.replace(' ','').str.upper()
    df['period'] = f'{period[1]:04d}-{period[0]:02d}-01'

    # Нумифицируем колонки
    df['total_deaths'] = df['total_deaths'].astype('float')
    df['confirmed_covid_deaths'] = df['confirmed_covid_deaths'].astype('float')
    df['possible_covid_deaths'] = df['possible_covid_deaths'].astype('float')

    # Отбрасываем summary записи по субъектам и РФ
    df_reg, df_fo = get_reg_map()
    df_check = df_fo[ df_fo['summary'] ].merge(df.reset_index(), how='inner', on='name_key')
    if not df_check.empty:
        df.drop(df_check['index'], inplace=True)

    # Объединяем со списком регионов, проверяем пропущенные
    df = df.merge(df_reg, how='left', on='name_key')
    df_check = df[ df['reg_name'].isna() ]
    if not df_check.empty:
        print(f'--> Не найден маппинг регионов: {",".join(df_check["subject"].to_list())}')

    # Объединяем со списком ФО, проверяем пропущенные
    df = df.merge(df_fo, how='left', on='fo_code')
    df_check = df[ df['fo_name'].isna() ]
    if not df_check.empty:
        print(f'--> Не найден маппинг ФО: {",".join(df_check["fo_code"].to_list())}')

    print(f'--> Готово, {df.shape[0]} записей сохранено')
    return df.drop(columns=['name_key_x', 'name_key_y', 'reg_name', 'fo_code', 'summary'])

def main():
    # Собираем данные в виде списка
    data_list = [process_file(str(f)) for f in Path('data').glob('*.xlsx')]

    # Исключаем None
    data_list_filtered = filter(lambda x: x is not None, data_list)

    # Собираем в единый dataframe
    df_data = reduce(lambda x, y: x.append(y) if x is not None else y, data_list_filtered)
    print(f'Итоговое количество записей: {df_data.shape[0]}')
    
    # Сохраняем в файл
    df_data.sort_values(['subject', 'period'], inplace=True)
    df_data.to_excel('covid_russia.xlsx', index=False)
    print('Сводный файл обновлен')

if __name__ == '__main__':
    main()
