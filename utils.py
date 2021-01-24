import os 
import pandas as pd 
from collections import defaultdict
import json
    
data_directory = os.path.join(os.getcwd(), 'data')

DATE_COLUMN = 'date'
OPEN_COLUMN = 'open'
CLOSE_COLUMN = 'close'


def create_merged_csv(symbols):
    df_merged = pd.DataFrame()
    date_added = False

    for symbol in symbols:
        filename = '%s.csv' % symbol
        filepath = os.path.join(data_directory, filename)
        print('reading filepath = ', filepath)
        df = pd.read_csv(filepath)
        if not date_added:
            df_merged[DATE_COLUMN] = df[DATE_COLUMN]
            date_added = True
        df_merged['%s_OPEN' % symbol] = df[OPEN_COLUMN]
        df_merged['%s_CLOSE' % symbol] = df[CLOSE_COLUMN]
        
    dict_year_to_open_data = defaultdict(list)
    dict_year_to_close_data = defaultdict(list)

    for _, row in df_merged.iterrows():
        year = row[DATE_COLUMN].split('-')[0]
        
        
        open_data = {'Date': row[DATE_COLUMN]}
        close_data = {'Date': row[DATE_COLUMN]}
        
        for symbol in symbols:
            open_data[symbol] = row['%s_OPEN' % symbol]
            close_data[symbol] = row['%s_CLOSE' % symbol]

        file_year = get_file_year_for_data_year(year)
        dict_year_to_open_data[file_year].append(open_data)
        dict_year_to_close_data[file_year].append(close_data)   

    for key, value in dict_year_to_open_data.items():
        pd.DataFrame.from_dict(value).to_csv(os.path.join(data_directory, 'Open-%s.csv' % key), index=False)

    for key, value in dict_year_to_close_data.items():
        pd.DataFrame.from_dict(value).to_csv(os.path.join(data_directory, 'Close-%s.csv' % key), index=False)

def get_file_year_for_data_year(data_year):
    dict_data_year_to_file_year = {
        '2016': '2016',
        '2017': '2016',
        '2018': '2016',
        '2019': '2016',
        '2020': '2020',
        '2021': '2021',
    }
    return dict_data_year_to_file_year[data_year]

def store_symbols(dict_zerodha_token_to_symbol):
    with open('data/symbols.json','w+') as symbols_pickle_file:
        json.dump(dict_zerodha_token_to_symbol, symbols_pickle_file)

def get_symbols():
    with open('data/symbols.json', 'r') as symbols_pickle_file:
        return json.load(symbols_pickle_file)

create_merged_csv(["969473","2952193"])
