from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
from .Notation import *

# search company by name, return ISIN code or None : An ISIN (International Securities Identification Number) is a 12-character alphanumeric code that uniquely identifies a specific security globally.
def get_code(name):
    notation__code = notation_code()
    for act in notation__code:
        if act['name'] == name:
            code = act['ISIN']
            return code
    return None

def get_valeur(name):
    if name == "MASI":
        print("invalid arg: MASI")
        return None
    value = notation_value()
    return value[name]

# parse data from bs4, pass it to DataFrame, making str date as index
def get_data(soup, decode):
    table = json.loads(soup.text.encode().decode(decode)) # converting from string to object(dictionary)
    row_data = pd.DataFrame(table['result'])
    row_data.columns = ["Date", "Value", "Min", "Max", "variation", "volume"]
    date = row_data['Date']
    row_data.drop(['Date'], axis=1, inplace=True)
    row_data.index = date
    return row_data

# parse data bs4, pass the first element to DataFrame, make label as index
def intradata(soup, decode):
    table = json.loads(soup.text.encode().decode(decode))
    row_data = pd.DataFrame(table['result'][0])
    index = row_data['labels'].values
    row_data.drop(['labels'], axis=1, inplace=True)
    row_data.index = index
    row_data.columns = ["Value"]
    return row_data

# similar to get_data but index is a timestamp not string
def get_index(soup, decode):
    table = json.loads(soup.text.encode().decode(decode))
    row_data = pd.DataFrame(table['result'])
    date = row_data['Date']
    row_data.drop(['Date'], axis=1, inplace=True)
    row_data.index = date.apply(lambda x: datetime.datetime.fromtimestamp(x).date())
    row_data.columns = ['Value']
    return row_data

# like slicing the data
def produce_data(data, start, end):
    start = pd.to_datetime(start).date()
    end = pd.to_datetime(end).date()
    return data.loc[start:end]


def getTables(soup):
    tabs = ['table1', 'table6', 'table7', 'table4']
    result = dict()
    for tab in tabs:
        tbl = soup.find(id=tab).find_all('span')
        tbl = [ x.get_text() for x in tbl]








    