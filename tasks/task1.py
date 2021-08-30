# -*- coding: utf-8 -*-
"""
Created on Tue Aug 17 17:53:02 2021

@author: HassaM3
"""

# I use SQLite lib for simplication
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO
import re
import sqlite3 as sql
from bs4 import BeautifulSoup

"""
Params
"""

def fill_stations(url, station_ids,database_link):
    """
    Fill stations table with stations data

    Parameters
    ----------
    url : str
        DESCRIPTION.
    station_ids : list
        DESCRIPTION.
    database_link : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
    
    
    """ Vars """
    colspecs = [(0,5),(6,14),(15,23),(23,38),(38,50),(50,60),(60,101),(101,None)]
    stations_columns=["id","date_from","date_to","station_height","latitude","longitude","name","state"]
    
    """ Get stations data """
    df_st_data = pd.read_fwf(url, encoding='cp1252', colspecs=colspecs, skiprows=[0,1], names=stations_columns)
    
    """ Filter columns """
    df_st_sub_data = df_st_data[["id","name","latitude","longitude"]]
    
    """ Filter rows """
    df_st_sub_data = df_st_sub_data[df_st_sub_data["id"].isin(station_ids)]
    
    """ Write to database """
    with sql.connect(database_link) as conn:
        df_st_sub_data.to_sql('stations', con=conn, if_exists='replace', index=False)



def fill_measurements(url, station_ids,database_link):
    """
    Fill measurements table

    Parameters
    ----------
    url : str
        DESCRIPTION.
    station_ids : list
        DESCRIPTION.
    database_link : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
    
    
    """ Vars """
    cert = "C:/Users/HassaM3/certs/cacert.pem"
    produkt_pattern = re.compile("produkt?")
    column_names = {'MESS_DATUM':'id','STATIONS_ID':'station_id','TMK':'value'}
    
    """ Get all links from datasource """
    respond = requests.get(url,verify=cert, stream=True)
    soup = BeautifulSoup(respond.text, 'html.parser')
    links = [link.get('href') for link in soup.find_all('a')]
    
    """ Loop through desired stations """
    df = pd.DataFrame()
    for idx in station_ids:
        station_link_pattern = re.compile(f"tageswerte_KL_{idx:05d}_?")
        filtered_link = list(filter(station_link_pattern.match,links))
        #print(filtered_link)
        
        """ Get zip file """
        respond = requests.get(url+filtered_link[0],verify=cert, stream=True)
        
        """ Open zipped file and read measurments data """
        with ZipFile(BytesIO(respond.content), 'r') as zip_file:
            filtered_list = list(filter(produkt_pattern.match, zip_file.namelist()))
            if filtered_list:
                
                """ Read data in DataFrame """
                df_data = pd.read_csv(BytesIO(zip_file.read(filtered_list[0])), sep=';')
                
                """ Clean columns names"""
                df_data.rename(columns=lambda column:column.strip(), inplace=True)
                
                """ Fliter columns """
                df_data = df_data[column_names.keys()]
                
                """ manipulate columns name to match table fields"""
                df_data.rename(columns=column_names, inplace=True)
                df_data['parameter'] = "temp_2m"
                df_data['date'] = pd.to_datetime(df_data['id'], format='%Y%m%d').dt.date
                
                df = df.append(df_data)
          
    """ Write to database """
    with sql.connect(database_link) as conn:
        df.to_sql('measurements', con=conn, if_exists='replace', index=False)
