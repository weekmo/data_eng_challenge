# -*- coding: utf-8 -*-
"""
Created on Thu Aug 19 08:45:36 2021

@author: HassaM3
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 19 02:25:41 2021

@author: HassaM3
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt

from tasks.task1 import fill_stations, fill_measurements


"""
Task 1
"""
station_ids = [78, 90, 5426, 5906]
root_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/"
database_link = 'temp.sqlite'  

default_args = {
    'owner': 'Mohammed',
    'stargt_date':dt.datetime(2021, 8,19),
    'retries': 3,
    'retry_delay': dt.timedelta(hours=3),
    }

measurement_args = {
    'url': root_url,
    'station_ids': station_ids,
    'database_link': database_link
    }

stations_args = {
    'url': root_url+"KL_Tageswerte_Beschreibung_Stationen.txt",
    'station_ids': station_ids,
    'database_link': database_link
    }

with DAG('interview_dag',
         default_args=default_args,
         schedule_interval = dt.timedelta(days=1),
         # '0 * * * *',
         ) as dag:
    fill_stations = PythonOperator(task_id='fill_stations',python_callable=fill_stations,op_kwargs=stations_args)
    fill_measurements = PythonOperator(task_id='fill_measurements',python_callable=fill_measurements,op_kwargs=measurement_args)



"""
Fill stations
"""
stations_link = root_url+"KL_Tageswerte_Beschreibung_Stationen.txt"
fill_stations(stations_link, station_ids,database_link)


"""
Fill measurements
"""
fill_measurements(root_url,station_ids,database_link)
