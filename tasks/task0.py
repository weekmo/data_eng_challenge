# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 16:32:21 2021

@author: HassaM3
"""
'''
import numpy as np
from sklearn.neighbors import KDTree

rng = np.random.RandomState(0)
print(rng)
X = rng.random_sample((10, 3))  # 10 points in 3 dimensions
print(X)
tree = KDTree(X, leaf_size=2)              
dist, ind = tree.query(X[:1], k=1)                
print(ind)  # indices of 3 closest neighbors
print(X[:1])
print(dist)
print(X)  # distances to 3 closest neighbors
print(X[0])

'''
'''
# Leading zeros
for i in station_ids:
    print(str(i).zfill(5))
    print(f"{i:05d}")

# Find file name
def find_item_name(name_list, pattern):
    for item in name_list:
        if item.startswith(pattern):
            return item
    return None

with ZipFile(BytesIO(respond.content),'r') as zip_file:
    data_file_name = find_item_name(zip_file.namelist(),"produkt")
    if data_file_name != None:
        df_data = pd.read_csv(BytesIO(zip_file.read(data_file_name)))
'''
import requests
from bs4 import BeautifulSoup
import re
import airflow

root_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/"


cert = "C:/Users/HassaM3/certs/cacert.pem"
respond = requests.get(root_url,verify=cert, stream=True)
soup = BeautifulSoup(respond.text, 'html.parser')
links = [link.get('href') for link in soup.find_all('a')]

r = re.compile("tageswerte_KL_00090_*")
new_links = list(filter(r.match,links))
print(new_links)
