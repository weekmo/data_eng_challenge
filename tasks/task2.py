# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 07:34:20 2021

@author: HassaM3
"""
# TODO: Docker
# Using database lib for easy migration
# The models has to be in a separet module for easy migration
from typing import List
import databases
import sqlalchemy
from pydantic import BaseModel
from fastapi import FastAPI
from sklearn.neighbors import KDTree
import pandas as pd

""" Prepare database """
DATABASE_URL = "sqlite:///../api.sqlite"
database = databases.Database(DATABASE_URL)

""" prepare tables metadata"""
metadata = sqlalchemy.MetaData()

""" prepare tables"""
stations = sqlalchemy.Table(
    "stations",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.Text),
    sqlalchemy.Column("latitude", sqlalchemy.REAL),
    sqlalchemy.Column("longitude", sqlalchemy.REAL),
    )

measurements = sqlalchemy.Table(
    "measurements",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Text,primary_key = True),
    sqlalchemy.Column("sensor_id", sqlalchemy.Integer),
    sqlalchemy.Column("datetime", sqlalchemy.REAL),
    sqlalchemy.Column("parameter", sqlalchemy.Text),
    sqlalchemy.Column("value", sqlalchemy.REAL),
    sqlalchemy.Column("date", sqlalchemy.Text),
    )

"""Create alchemy engin """
engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args = {"check_same_thread": False}
    )

""" Create meatadata """
metadata.create_all(engine)

""" Data models """
class Station(BaseModel):
    """
    Station model
    """
    name: str
    latitude: float
    longitude: float
    
class Measurement(BaseModel):
    """
    Measurement model
    """
    id: int
    sensor_id: int
    datetime: int
    parameter: str
    value: float
    date: str

""" Create api """
app = FastAPI()

@app.on_event("startup")
async def startup():
    """
    On start connect to datacbase

    Returns
    -------
    None.

    """
    await database.connect()
    
@app.on_event("shutdown")
async def shutdown():
    """
    Disconnect database on shutdown

    Returns
    -------
    None.

    """
    await database.disconnect()


# TODO: GET /stations: return all stations
@app.get("/stations")
async def get_stations():
    """
    Get all stations data

    Returns
    -------
    list
        return all stations data

    """
    query = stations.select()
    return await database.fetch_all(query)
    
# TODO: GET /measurements/{stationID}: return all measurements for a give stationID
@app.get("/measurements/{station_id}",response_model=List[Measurement])
async def get_measurement(station_id: int):
    """
    Get measurements by stations ID

    Parameters 
    ----------
    station_id : int
        DESCRIPTION.

    Returns
    -------
    list
        return all Measurements

    """
    query = measurements.select().where(measurements.c.sensor_id == station_id)
    return await database.fetch_all(query)

# TODO: GET /stations/find: Returns the stationID of the nearest stations for a give location (query parameters: latitude & longitude given in degrees)
@app.get("/stations/find")
async def get_find_station(latitude:float, longitude:float):
    """
    Find nearest station

    Parameters
    ----------
    latitude : float
        DESCRIPTION.
    longitude : float
        DESCRIPTION.

    Returns
    -------
    str
        return nearest station ID.

    """
    query = stations.select()
    records = await database.fetch_all(query)
    df = pd.DataFrame(records,columns=["stationID","name","latitude","longitude"])
    tree = KDTree(df[["latitude","longitude"]].values, metric='euclidean')
    ind = tree.query([[latitude,longitude]], k=1,return_distance=False)
    return str(df.iloc[ind[0,0]]["stationID"]).zfill(5)

# TODO: POST /stations: add a new entry to the stations table and returns the stationID of the newly added station
@app.post("/stations")
async def post_station(station: Station):
    """
    Add a new station

    Parameters
    ----------
    station : Station
        DESCRIPTION.

    Returns
    -------
    str
        returns the stationID of the newly added station

    """
    #.returning('id')
    id = await database.fetch_one('select MAX(id) from stations')
    id=id[0]+1
    query = stations.insert().values(
        id=id,
        name=station.name,
        latitude = station.latitude,
        longitude = station.longitude
        )
    
    await database.execute(query)
    
    return str(id).zfill(5)

    