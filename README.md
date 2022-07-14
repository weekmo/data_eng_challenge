# Data Engineering
## Challenge 1 - ETL

### Task

The german weather service (DWD) provides daily weather data from their weather stations.
Your task is to write an ETL (extract, transform, load) script that, for a given station ID, downloads the temperature data and stores it into a SQL database.

### Materials

DWD provides the data we are interested in on an FTP Server: https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/

A detailled description of the data can be found here: https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/DESCRIPTION_obsgermany_climate_daily_kl_historical_en.pdf

For portability reasons please write your data into the attached file-based SQLite database: `temp.sqlite`
This gives you already the desired data model and an example entry.

### Requirements

1. Write an ETL script that for a given set of station ids, adds the dwd station to the database & all temperature data of this station.
2. We are interested in the variable: "TMK daily mean of temperature °C" and in stations 00078, 00090, 05426, 05906 
3. Populate the database using your script
4. Document you script 
5. On interview day present your solution and the key concepts you used.

## Challenge 2 - API

### Task

We have a SQL database for observed weather measurements.
Your task is expose a SQL database in a RESTful API in order to enable FAIR (Findable, Accessible, Interoperable & Reusable) principles.

### Materials

We provide you a file-based SQLite database: `api.sqlite`.

You are free in your technology choice. 


### Requirements

The API should expose these 4 endpoints:

1. GET /stations: return all stations
2. GET /measurements/{stationID}: return all measurements for a give stationID
3. GET /stations/find: Returns the stationID of the nearest stations for a give location (query parameters: latitude & logitude given in degrees)
4. POST /stations: add a new entry to the stations table and returns the stationID of the newly added station

Please document your API according to standards and document also your code.
On interview day present your solution and the key concepts you used.
