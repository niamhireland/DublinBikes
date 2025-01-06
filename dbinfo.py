import json

# Read from txt file
filepath = "../pw.txt"
with open(filepath, 'r') as file:
    json_data = json.load(file)

# API for bike station data
JCKEY = json_data["JCKEY"]
CITYNAME = "dublin"
JC_URI = "https://api.jcdecaux.com/vls/v1/stations"

# Database infomation
USER = json_data["USER"]
DB_URI = json_data["DB_URI"]
DB_PASSWORD = json_data["DB_PASSWORD"]
PORT = "3306"
DBNAME = "dbikes"           # database
TNAME1 = "stations"         # table for static data of bike stations
TNAME2 = "availability"     # table for real-time data of availability at stations
TNAME3 = "current"          # table for current and historical weather data
TNAME4 = "forcast_h"        # table for hourly weather forcast data
TNAME5 = "forcast_d"        # table for daily weather forcast data

# API for weather data
WKEY = json_data["WKEY"]
W_C_URI = "https://api.openweathermap.org/data/2.5/weather"
W_HF_URI = "https://pro.openweathermap.org/data/2.5/forecast/hourly"
W_DF_URI = "https://pro.openweathermap.org/data/2.5/forecast/daily"
LAT = 53.344
LON = -6.2672
CITY_ID = 2964574

# API for Google Map
GMKEY = json_data["GMKEY"]