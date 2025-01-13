# "lat":53.3498006,"lon":-6.2602964
# https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}
# Weather Icon Img: https://openweathermap.org/img/wn/10d@2x.png
# Current data update: apprx 10-20 min

import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import traceback
# import schedule
import dbinfo

def log_message(type, message):
    with open('log_db_weather.txt', 'a') as f:
        f.write(f'{{"dt": "{datetime.now()}", "type": "{type}", "message": "{message}"}}' + '\n')

def create_database():
    try:
        # Create database engine
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                       dbinfo.PORT, dbinfo.DBNAME))
        with engine.connect() as connection:
            # SQL query to create tables for current weather
            query = f"""
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME3} (
                dt INTEGER PRIMARY KEY, 
                weather_id INTEGER, 
                weather_main VARCHAR(64), 
                weather_des VARCHAR(64), 
                weather_icon VARCHAR(16), 
                temp FLOAT, 
                feels_like FLOAT, 
                wind_speed FLOAT, 
                cloudiness INTEGER, 
                visibility INTEGER, 
                rain_1h FLOAT)
            """
            # Execute the SQL query
            connection.execute(text(query))
            # SQL query to create tables for hourly weather forcast
            query = f"""
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME4} (
                dt INTEGER PRIMARY KEY, 
                weather_id INTEGER, 
                weather_main VARCHAR(64), 
                weather_des VARCHAR(64), 
                weather_icon VARCHAR(16), 
                temp FLOAT, 
                feels_like FLOAT, 
                wind_speed FLOAT, 
                cloudiness INTEGER, 
                visibility INTEGER, 
                pop FLOAT, 
                rain_1h FLOAT)
            """
            # Execute the SQL query
            connection.execute(text(query))
            # SQL query to create tables for daily weather forcast
            query = f"""
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME5} (
                dt INTEGER PRIMARY KEY, 
                weather_id INTEGER, 
                weather_main VARCHAR(64), 
                weather_des VARCHAR(64), 
                weather_icon VARCHAR(16), 
                temp_day FLOAT, 
                temp_eve FLOAT, 
                temp_night FLOAT, 
                temp_morn FLOAT, 
                temp_min FLOAT, 
                temp_max FLOAT, 
                feels_like_day FLOAT, 
                feels_like_eve FLOAT, 
                feels_like_night FLOAT, 
                feels_like_morn FLOAT, 
                wind_speed FLOAT, 
                cloudiness INTEGER, 
                pop FLOAT, 
                rain FLOAT)
            """
            # Execute the SQL query
            connection.execute(text(query))
    except :
        log_message("Error", f"{traceback.format_exc()}")

def collect_current():
    try:
        # Fetch data from the API for current weather
        r = requests.get(dbinfo.W_C_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
        if r.status_code == 200:
            data = json.loads(r.text)
            # Prepare the data
            dt = data['dt']
            weather_id = data['weather'][0]['id']
            weather_main = data['weather'][0]['main']
            weather_des = data['weather'][0]['description']
            weather_icon = data['weather'][0]['icon']
            temp = data['main']['temp']
            feels_like = data['main']['feels_like']
            wind_speed = data['wind']['speed']
            cloudiness = data['clouds']['all']
            visibility = data['visibility']
            if "rain" in data and "1h" in data['rain']:
                rain_1h = data['rain']['1h']
            else:
                rain_1h = 0.0
            # Create database engine
            engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                           dbinfo.PORT, dbinfo.DBNAME))
            # Open a connection
            with engine.connect() as connection:
                # SQL query to insert into the table for current weather
                query = f"""
                INSERT INTO {dbinfo.TNAME3} (dt, weather_id, weather_main, weather_des, weather_icon, temp, feels_like, wind_speed, 
                cloudiness, visibility, rain_1h) 
                VALUES (:dt, :weather_id, :weather_main, :weather_des, :weather_icon, :temp, :feels_like, :wind_speed, :cloudiness, 
                :visibility, :rain_1h)
                ON DUPLICATE KEY UPDATE 
                weather_id = VALUES(weather_id), weather_main = VALUES(weather_main), weather_des = VALUES(weather_des), 
                weather_icon = VALUES(weather_icon), temp = VALUES(temp), feels_like = VALUES(feels_like), 
                wind_speed = VALUES(wind_speed), cloudiness = VALUES(cloudiness), visibility = VALUES(visibility), 
                rain_1h = VALUES(rain_1h)
                """
                # Execute the SQL query
                connection.execute(text(query), dt=dt, weather_id=weather_id, weather_main=weather_main, weather_des=weather_des, 
                                   weather_icon=weather_icon, temp=temp, feels_like=feels_like, wind_speed=wind_speed, 
                                   cloudiness=cloudiness, visibility=visibility, rain_1h=rain_1h)
            log_message("Success", "1 row inserted/updated.")
    except:
        log_message("Error", f"{traceback.format_exc()}")

def collect_forcast_h():
    try:
        # Fetch data from the hourly forcast API
        r = requests.get(dbinfo.W_HF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
        if r.status_code == 200:
            data = json.loads(r.text)
            # Counters for tracking the number of collected and inserted/updated rows
            n = data['cnt']
            j = 0
            # Create database engine
            engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                           dbinfo.PORT, dbinfo.DBNAME))
            # Open a connection
            with engine.connect() as connection:
                # SQL query to insert into/update the table for hourly weather forcast
                query = f"""
                INSERT INTO {dbinfo.TNAME4} (dt, weather_id, weather_main, weather_des, weather_icon, temp, feels_like, wind_speed, 
                cloudiness, visibility, pop, rain_1h) 
                VALUES (:dt, :weather_id, :weather_main, :weather_des, :weather_icon, :temp, :feels_like, :wind_speed, :cloudiness, 
                :visibility, :pop, :rain_1h)
                ON DUPLICATE KEY UPDATE 
                weather_id = VALUES(weather_id), weather_main = VALUES(weather_main), weather_des = VALUES(weather_des), 
                weather_icon = VALUES(weather_icon), temp = VALUES(temp), feels_like = VALUES(feels_like), 
                wind_speed = VALUES(wind_speed), cloudiness = VALUES(cloudiness), visibility = VALUES(visibility),
                pop = VALUES(pop), rain_1h = VALUES(rain_1h)
                """
                try:
                    for hourly in data['list']:
                        # Prepare the data
                        dt = hourly['dt']
                        weather_id = hourly['weather'][0]['id']
                        weather_main = hourly['weather'][0]['main']
                        weather_des = hourly['weather'][0]['description']
                        weather_icon = hourly['weather'][0]['icon']
                        temp = hourly['main']['temp']
                        feels_like = hourly['main']['feels_like']
                        wind_speed = hourly['wind']['speed']
                        cloudiness = hourly['clouds']['all']
                        visibility = hourly['visibility']
                        pop = hourly['pop']
                        if "rain" in hourly and "1h" in hourly['rain']:
                            rain_1h = hourly['rain']['1h']
                        else:
                            rain_1h = 0.0
                        # Execute the SQL query
                        connection.execute(text(query), dt=dt, weather_id=weather_id, weather_main=weather_main, weather_des=weather_des, 
                                        weather_icon=weather_icon, temp=temp, feels_like=feels_like, wind_speed=wind_speed, 
                                        cloudiness=cloudiness, visibility=visibility, pop=pop, rain_1h=rain_1h)
                        # Increment row counter
                        j += 1
                    log_message("Success", f"{n} timestamps returned by API; {j} timestamps inserted/updated.")
                except:
                    log_message("Error", f"{traceback.format_exc()}")
    except:
        log_message("Error", f"{traceback.format_exc()}")

def collect_forcast_d():
    try:
        # Fetch data from the daily forcast API
        r = requests.get(dbinfo.W_DF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric", "cnt": "16"})
        if r.status_code == 200:
            data = json.loads(r.text)
            # Counters for tracking the number of collected and inserted/updated rows
            n = data['cnt']
            j = 0
            # Create database engine
            engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                           dbinfo.PORT, dbinfo.DBNAME))
            # Open a connection
            with engine.connect() as connection:
                # SQL query to insert into/update the table for hourly weather forcast
                query = f"""
                INSERT INTO {dbinfo.TNAME5} (dt, weather_id, weather_main, weather_des, weather_icon, temp_day, temp_eve, temp_night,
                temp_morn, temp_min, temp_max, feels_like_day, feels_like_eve, feels_like_night, feels_like_morn, wind_speed, 
                cloudiness, pop, rain) 
                VALUES (:dt, :weather_id, :weather_main, :weather_des, :weather_icon, :temp_day, :temp_eve, :temp_night, :temp_morn, 
                :temp_min, :temp_max, :feels_like_day, :feels_like_eve, :feels_like_night, :feels_like_morn, :wind_speed, :cloudiness, 
                :pop, :rain)
                ON DUPLICATE KEY UPDATE 
                weather_id = VALUES(weather_id), weather_main = VALUES(weather_main), weather_des = VALUES(weather_des), 
                weather_icon = VALUES(weather_icon), temp_day = VALUES(temp_day), temp_eve = VALUES(temp_eve), 
                temp_night = VALUES(temp_night), temp_morn = VALUES(temp_morn), temp_min = VALUES(temp_min), temp_max = VALUES(temp_max), 
                feels_like_day = VALUES(feels_like_day), feels_like_eve = VALUES(feels_like_eve), feels_like_night = VALUES(feels_like_night), 
                feels_like_morn = VALUES(feels_like_morn), wind_speed = VALUES(wind_speed), cloudiness = VALUES(cloudiness), 
                pop = VALUES(pop), rain = VALUES(rain)
                """
                try:
                    for daily in data['list']:
                        # Prepare the data
                        dt = daily['dt']
                        weather_id = daily['weather'][0]['id']
                        weather_main = daily['weather'][0]['main']
                        weather_des = daily['weather'][0]['description']
                        weather_icon = daily['weather'][0]['icon']
                        temp_day = daily['temp']['day']
                        temp_eve = daily['temp']['eve']
                        temp_night = daily['temp']['night']
                        temp_morn = daily['temp']['morn']
                        temp_min = daily['temp']['min']
                        temp_max = daily['temp']['max']
                        feels_like_day = daily['feels_like']['day']
                        feels_like_eve = daily['feels_like']['eve']
                        feels_like_night = daily['feels_like']['night']
                        feels_like_morn = daily['feels_like']['morn']
                        wind_speed = daily['speed']
                        cloudiness = daily['clouds']
                        pop = daily['pop']
                        if "rain" in daily:
                            rain = daily['rain']
                        else:
                            rain = 0.0
                        # Execute the SQL query
                        connection.execute(text(query), dt=dt, weather_id=weather_id, weather_main=weather_main, weather_des=weather_des, 
                                        weather_icon=weather_icon, temp_day=temp_day, temp_eve=temp_eve, temp_night=temp_night, 
                                        temp_morn=temp_morn, temp_min=temp_min, temp_max=temp_max, feels_like_day=feels_like_day, 
                                        feels_like_eve=feels_like_eve, feels_like_night=feels_like_night, feels_like_morn=feels_like_morn, 
                                        wind_speed=wind_speed, cloudiness=cloudiness, pop=pop, rain=rain)
                        # Increment row counter
                        j += 1
                    log_message("Success", f"{n} timestamps returned by API; {j} timestamps inserted/updated.")
                except:
                    log_message("Error", f"{traceback.format_exc()}")
    except:
        log_message("Error", f"{traceback.format_exc()}")

def main():
    # Prepare the database
    create_database()
    # Fetch and insert/update data
    collect_current()
    collect_forcast_h()
    collect_forcast_d()
    

if __name__ == "__main__":
    main()
