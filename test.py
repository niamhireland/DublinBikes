from flask import Flask, render_template
from sqlalchemy import create_engine, text
import dbinfo
import json
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import datetime
import time
import traceback
import schedule
import numpy as np
import pickle
import dbquery
import pandas as pd

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                dbinfo.PORT, dbinfo.DBNAME))

# with engine.connect() as connection:
#     queryAvailability = f"SELECT * FROM {dbinfo.TNAME2}"
#     allRows = connection.execute(text(queryAvailability)).fetchall()
#     connection.commit()

# fetchall() returns a list of pyodbc.Row objects.
# Row objects are similar to tuples but they are not JSON serializable.

stationNumber = 11

allRows_converted = []

# for row in allRows:
    
#     if row[0] == stationNumber:
#         # Convert the row into a tuple and append it to the new list
#         tuple_converted_row = tuple(row)
#         allRows_converted.append(tuple_converted_row)


# availabiltiyJSON = json.dumps(allRows_converted)

# print(availabiltiyJSON)


import schedule
import dbinfo

# # r = requests.get(dbinfo.W_HF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
# # data = json.loads(r.text)
# # print(data)

# # for hourly in data['list']:
# #     # Prepare the data
# #     dt = hourly['dt']
# #     weather_id = hourly['weather'][0]['id']
# #     weather_main = hourly['weather'][0]['main']
# #     weather_des = hourly['weather'][0]['description']
# #     weather_icon = hourly['weather'][0]['icon']
# #     temp = hourly['main']['temp']
# #     feels_like = hourly['main']['feels_like']
# #     wind_speed = hourly['wind']['speed']
# #     cloudiness = hourly['clouds']['all']
# #     visibility = hourly['visibility']
# #     pop = hourly['pop']
# #     if "rain" in hourly and "1h" in hourly['rain']:
# #         rain_1h = hourly['rain']['1h']
# #     else:
# #         rain_1h = 0.0

# #     print(datetime.fromtimestamp(dt))
    
 



TNAME3 = "current"          # table for current and historical weather data
TNAME4 = "forcast_h"        # table for hourly weather forcast data
TNAME5 = "forcast_d"        # table for daily weather forcast data

  


# elif tname == dbinfo.TNAME3:
#     r = requests.get(dbinfo.W_C_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
# elif tname == dbinfo.TNAME4:
#     r = requests.get(dbinfo.W_HF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
# elif tname == dbinfo.TNAME5:
#     r = requests.get(dbinfo.W_DF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric", "cnt": "16"})



# r = requests.get(dbinfo.W_HF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
# data = json.loads(r.text)
# print(data)

# for daily in data['list']:
#     # Prepare the data
#     dt = daily['dt']
#     weather_id = daily['weather'][0]['id']
#     weather_main = daily['weather'][0]['main']
#     weather_des = daily['weather'][0]['description']
#     weather_icon = daily['weather'][0]['icon']
#     temp_day = daily['temp']['day']
#     temp_eve = daily['temp']['eve']
#     temp_night = daily['temp']['night']
#     temp_morn = daily['temp']['morn']
#     temp_min = daily['temp']['min']
#     temp_max = daily['temp']['max']
#     feels_like_day = daily['feels_like']['day']
#     feels_like_eve = daily['feels_like']['eve']
#     feels_like_night = daily['feels_like']['night']
#     feels_like_morn = daily['feels_like']['morn']
#     wind_speed = daily['speed']
#     cloudiness = daily['clouds']
#     pop = daily['pop']
#     if "rain" in daily:
#         rain = daily['rain']
#     else:
#         rain = 0.0

#     print(data)

# def predict(id, month, day, hour):
#     """Returns data: predicted bike&stand availability for a given station"""
#     # Obtain model
#     with open(f'models/model_{id}.pkl', 'rb') as f:
#         model = pickle.load(f)
#     # Prepare input data
#     forcast = json.loads(dbquery.get_weather_forcast(engine, month, day, hour))
#     weekday = datetime.datetime(2024, month, day).weekday()
#     bike_stands = dbquery.get_number_stands(engine, id)
#     temp = forcast.get('temp')
#     feels_like = forcast.get('feels_like')
#     wind_speed = forcast.get('wind_speed')
#     cloudiness = forcast.get('cloudiness')
#     visibility = forcast.get('visibility')
#     pop = forcast.get('pop')
#     rain_1h = forcast.get('rain_1h')
#     if forcast.get('weather_main') == 'Clouds':
#         weather_main_Clouds = True
#         weather_main_Rain = False
#     elif forcast.get('weather_main') == 'Rain':
#         weather_main_Clouds = False
#         weather_main_Rain = True
#     else:
#         weather_main_Clouds = False
#         weather_main_Rain = False
#     if forcast.get('weather_des') == 'clear sky':
#         weather_des_clear_sky = True
#         weather_des_few_clouds = False
#         weather_des_light_rain = False
#         weather_des_moderate_rain = False
#         weather_des_overcast_clouds = False
#         weather_des_scattered_clouds = False
#     elif forcast.get('weather_des') == 'few clouds':
#         weather_des_clear_sky = False
#         weather_des_few_clouds = True
#         weather_des_light_rain = False
#         weather_des_moderate_rain = False
#         weather_des_overcast_clouds = False
#         weather_des_scattered_clouds = False
#     elif forcast.get('weather_des') == 'light rain':
#         weather_des_clear_sky = False
#         weather_des_few_clouds = False
#         weather_des_light_rain = True
#         weather_des_moderate_rain = False
#         weather_des_overcast_clouds = False
#         weather_des_scattered_clouds = False
#     elif forcast.get('weather_des') == 'moderate rain':
#         weather_des_clear_sky = False
#         weather_des_few_clouds = False
#         weather_des_light_rain = False
#         weather_des_moderate_rain = True
#         weather_des_overcast_clouds = False
#         weather_des_scattered_clouds = False
#     elif forcast.get('weather_des') == 'overcast clouds':
#         weather_des_clear_sky = False
#         weather_des_few_clouds = False
#         weather_des_light_rain = False
#         weather_des_moderate_rain = False
#         weather_des_overcast_clouds = True
#         weather_des_scattered_clouds = False
#     elif forcast.get('weather_des') == 'scattered clouds':
#         weather_des_clear_sky = False
#         weather_des_few_clouds = False
#         weather_des_light_rain = False
#         weather_des_moderate_rain = False
#         weather_des_overcast_clouds = False
#         weather_des_scattered_clouds = True
#     else:
#         weather_des_clear_sky = False
#         weather_des_few_clouds = False
#         weather_des_light_rain = False
#         weather_des_moderate_rain = False
#         weather_des_overcast_clouds = False
#         weather_des_scattered_clouds = False
#     # Prepare the input dataframe
#     inputs = pd.DataFrame()
#     feature_names = ['temp', 'feels_like', 'wind_speed', 'cloudiness', 'visibility', 'pop', 'rain_1h', 'month', 'day', 'weekday', 'hour', 
#                      'weather_main_Clouds', 'weather_main_Rain', 'weather_des_clear sky', 'weather_des_few clouds', 'weather_des_light rain', 
#                      'weather_des_moderate rain', 'weather_des_overcast clouds', 'weather_des_scattered clouds']
#     feature_values = [temp, feels_like, wind_speed, cloudiness, visibility, pop, rain_1h, month, day, weekday, hour, weather_main_Clouds, 
#                         weather_main_Rain, weather_des_clear_sky, weather_des_few_clouds, weather_des_light_rain, weather_des_moderate_rain, 
#                         weather_des_overcast_clouds, weather_des_scattered_clouds]
#     for feature_name, value in zip(feature_names, feature_values):
#         inputs.loc[0, feature_name] = value
#     # Predict with model
#     bikes_predicted = int(round(model.predict(inputs)[0],0))
#     stands_predicted = bike_stands - bikes_predicted
#     if bikes_predicted < 0:
#         bikes_predicted = 0
#         stands_predicted = bike_stands
#     elif bikes_predicted > bike_stands:
#         bikes_predicted = bike_stands
#         stands_predicted = 0
#     response_dict = {'predicted_bikes': bikes_predicted, 'predicted_stands': stands_predicted}
#     response_json = json.dumps(response_dict)

#     return response_json

# test0411 = predict(1, 2, 28, 19)
# print(test0411)
# print(dbquery.get_weather_forcast(engine, 2, 28, 19))
hour = 16
id = 19
pattern_hour = json.loads(dbquery.get_pattern_hour(engine, id))
for hour_data in pattern_hour:
    if hour_data['hour'] == hour:
        bikes_predicted = int(round(hour_data['avg_available_bikes']))
        stands_predicted = int(round(hour_data['avg_available_stands']))
        break
response_dict = {'bikes_predicted': bikes_predicted, 'stands_predicted': stands_predicted}
response_json = json.dumps(response_dict)
print(response_json)