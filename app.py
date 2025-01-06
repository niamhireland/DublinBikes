from flask import Flask, render_template, jsonify, send_file
from sqlalchemy import create_engine
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import json
import io
import pickle
import numpy as np
import datetime
import pandas as pd
import dbinfo
import dbquery

app = Flask(__name__)

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                               dbinfo.PORT, dbinfo.DBNAME))

@app.route('/')
def index():
    """Returns a html: also passing static info & availability for all stations and current weather"""
    weather = dbquery.get_current_weather(engine)
    stations = dbquery.get_all_stations(engine)
    availability_all = dbquery.get_all_availability(engine)
    return render_template('index.html', stations = stations, weather = weather, availability_all = availability_all, key = dbinfo.GMKEY)


@app.route('/availabilty/<int:id>', methods=['GET'])
def station_detail(id):
    """Returns data: bike&stand availability for a given station"""
    availability = dbquery.get_availability(engine, id)
    return availability


@app.route('/plot/<int:id>', methods=['GET'])
def plot_bike(id):
    """Returns an image: plots showing hourly average bike availability for a given station"""
    data = json.loads(dbquery.get_pattern_hour(engine, id))
    hours = [entry["hour"] for entry in data]
    avg_available_bikes = [entry["avg_available_bikes"] for entry in data]
    avg_available_stands = [entry["avg_available_stands"] for entry in data]
    
    plt.figure(figsize=(3, 7))
    plt.rcParams.update({'font.size': 7})
    plt.subplots_adjust(hspace=0.5)

    # first graph: bike availability
    plt.subplot(2, 1, 1)
    plt.bar(hours, avg_available_bikes, color='skyblue')
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.xlabel('Hour')
    plt.title('Available Bikes per Hour')
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)

    # second graph: stand availability
    plt.subplot(2, 1, 2)
    plt.bar(hours, avg_available_stands, color='skyblue')
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.xlabel('Hour')
    plt.title('Available Stands per Hour')
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)

    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)

    plt.close()

    return send_file(img, mimetype='image/png')


@app.route('/prediction/<int:id>/<int:month>/<int:day>/<int:hour>', methods=['GET'])
def predict(id, month, day, hour): 
    """Returns data: predicted bike&stand availability for a given station"""
    try:
        # Obtain model
        with open(f'models/model_{id}.pkl', 'rb') as f:
            model = pickle.load(f)
        # Prepare input data
        forcast = json.loads(dbquery.get_weather_forcast(engine, month, day, hour))
        weekday = (int)(datetime.datetime(2024, month, day).weekday())
        bike_stands = (int)(dbquery.get_number_stands(engine, id))
        temp = forcast.get('temp')
        feels_like = forcast.get('feels_like')
        wind_speed = forcast.get('wind_speed')
        cloudiness = forcast.get('cloudiness')
        visibility = forcast.get('visibility')
        pop = forcast.get('pop')
        rain_1h = forcast.get('rain_1h')
        if forcast.get('weather_main') == 'Clouds':
            weather_main_Clouds = True
            weather_main_Rain = False
        elif forcast.get('weather_main') == 'Rain':
            weather_main_Clouds = False
            weather_main_Rain = True
        else:
            weather_main_Clouds = False
            weather_main_Rain = False
        if forcast.get('weather_des') == 'clear sky':
            weather_des_clear_sky = True
            weather_des_few_clouds = False
            weather_des_light_rain = False
            weather_des_moderate_rain = False
            weather_des_overcast_clouds = False
            weather_des_scattered_clouds = False
        elif forcast.get('weather_des') == 'few clouds':
            weather_des_clear_sky = False
            weather_des_few_clouds = True
            weather_des_light_rain = False
            weather_des_moderate_rain = False
            weather_des_overcast_clouds = False
            weather_des_scattered_clouds = False
        elif forcast.get('weather_des') == 'light rain':
            weather_des_clear_sky = False
            weather_des_few_clouds = False
            weather_des_light_rain = True
            weather_des_moderate_rain = False
            weather_des_overcast_clouds = False
            weather_des_scattered_clouds = False
        elif forcast.get('weather_des') == 'moderate rain':
            weather_des_clear_sky = False
            weather_des_few_clouds = False
            weather_des_light_rain = False
            weather_des_moderate_rain = True
            weather_des_overcast_clouds = False
            weather_des_scattered_clouds = False
        elif forcast.get('weather_des') == 'overcast clouds':
            weather_des_clear_sky = False
            weather_des_few_clouds = False
            weather_des_light_rain = False
            weather_des_moderate_rain = False
            weather_des_overcast_clouds = True
            weather_des_scattered_clouds = False
        elif forcast.get('weather_des') == 'scattered clouds':
            weather_des_clear_sky = False
            weather_des_few_clouds = False
            weather_des_light_rain = False
            weather_des_moderate_rain = False
            weather_des_overcast_clouds = False
            weather_des_scattered_clouds = True
        else:
            weather_des_clear_sky = False
            weather_des_few_clouds = False
            weather_des_light_rain = False
            weather_des_moderate_rain = False
            weather_des_overcast_clouds = False
            weather_des_scattered_clouds = False
        # Prepare the input dataframe
        inputs = pd.DataFrame()
        feature_names = ['temp', 'feels_like', 'wind_speed', 'cloudiness', 'visibility', 'pop', 'rain_1h', 'month', 'day', 'weekday', 'hour', 
                        'weather_main_Clouds', 'weather_main_Rain', 'weather_des_clear sky', 'weather_des_few clouds', 'weather_des_light rain', 
                        'weather_des_moderate rain', 'weather_des_overcast clouds', 'weather_des_scattered clouds']
        feature_values = [temp, feels_like, wind_speed, cloudiness, visibility, pop, rain_1h, month, day, weekday, hour, weather_main_Clouds, 
                            weather_main_Rain, weather_des_clear_sky, weather_des_few_clouds, weather_des_light_rain, weather_des_moderate_rain, 
                            weather_des_overcast_clouds, weather_des_scattered_clouds]
        for feature_name, value in zip(feature_names, feature_values):
            inputs.loc[0, feature_name] = value
        # Predict with model
        bikes_predicted = int(round(model.predict(inputs)[0],0))
        stands_predicted = bike_stands - bikes_predicted
        if bikes_predicted < 0:
            bikes_predicted = 0
            stands_predicted = bike_stands
        elif bikes_predicted > bike_stands:
            bikes_predicted = bike_stands
            stands_predicted = 0
        response_dict = {'bikes_predicted': bikes_predicted, 'stands_predicted': stands_predicted}
        response_json = json.dumps(response_dict)
    except Exception as e:
        print(f"Error occurred in prediction: {e}")
        pattern_hour = json.loads(dbquery.get_pattern_hour(engine, id))
        for hour_data in pattern_hour:
            if hour_data['hour'] == hour:
                bikes_predicted = int(round(hour_data['avg_available_bikes']))
                stands_predicted = int(round(hour_data['avg_available_stands']))
                break
        response_dict = {'bikes_predicted': bikes_predicted, 'stands_predicted': stands_predicted}
        response_json = json.dumps(response_dict)

    return response_json


if __name__ == "__main__":
    # This will run the Flask application
    app.run(debug=True, host='0.0.0.0', port=80)
