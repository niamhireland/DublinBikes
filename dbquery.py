from sqlalchemy import text
import json
import datetime
import time
import dbinfo

def get_all_stations(engine):
    """Fetch all stations from the database, using table stations"""
    with engine.connect() as connection:
        query = f"""
        SELECT * FROM {dbinfo.TNAME1}
        """
        stations_data = connection.execute(text(query)).fetchall()
    if stations_data:
        column_names = ['number', 'name', 'address', 'position_lat', 'position_lng', 'banking']
        stations_data_list = [dict(zip(column_names, row)) for row in stations_data]
        stations_data_json = json.dumps(stations_data_list)
        return stations_data_json
    else:
        # Return an empty json data if fails
        return json.dumps([])

def get_availability(engine, number):
    """Fetch availability and static info for a specific station at the latest timestamp from the database, using table stations and availability"""
    with engine.connect() as connection:
        query = f"""
        SELECT {dbinfo.TNAME1}.number, name, address, position_lat, position_lng, banking, bike_stands, available_bikes, 
        available_stands, status, last_update FROM {dbinfo.TNAME1}, {dbinfo.TNAME2}
        WHERE {dbinfo.TNAME1}.number={dbinfo.TNAME2}.number
        AND {dbinfo.TNAME2}.number={number}
        ORDER BY last_update DESC
        LIMIT 1
        """
        availability_data = connection.execute(text(query)).fetchone()
    if availability_data:
        column_names = ['number', 'name', 'address', 'position_lat', 'position_lng', 'banking', 'bike_stands', 'available_bikes', 
                        'available_stands', 'status', 'last_update']
        availability_data_dict = dict(zip(column_names,availability_data))
        availability_data_json = json.dumps(availability_data_dict)
        return availability_data_json
    else:
        # Return an empty json data if fails
        return json.dumps({})

def get_all_availability(engine):
    """Fetch availability and static info for all stations at the latest timestamp from the database, using table stations and availability"""
    with engine.connect() as connection:
        query = f"""
        SELECT a.*
        FROM {dbinfo.TNAME2} a
        JOIN (
            SELECT number, MAX(last_update) AS latest_update
            FROM {dbinfo.TNAME2}
            GROUP BY number
        ) AS b 
        ON a.number = b.number 
        AND a.last_update = b.latest_update
        ORDER BY a.number
        """
        availability_data = connection.execute(text(query)).fetchall()
    if availability_data:
        column_names = ['number', 'bike_stands', 'available_bikes', 'available_stands', 'status', 'last_update']
        availability_data_list = [dict(zip(column_names, row)) for row in availability_data]
        availability_data_json = json.dumps(availability_data_list)
        return availability_data_json
    else:
        # Return an empty json data if fails
        return json.dumps([])

def get_pattern_day(engine, number):
    """Fetch daily availability pattern (average availablity for each day) for a specific station from the database, using table availability"""
    with engine.connect() as connection:
        query = f"""
        SELECT DAYOFWEEK(FROM_UNIXTIME(last_update)), avg(available_bikes), avg(available_stands) 
        FROM {dbinfo.TNAME2}
        WHERE number={number}
        GROUP BY DAYOFWEEK(FROM_UNIXTIME(last_update))
        ORDER BY DAYOFWEEK(FROM_UNIXTIME(last_update))
        """
        pattern_day_data = connection.execute(text(query)).fetchall()
    if pattern_day_data:
        pattern_day_list = []
        for row in pattern_day_data:
            day = int(row[0])
            avg_available_bikes = float(row[1])
            avg_available_stands = float(row[2])
            pattern_day_list.append({'day': day, 'avg_available_bikes': avg_available_bikes, 'avg_available_stands': avg_available_stands})
        pattern_day_json = json.dumps(pattern_day_list)
        return pattern_day_json
    else:
        # Return an empty json data if fails
        return json.dumps([])

def get_pattern_hour(engine, number):
    """Fetch hourly availability pattern (average availablity for each hour) for a specific station from the database, using table availability"""
    with engine.connect() as connection:
        query = f"""
        SELECT HOUR(FROM_UNIXTIME(last_update)), avg(available_bikes), avg(available_stands) FROM {dbinfo.TNAME2}
        WHERE number={number}
        GROUP BY HOUR(FROM_UNIXTIME(last_update))
        ORDER BY HOUR(FROM_UNIXTIME(last_update))
        """
        pattern_hour_data = connection.execute(text(query)).fetchall()
    if pattern_hour_data:
        pattern_hour_list = []
        for row in pattern_hour_data:
            hour = int(row[0])
            avg_available_bikes = float(row[1])
            avg_available_stands = float(row[2])
            pattern_hour_list.append({'hour': hour, 'avg_available_bikes': avg_available_bikes, 'avg_available_stands': avg_available_stands})
        pattern_hour_json = json.dumps(pattern_hour_list)
        return pattern_hour_json
    else:
        # Return an empty json data if fails
        return json.dumps([])

def get_current_weather(engine):
    """Fetch current weather, using table current"""
    with engine.connect() as connection:
        query = f"""
        SELECT * FROM {dbinfo.TNAME3}
        ORDER BY dt DESC
        LIMIT 1
        """
        current_data = connection.execute(text(query)).fetchone()
    if current_data:
        column_names = ['dt', 'weather_id', 'weather_main', 'weather_des', 'weather_icon', 'temp', 'feels_like', 'wind_speed', 'cloudiness', 
                        'visibility', 'rain_1h']
        current_data_dict = dict(zip(column_names, current_data))
        current_data_json = json.dumps(current_data_dict)
        return current_data_json
    else:
        # Return an empty json data if fails
        return json.dumps({})
    
def get_weather_forcast(engine, month, day, hour):
    """Fetch weather forcast for a given time, using table forcast_h"""
    # Get epoch time
    dt = datetime.datetime(2024, month, day, hour)
    epoch_time = int(time.mktime(dt.timetuple()))
    with engine.connect() as connection:
        query = f"""
        SELECT * FROM {dbinfo.TNAME4}
        WHERE dt={epoch_time}
        """
        forcast_data = connection.execute(text(query)).fetchone()
        if forcast_data:
            column_names = ['dt', 'weather_id', 'weather_main', 'weather_des', 'weather_icon', 'temp', 'feels_like', 'wind_speed', 'cloudiness', 
                        'visibility', 'pop', 'rain_1h']
            forcast_data_dict = dict(zip(column_names, forcast_data))
            forcast_data_json = json.dumps(forcast_data_dict)
            return forcast_data_json
        else:
            # Return an empty json data if fails
            return json.dumps({})

def get_number_stands(engine, number):
    """Fetch the number of total stands of a given station, using table availability"""
    with engine.connect() as connection:
        query = f"""
        SELECT bike_stands FROM {dbinfo.TNAME2}
        WHERE number={number}
        ORDER BY last_update DESC
        LIMIT 1
        """
        data = connection.execute(text(query)).fetchone()
        if data:
            return data[0]
        else:
            return None
