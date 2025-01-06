import requests
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import time
import traceback
import schedule
import dbinfo

def log_message(message):
    filepath = "log_database.txt"
    with open(filepath, 'a') as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - {message}\n")

def create_database():
    try:
        # Create database engine
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, dbinfo.PORT))
        # Open a connection
        with engine.connect() as connection:
            # SQL query to create the database
            query = f"CREATE DATABASE IF NOT EXISTS {dbinfo.DBNAME}"
            # Execute the SQL query
            connection.execute(text(query))
        
        # Recreate database engine with database name
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, dbinfo.PORT, 
                                                                       dbinfo.DBNAME))
        with engine.connect() as connection:
            # SQL query to create tables for bike stations
            query = f"""
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME1} 
            (number INTEGER PRIMARY KEY, 
            name VARCHAR(64), 
            address VARCHAR(64), 
            position_lat FLOAT, 
            position_lng FLOAT, 
            banking VARCHAR(64))
            """
            # Execute the SQL query
            connection.execute(text(query))

            # SQL query to create tables for availability
            query = f"""
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME2} 
            (number INTEGER, 
            bike_stands INTEGER, 
            available_bikes INTEGER, 
            available_stands INTEGER, 
            status VARCHAR(64), 
            last_update BIGINT, 
            PRIMARY KEY (number, last_update), 
            FOREIGN KEY (number) REFERENCES {dbinfo.TNAME1}(number))
            """
            # Execute the SQL query
            connection.execute(text(query))

            # SQL query to create tables for current weather
            query = f"""
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME3} 
            (dt INTEGER PRIMARY KEY, 
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
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME4} 
            (dt INTEGER PRIMARY KEY, 
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
            CREATE TABLE IF NOT EXISTS {dbinfo.TNAME5} 
            (dt INTEGER PRIMARY KEY, 
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
    except Exception as e:
        log_message(f"Error creating database {e}")

def connect_database(max_retries=3, retry_delay=5):
    retries = 0
    while retries < max_retries:
        try:
            # Create database engine
            engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, dbinfo.PORT, 
                                                                           dbinfo.DBNAME))
            # Open a connection
            connection = engine.connect()
            return connection
        except Exception as e:
            log_message(f"Error connecting to database: {e}")
            # Increment retry count
            retries += 1
            # Wait before retrying
            time.sleep(retry_delay)
    log_message(f"Error connecting to database: Failed to connect to database after {max_retries} retries")
    return None

def fetch_API(tname, max_retries=3, retry_delay=30):
    retries = 0
    try:
        while retries < max_retries:
            # Choose the appropriate API based on the table name and make the API call
            if tname == dbinfo.TNAME1 or tname == dbinfo.TNAME2:
                r = requests.get(dbinfo.JC_URI, params={"apiKey": dbinfo.JCKEY, "contract": dbinfo.CITYNAME})
            elif tname == dbinfo.TNAME3:
                r = requests.get(dbinfo.W_C_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
            elif tname == dbinfo.TNAME4:
                r = requests.get(dbinfo.W_HF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric"})
            elif tname == dbinfo.TNAME5:
                r = requests.get(dbinfo.W_DF_URI, params={"apiKey": dbinfo.WKEY, "id": dbinfo.CITY_ID, "units": "metric", "cnt": "16"})
            
            # Convert data into json format and return
            if r.status_code == 200:
                data = json.loads(r.text)
                return data
            else:
                # Log error message if status code is not 200
                log_message(f"Error fetching data from API for table {tname}: {r.status_code}")
                # Increment retry count
                retries += 1
                # Wait before retrying
                time.sleep(retry_delay)
    except Exception as e:
        # Log any other exceptions that occur
        log_message(f"Error fetching data from API for table {tname}: {e}")
    return None

def init_stations(connection):
    try:
        json_data = fetch_API(dbinfo.TNAME1)
        if json_data and connection:
            # Check and populate only if the table for bike stations is empty
            row_count = connection.execute(text(f"SELECT COALESCE(COUNT(*), 0) FROM {dbinfo.TNAME1}")).fetchone()[0]
            if not row_count:
                # Counter for tracking the number of inserted rows
                n = 0
                j = 0
                for station in json_data:
                    try:
                        # SQL query to insert into the table for stations
                        query = f"insert into {dbinfo.TNAME1} (number, name, address, position_lat, position_lng, banking) \
                            VALUES (:number, :name, :address, :position_lat, :position_lng, :banking)"
                        # Prepare data
                        name = station['name'].replace("'", "&#39;")
                        address = station['address'].replace("'", "&#39;")
                        if station['banking']:
                            banking = "Yes"
                        else:
                            banking = "No"
                        # Execute the SQL query
                        connection.execute(text(query), {'number': station['number'], 'name': name, 'address': address,
                                    'position_lat': station['position']['lat'], 'position_lng': station['position']['lng'],
                                    'banking': banking})
                        # Increment row counter
                        n += 1
                    except IntegrityError as e:
                        # Handle duplicate entry errors (shouldn't arise due to row_count check at the beginning)
                        j += 1
                # Commit the transaction
                connection.commit()
                log_message(f"Update in table {dbinfo.TNAME1}: {n} rows of static data imported ({j} duplicate {'entries ingored' if j>0 else 'entry'})")
                time.sleep(10)
        else:
            log_message(f"Error updating table {dbinfo.TNAME1}: No valid data or connection")
    except Exception as e:
        log_message(f"Error updating table {dbinfo.TNAME1}: {e}")

def update_availability(connection):
    try:
        json_data = fetch_API(dbinfo.TNAME2)
        if json_data and connection:
            # Counters for tracking the number of inserted rows
            n = 0
            j = 0
            # SQL query to insert into the table for availability
            query = f"""
            INSERT INTO {dbinfo.TNAME2} 
            (number, bike_stands, available_bikes, available_stands, status, last_update) 
            VALUES (:number, :bike_stands, :available_bikes, :available_stands, :status, :last_update)
            """
            for station in json_data:
                try:
                    # Prepare data
                    last_update = int(station['last_update'])/1000
                    # Execute the SQL query
                    connection.execute(text(query), {'number': station['number'], 'bike_stands': station['bike_stands'], 
                                        'available_bikes': station['available_bikes'], 'available_stands': station['available_bike_stands'], 
                                        'status': station['status'], 'last_update': last_update})
                    # Increment row counter
                    n += 1
                except IntegrityError as e:
                    # Handle duplicate entry errors
                    j += 1
            # Commit the transaction
            connection.commit()
            log_message(f"Update in table {dbinfo.TNAME2}: Insert {n} rows; {j} stations haven't updated yet")
        else:
            log_message(f"Error updating table {dbinfo.TNAME2}: No valid data or connection")
    except Exception as e:
        log_message(f"Error updating table {dbinfo.TNAME2}: {e}")
        
def update_current(connection):
    try:
        json_data = fetch_API(dbinfo.TNAME3)
        if json_data and connection:
            # Prepare the data
            dt = json_data['dt']
            weather_id = json_data['weather'][0]['id']
            weather_main = json_data['weather'][0]['main']
            weather_des = json_data['weather'][0]['description']
            weather_icon = json_data['weather'][0]['icon']
            temp = json_data['main']['temp']
            feels_like = json_data['main']['feels_like']
            wind_speed = json_data['wind']['speed']
            cloudiness = json_data['clouds']['all']
            visibility = json_data['visibility']
            if "rain" in json_data and "1h" in json_data['rain']:
                rain_1h = json_data['rain']['1h']
            else:
                rain_1h = 0.0
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
            connection.execute(text(query), {'dt': dt, 'weather_id': weather_id, 'weather_main': weather_main, 'weather_des': weather_des, 
                               'weather_icon': weather_icon, 'temp': temp, 'feels_like': feels_like, 'wind_speed': wind_speed, 
                               'cloudiness': cloudiness, 'visibility': visibility, 'rain_1h': rain_1h})
            # Commit the transaction
            connection.commit()
            log_message(f"Update in table {dbinfo.TNAME3}: 1 row inserted/updated")
        else:
            log_message(f"Error updating table {dbinfo.TNAME3}: No valid data or connection")
    except Exception as e:
        log_message(f"Error updating table {dbinfo.TNAME3}: {e}")

def update_forcast_h(connection):
    try:
        json_data = fetch_API(dbinfo.TNAME4)
        if json_data and connection:
            # Counters for tracking the number of collected and inserted/updated rows
            n = json_data['cnt']
            j = 0
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
            for hourly in json_data['list']:
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
                connection.execute(text(query), {'dt': dt, 'weather_id': weather_id, 'weather_main': weather_main, 
                                                 'weather_des': weather_des, 'weather_icon': weather_icon, 'temp': temp, 
                                                 'feels_like': feels_like, 'wind_speed': wind_speed, 'cloudiness': cloudiness, 
                                                 'visibility': visibility, 'pop': pop, 'rain_1h': rain_1h})
                # Increment row counter
                j += 1
            # Commit the transaction
            connection.commit()
            log_message(f"Update in table {dbinfo.TNAME4}: {n} timestamps returned by API; {j} timestamps inserted/updated")
        else:
            log_message(f"Error updating table {dbinfo.TNAME4}: No valid data or connection")
    except Exception as e:
        log_message(f"Error updating table {dbinfo.TNAME4}: {e}")

def update_forcast_d(connection):
    try:
        json_data = fetch_API(dbinfo.TNAME5)
        if json_data and connection:
            # Counters for tracking the number of collected and inserted/updated rows
            n = json_data['cnt']
            j = 0
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
            for daily in json_data['list']:
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
                connection.execute(text(query), {'dt': dt, 'weather_id': weather_id, 'weather_main': weather_main, 
                                                 'weather_des': weather_des, 'weather_icon': weather_icon, 'temp_day': temp_day, 
                                                 'temp_eve': temp_eve, 'temp_night': temp_night, 'temp_morn': temp_morn, 
                                                 'temp_min': temp_min, 'temp_max': temp_max, 'feels_like_day': feels_like_day, 
                                                 'feels_like_eve': feels_like_eve, 'feels_like_night': feels_like_night, 
                                                 'feels_like_morn': feels_like_morn, 'wind_speed': wind_speed, 'cloudiness': cloudiness, 
                                                 'pop': pop, 'rain': rain})
                # Increment row counter
                j += 1
            # Commit the transaction
            connection.commit()
            log_message(f"Update in table {dbinfo.TNAME5}: {n} timestamps returned by API; {j} timestamps inserted/updated")
        else:
            log_message(f"Error updating table {dbinfo.TNAME5}: No valid data or connection")
    except Exception as e:
        log_message(f"Error updating table {dbinfo.TNAME5}: {e}")

def main():
    # Initialize connection to None
    connection = None
    try:
        # Prepare database (executed only once)
        create_database()
        # Establish database connection
        connection = connect_database()
        # Populate static bike station data (executed only once)
        init_stations(connection)

        # First time call
        update_availability(connection)
        update_current(connection)
        update_forcast_h(connection)
        update_forcast_d(connection)

        # Schedule to update availability every 5 minutes
        schedule.every(5).minutes.do(update_availability, connection=connection)
        # Schedule to update current weather every 30 minutes
        schedule.every(30).minutes.do(update_current, connection=connection)
        # Schedule to update hourly_forecast every hour
        schedule.every().hour.do(update_forcast_h, connection=connection)
        # Schedule to update daily_forecast every 12 hour
        schedule.every(12).hours.do(update_forcast_d, connection=connection)

        while True:
            schedule.run_pending()
            time.sleep(1)
    except:
        log_message(f"Error at {__name__}: {traceback.format_exc()}")
    finally:
        # Close database connection when the program exits
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
