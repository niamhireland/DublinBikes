import requests
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import time
import traceback
import dbinfo


def log_message(message):
    with open('log_db_stations.txt', 'a') as f:
        f.write(f"[{datetime.now()}] - {message}\n")

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
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                       dbinfo.PORT, dbinfo.DBNAME))
        with engine.connect() as connection:
            # SQL query to create tables for bike stations
            query = f"CREATE TABLE IF NOT EXISTS {dbinfo.TNAME1} (number INTEGER PRIMARY KEY, name VARCHAR(64), address VARCHAR(64), \
                position_lat FLOAT, position_lng FLOAT, banking VARCHAR(64))"
            # Execute the SQL query
            connection.execute(text(query))
            # SQL query to create tables for availability
            query = f"CREATE TABLE IF NOT EXISTS {dbinfo.TNAME2} (number INTEGER, bike_stands INTEGER, available_bikes INTEGER, \
                available_stands INTEGER, status VARCHAR(64), last_update BIGINT, \
                PRIMARY KEY (number, last_update), \
                FOREIGN KEY (number) REFERENCES {dbinfo.TNAME1}(number))"
            # Execute the SQL query
            connection.execute(text(query)) 
    except Exception as e:
        log_message(f"Error at database creation: {e}")

def pop_static(json_data):
    try:
        # Create database engine
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                       dbinfo.PORT, dbinfo.DBNAME))
        # Open a connection
        with engine.connect() as connection:
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
                        name = station['name'].replace("'", "")
                        address = station['address'].replace("'", "")
                        # name = station['name'].replace("'", "\'")
                        # address = station['address'].replace("'", "\'")
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
                log_message(f"Static data imported: {n} rows ({j} duplicate {'entries' if j>1 else 'entry'} ingored)")
                # After successful population, wait 30s to avoid frequent requests
                time.sleep(30)
    except Exception as e:
        log_message(f"Error at static data insertion: {e}")

def insert_avail(json_data):
    try:
        # Create database engine
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(dbinfo.USER, dbinfo.DB_PASSWORD, dbinfo.DB_URI, 
                                                                       dbinfo.PORT, dbinfo.DBNAME))
        # Open a connection
        with engine.connect() as connection:
            # Counter for tracking the number of inserted rows
            n = 0
            j = 0
            for station in json_data:
                try:
                    # SQL query to insert into the table for availability
                    query = f"INSERT INTO {dbinfo.TNAME2} (number, bike_stands, available_bikes, available_stands, status, \
                        last_update) VALUES (:number, :bike_stands, :available_bikes, :available_stands, :status, :last_update)"
                    # Prepare data
                    last_update = int(station['last_update'])/1000
                    # Execute the SQL query
                    connection.execute(text(query), {'number': station['number'], 'bike_stands': station['bike_stands'], 
                                        'available_bikes': station['available_bikes'], 'available_stands': station['available_bike_stands'], 
                                        'status':station['status'], 'last_update': last_update})
                    # Increment row counter
                    n += 1
                except IntegrityError as e:
                    # Handle duplicate entry errors
                    j += 1
            # Commit the transaction
            connection.commit()
            log_message(f"Insert {n} rows; {j} stations haven't updated yet")
    except Exception as e:
        log_message(f"Error at real-time data insertion: {e}")

def fetch_and_insert(tname):
    try:
        # Fetch data from the API
        r = requests.get(dbinfo.JC_URI, params={"apiKey": dbinfo.JCKEY, "contract": dbinfo.CITYNAME})
        if r.status_code == 200:
            data = json.loads(r.text)
            # Insert the fetched data into the given table
            if tname == dbinfo.TNAME1:
                pop_static(data)
            elif tname == dbinfo.TNAME2:
                insert_avail(data)
    except Exception as e:
        log_message(f"Error at API request: {e}")

def main():
    # Prepare database and static data (should be executed only once)
    try:
        create_database()
        fetch_and_insert(dbinfo.TNAME1)
    except:
        # Handle any unexpected exceptions
        log_message(traceback.format_exc())

    # Fetch and insert real-time data every 5 min
    while True:
        try:
            fetch_and_insert(dbinfo.TNAME2)
            time.sleep(5*60)
        except:
            # Handle any unexpected exceptions
            log_message(traceback.format_exc())


if __name__ == "__main__":
    main()
