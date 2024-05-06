import json
import sqlite3
import time
from datetime import datetime
from api_helper import get_time_zone_list, get_time_zone_details

db_connection = ''
cursor = ''
db_name = 'timezones'

def connect_to_database():
    '''
    Connect to the database (creates the database if it does not exist), and initialize the cursor to be used to
    execute SQL queries.
    '''
    global db_connection
    global db_name
    global cursor

    try:
        # Connects to SQLite database (or create database if it does not exist)
        db_connection = sqlite3.connect(db_name + '.db')

        # Creates a cursor object to execute SQL queries
        cursor = db_connection.cursor()
    except Exception:
        print("Error connecting to database.")


def setup_database():
    global db_name
    global cursor

    try:
        # Query to delete the time zone list table if it exists (i.e. if the database has been created before)
        query_delete_list_table = '''
        DROP TABLE IF EXISTS TZDB_TIMEZONES
        '''

        # Query to create the time zone list table
        query_create_list_table = '''
        CREATE TABLE IF NOT EXISTS TZDB_TIMEZONES (        
            COUNTRYCODE VARCHAR(2) NOT NULL,
            COUNTRYNAME VARCHAR(100) NOT NULL,
            ZONENAME VARCHAR(100) PRIMARY KEY NOT NULL,
            GMTOFFSET INTEGER,
            IMPORT_DATE TEXT
        )
        '''

        # Query to create the time zone details table
        # Note: ZONEEND is not set as one of the primary keys because the API often returns NULL for the value
        query_create_zone_details_table = '''
        CREATE TABLE IF NOT EXISTS TZDB_ZONE_DETAILS (        
            COUNTRYCODE VARCHAR(2) NOT NULL,
            COUNTRYNAME VARCHAR(100) NOT NULL,
            ZONENAME VARCHAR(100) NOT NULL,
            GMTOFFSET INTEGER NOT NULL,
            DST INTEGER NOT NULL,
            ZONESTART INTEGER NOT NULL,
            ZONEEND INTEGER NOT NULL,
            IMPORT_DATE TEXT,
            PRIMARY KEY (ZONENAME, ZONESTART)
        )
        '''

        # Query to create the table to log errors
        query_create_error_log_table = '''
        CREATE TABLE IF NOT EXISTS TZDB_ERROR_LOG (        
            ERROR_DATE TEXT NOT NULL,
            ERROR_MESSAGE VARCHAR(1000) NOT NULL
        )
        '''

        # Execute and commit the changes to the db
        cursor.execute(query_delete_list_table)
        cursor.execute(query_create_list_table)
        cursor.execute(query_create_zone_details_table)
        cursor.execute(query_create_error_log_table)

        db_connection.commit()
    except Exception:
        # Close the cursor and the database connection
        cursor.close()
        db_connection.close()


def populate_time_zone_list_table():
    '''
    Retrieves the list of time zones and populates its corresponding database table.
    '''
    try:
        response = ''

        # Retrieve zone list from the API. Retries are in case of API error
        max_retries = 100
        for attempt in range(1, max_retries + 1):
            try:
                response = get_time_zone_list()

                # Check the response status code
                if response.status_code == 200:
                    # Successful response
                    break
                else:
                    # Retry if status code indicates an error (e.g., 400)
                    log_api_error(response.reason)
                    time.sleep(1)  # Add a delay before retrying

            except Exception as error:
                if attempt == max_retries:
                    raise error

        time_zone_list = json.loads(response.content)['zones']

        # Populate the database with each time zone
        for time_zone in time_zone_list:
            time_zone_row = {
                'COUNTRYCODE': time_zone['countryCode'],
                'COUNTRYNAME': time_zone['countryName'],
                'ZONENAME': time_zone['zoneName'],
                'GMTOFFSET': time_zone['gmtOffset'],
                # The API returns a UNIX timestamp, convert it to the desired string format
                'IMPORT_DATE': datetime.fromtimestamp(time_zone['timestamp']).strftime('%m/%d/%Y %I:%M:%S %p'),
            }

            insert_into_table('TZDB_TIMEZONES', time_zone_row)
    except Exception as e:
        # Close the cursor and the database connection
        cursor.close()
        db_connection.close()


def populate_time_zone_details_table():
    '''
    Retrieves details about each time zone and populates the corresponding database table. This is achieved by loading
    the data into a stage table, then adding to the details table only rows from the staging table that are not already
    in the details table.
    '''
    try:
        # Create staging table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS STAGING (
                COUNTRYCODE VARCHAR(2) NOT NULL,
                COUNTRYNAME VARCHAR(100) NOT NULL,
                ZONENAME VARCHAR(100) NOT NULL,
                GMTOFFSET INTEGER NOT NULL,
                DST INTEGER NOT NULL,
                ZONESTART INTEGER NOT NULL,
                ZONEEND INTEGER NOT NULL,
                IMPORT_DATE TEXT,
                PRIMARY KEY (ZONENAME, ZONESTART)
            )
        ''')

        # Fetch time zone list from the time zone list table
        cursor.execute(f"SELECT ZONENAME FROM TZDB_TIMEZONES")
        rows = cursor.fetchall()
        zones = [row[0] for row in rows]


        # Upload elements to the stage table
        for zone in zones:
            details = ''

            # Retries to account for any API errors
            max_retries = 100
            for attempt in range(1, max_retries + 1):
                try:
                    response = get_time_zone_details(zone)

                    # Check the response status code
                    if response.status_code == 200:
                        # Successful response
                        details = json.loads(response.content)
                        break
                    else:
                        # Retry if status code indicates an error (e.g., 400)
                        log_api_error(response.reason)
                        time.sleep(1)  # Add a delay before retrying

                except Exception as error:
                    if attempt == max_retries:
                        raise error


            # Add row to stage table
            zone_details_row = {
                'COUNTRYCODE': details['countryCode'],
                'COUNTRYNAME': details['countryName'],
                'ZONENAME': details['zoneName'],
                'GMTOFFSET': details['gmtOffset'],
                'DST': details['dst'],
                'ZONESTART': details['zoneStart'],
                'ZONEEND': details['zoneEnd'],
                'IMPORT_DATE': datetime.fromtimestamp(details['timestamp']).strftime('%m/%d/%Y %I:%M:%S %p'),
            }
            insert_into_table('STAGING', zone_details_row)

        # Add to the details table all the elements from the stage table that are not already in the details table
        cursor.execute('''
            INSERT INTO TZDB_ZONE_DETAILS
            SELECT *
            FROM STAGING
            WHERE NOT EXISTS (
                SELECT 1
                FROM TZDB_ZONE_DETAILS
                WHERE TZDB_ZONE_DETAILS.ZONENAME = STAGING.ZONENAME
            )
        ''')

        # Delete stage table
        cursor.execute('DROP TABLE IF EXISTS STAGING')

        # Commit the transaction to make the changes permanent
        db_connection.commit()
    except Exception as e:
        print(e)
        # Close the cursor and the database connection
        cursor.close()
        db_connection.close()


def log_api_error(error_message:str):
    '''
    Log API error into the error log table
    :param error_message: the error message
    '''

    error = {
        'ERROR_DATE': datetime.now().strftime('%m/%d/%Y %I:%M:%S %p'),
        'ERROR_MESSAGE': error_message
    }

    insert_into_table('TZDB_ERROR_LOG', error)

def insert_into_table(table_name, data):
    # Validate date format. First determine the type of date (import or error) depending on what table
    # the data is being inserted into.
    date_column = 'IMPORT_DATE'
    if "ERROR_DATE" in data:
        date_column = 'ERROR_DATE'

    if not validate_date(data[date_column]):
        raise TypeError("Invalid, string does not match the date format")

    # Construct the SQL query dynamically based on table name and data dictionary
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?' for _ in data])

    # Format values based on their data type
    formatted_values = [f'{value}' if isinstance(value, str) else str(value) for value in data.values()]
    query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
    cursor.execute(query, formatted_values)

    # Commit the changes to make them permanent
    db_connection.commit()


def validate_date(date_string):
    '''
    Helper function to validate the format of a date string. Note: SQLite does not support a DATE, so the tables
    use a TEXT type and the validator determines if the string represents a formatted date
    :param date_string: date string to check
    :return: True if the string is the correct date format, False otherwise
    '''
    try:
        datetime.strptime(date_string, '%m/%d/%Y %I:%M:%S %p')
        return True
    except ValueError:
        return False

def table_exists(table_name:str):
    '''
    Checks if a table exists in the database. Note: this function is used for testing purposes.
    :param table_name: table name
    :return: True if table exists, False otherwise
    '''
    global cursor

    cursor.execute('PRAGMA table_info({})'.format(table_name))
    exists = cursor.fetchall()
    return bool(exists)


def create_and_populate_database():
    try:
        connect_to_database()
        setup_database()
        populate_time_zone_list_table()
        populate_time_zone_details_table()
    finally:
        cursor.close()
        db_connection.close()

create_and_populate_database()