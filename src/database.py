import mysql.connector
import random
import time
import datetime

# methods to interact with the Database

# This method establishes the connection with the MySQL db
def create_server_connection(host_name, user_name, user_password):
    # Implement the logic to create the server connection
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

# This method creates the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    # For database creatio nuse this method
    # If you have created your databse using UI, no need to implement anything
    cursor = connection.cursor()
    try:
        drop_query = "DROP DATABASE IF EXISTS " + db_name
        db_query = "CREATE DATABASE " + db_name
        switch_db_query = "USE " + switch_db
        cursor.execute(drop_query)
        cursor.execute(db_query)
        cursor.execute(switch_db_query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

# This method establishes the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    cursor = connection.cursor()
    try:
        cursor.execute(table_creation_statement)
        connection.commit()
        print("Create table successful")
    except Error as err:
        print(f"Error: '{err}'")

# Use this function to drop tables in a database
def drop_table(connection, table_drop_statement):
    cursor = connection.cursor()
    try:
        cursor.execute(table_drop_statement)
        connection.commit()
        print("Drop table successful")
    except Error as err:
        print(f"Error: '{err}'")

# Perform all single insert statements in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Insert successful")
    except Error as err:
        print(f"Error: '{err}'")

# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table 
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Bulk insert successful")
    except Error as err:
        print(f"Error: '{err}'")

