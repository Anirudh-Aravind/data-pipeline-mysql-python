import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

import time

load_dotenv()

# Database connection details
config = {
    'user': os.getenv("USER_NAME"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("HOST"),
    'database': os.getenv("DATABASE")
}

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection = self.get_db_connection()

    def get_db_connection(self):
        try:
            conn = mysql.connector.connect(**self.config)
            return conn
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return None

    def execute_query(self, query, params=None):
        cursor = None
        try:
            if not self.connection.is_connected():
                self.connection = self.get_db_connection()

            cursor = self.connection.cursor()
            cursor.execute(query, params)
            return cursor
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def commit(self):
        if self.connection:
            self.connection.commit()

    def close(self):
        if self.connection:
            self.connection.close()

class DataProcessor:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def upsert_groups(self, group_name, description):
        try:
            group_table = os.getenv("GROUP_TABLE")
            cursor = self.db_manager.execute_query(
                f"SELECT group_id FROM {group_table} WHERE group_name = %s", (group_name,))
            result = cursor.fetchone()

            # Consume any remaining results to avoid "unread result found" error
            cursor.fetchall()
            
            if result:
                group_id = result[0]
                self.db_manager.execute_query(
                    f"UPDATE {group_table} SET description = %s, updation_date = CURDATE() WHERE group_id = %s", (description, group_id))
            else:
                cursor = self.db_manager.execute_query(
                    f"INSERT INTO {group_table} (group_name, description, creation_date, updation_date) VALUES (%s, %s, CURDATE(), CURDATE())", (group_name, description))
                group_id = cursor.lastrowid

            return group_id
        except Exception as e:
            print(f"Error occured while performing upsert operation into the groups table - {e}")

    def upsert_locations(self, location_name, address, city, country, group_id):
        try:
            location_table = os.getenv("LOCATION_TABLE")
            cursor = self.db_manager.execute_query(
                f"SELECT location_id FROM {location_table} WHERE location_name = %s AND group_id = %s", (location_name, group_id))
            result = cursor.fetchone()

            # Consume any remaining results to avoid "unread result found" error
            cursor.fetchall()
            
            if result:
                location_id = result[0]
                self.db_manager.execute_query(
                    f"UPDATE {location_table} SET address = %s, city = %s, country = %s WHERE location_id = %s", (address, city, country, location_id))
            else:
                cursor = self.db_manager.execute_query(
                    f"INSERT INTO {location_table} (location_name, address, city, country, group_id) VALUES (%s, %s, %s, %s, %s)", (location_name, address, city, country, group_id))
                location_id = cursor.lastrowid
            return location_id     
        except Exception as e:
            print(f"Error occured while performing upsert operation into the location table - {e}")

    def upsert_users(self, user_name, email, phone_number, location_id):
        try:
            user_table = os.getenv("USER_TABLE")
            cursor = self.db_manager.execute_query(
                f"SELECT user_id FROM {user_table} WHERE email = %s", (email,))
            result = cursor.fetchone()

            # Consume any remaining results to avoid "unread result found" error
            cursor.fetchall()
            
            if result:
                user_id = result[0]
                self.db_manager.execute_query(
                    f"UPDATE {user_table} SET user_name = %s, phone_number = %s, location_id = %s WHERE user_id = %s", (user_name, phone_number, location_id, user_id))
            else:
                cursor = self.db_manager.execute_query(
                    f"INSERT INTO {user_table} (user_name, email, phone_number, location_id) VALUES (%s, %s, %s, %s)", (user_name, email, phone_number, location_id))
                user_id = cursor.lastrowid
            return user_id
        except Exception as e:
            print(f"Error occured while performing upsert operation into the user table - {e}")

    def process_row(self, row):
        try:
            group_id = self.upsert_groups(row['groupname'], row['group_description'])
            if group_id:
                location_id = self.upsert_locations(row['locationname'], row['location_address'], row['city'], row['country'], group_id)
                if location_id:
                    self.upsert_users(row['user_name'], row['email'], row['phone_number'], location_id)

        except Exception as e:
            print(f"Error occured while performing process operation - {e}")

class DataPipeline:
    def __init__(self, file_path, db_manager):
        self.file_path = file_path
        self.db_manager = db_manager

    def read_data(self):
        return pd.read_excel(self.file_path)

    def process_data(self):
        try:
            df = self.read_data()
            processor = DataProcessor(self.db_manager)

            for index, row in df.iterrows():
                # if index in (5,6,7,8,9):
                    processor.process_row(row)
            
            # Commit all changes after all threads have completed their work
            self.db_manager.commit()
        except Exception as e:
            print(f"Error occured while processing the data - {e}")

def main():
    start_time = time.process_time()
    db_manager = DatabaseManager(config)
    pipeline = DataPipeline('workshop_data.xlsx', db_manager)
    pipeline.process_data()
    db_manager.close()

    end_time = time.process_time()
    # Calculate the execution time
    execution_time = end_time - start_time
    print(f"Process time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()
