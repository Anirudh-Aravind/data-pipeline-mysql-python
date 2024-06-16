import os
from dotenv import load_dotenv
import threading

load_dotenv()

class DataProcessor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.lock = threading.Lock()

    def upsert_groups(self, conn, group_name, description):
        try:
            group_table = os.getenv("GROUP_TABLE")
            cursor = conn.cursor()

            with self.lock:
                cursor.execute(
                    f"SELECT group_id FROM {group_table} WHERE group_name = %s", (group_name,))
                result = cursor.fetchone()

                # Consume any remaining results to avoid "unread result found" error
                cursor.fetchall()

                if result:
                    group_id = result[0]
                    cursor.execute(
                        f"UPDATE {group_table} SET description = %s, updation_date = CURDATE() WHERE group_id = %s", (description, group_id))
                else:
                    cursor.execute(
                        f"INSERT INTO {group_table} (group_name, description, creation_date, updation_date) VALUES (%s, %s, CURDATE(), CURDATE())", (group_name, description))
                    group_id = cursor.lastrowid

                conn.commit()  # Commit changes

            cursor.close()  # Close cursor after use

            return group_id

        except Exception as e:
            print(f"Error occurred while performing upsert operation into the groups table - {e}")
            conn.rollback()  # Rollback in case of error
            return None

    def upsert_locations(self, conn, location_name, address, city, country, group_id):
        try:
            location_table = os.getenv("LOCATION_TABLE")
            cursor = conn.cursor()

            with self.lock:
                cursor.execute(
                    f"SELECT location_id FROM {location_table} WHERE location_name = %s AND group_id = %s", (location_name, group_id))
                result = cursor.fetchone()

                # Consume any remaining results to avoid "unread result found" error
                cursor.fetchall()

                if result:
                    location_id = result[0]
                    cursor.execute(
                        f"UPDATE {location_table} SET address = %s, city = %s, country = %s WHERE location_id = %s", (address, city, country, location_id))
                else:
                    cursor.execute(
                        f"INSERT INTO {location_table} (location_name, address, city, country, group_id) VALUES (%s, %s, %s, %s, %s)", (location_name, address, city, country, group_id))
                    location_id = cursor.lastrowid

                conn.commit()

            cursor.close()
            return location_id     
        except Exception as e:
            print(f"Error occurred while performing upsert operation into the location table - {e}")

    def upsert_users(self, conn, user_name, email, phone_number, location_id):
        try:
            user_table = os.getenv("USER_TABLE")
            cursor = conn.cursor()

            with self.lock:
                cursor.execute(
                    f"SELECT user_id FROM {user_table} WHERE email = %s", (email,))
                result = cursor.fetchone()

                # Consume any remaining results to avoid "unread result found" error
                cursor.fetchall()

                if result:
                    user_id = result[0]
                    cursor.execute(
                        f"UPDATE {user_table} SET user_name = %s, phone_number = %s, location_id = %s WHERE user_id = %s", (user_name, phone_number, location_id, user_id))
                else:
                    cursor.execute(
                        f"INSERT INTO {user_table} (user_name, email, phone_number, location_id) VALUES (%s, %s, %s, %s)", (user_name, email, phone_number, location_id))
                    user_id = cursor.lastrowid

                conn.commit()

            cursor.close()
            return user_id
        except Exception as e:
            print(f"Error occurred while performing upsert operation into the user table - {e}")

    def process_data_batch(self, data):
        try:
            db_conn = self.db_manager.get_connection()
            if db_conn:
                with db_conn.cursor() as cursor:
                    for row in data:
                        group_id = self.upsert_groups(db_conn, row['groupname'], row['group_description'])
                        if group_id:
                            location_id = self.upsert_locations(db_conn, row['locationname'], row['location_address'], row['city'], row['country'], group_id)
                            if location_id:
                                self.upsert_users(db_conn, row['user_name'], row['email'], row['phone_number'], location_id)
                db_conn.commit()
        except Exception as e:
            print(f"Error occurred while processing data batch - {e}")
        finally:
            if db_conn:
                db_conn.close()

