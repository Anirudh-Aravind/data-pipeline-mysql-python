from mysql.connector import pooling
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection details
config = {
    'user': os.getenv("USER_NAME"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("HOST"),
    'database': os.getenv("DATABASE"),
    'pool_name': "mypool",
    'pool_size': 5  # Adjust as needed
}
print(config)

class DatabaseManager:
    def __init__(self):
        self.config = config
        self.pool = pooling.MySQLConnectionPool(**self.config)

    def get_connection(self):
        return self.pool.get_connection()