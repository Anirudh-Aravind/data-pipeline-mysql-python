import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from scripts.data_processor import DataProcessor
import os
from dotenv import load_dotenv

load_dotenv()

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

            with ThreadPoolExecutor(max_workers=4) as executor:
                # Split data into chunks for processing
                chunk_size = 10  # Adjust based on data size and database constraints
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    executor.submit(processor.process_data_batch, chunk.to_dict('records'))

        except Exception as e:
            print(f"Error occurred while processing the data - {e}")
