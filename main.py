import time
from scripts.database_manager import DatabaseManager
from scripts.data_pipeline import DataPipeline

def main():
    start_time = time.process_time()
    db_manager = DatabaseManager()
    pipeline = DataPipeline('workshop_data.xlsx', db_manager)
    pipeline.process_data()

    end_time = time.process_time()
    # Calculate the execution time
    execution_time = end_time - start_time
    print(f"Process time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()
