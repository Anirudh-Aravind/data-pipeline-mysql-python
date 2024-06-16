# Data Pipeline using MySQL and Python

This repository contains a Python script that implements a data pipeline for MySQL. The script performs the following tasks:

- Establishes the table structure using MySQL Data Definition Language (DDL).
- Ingests data from an Excel file into the respective tables.
- Utilizes Python's `mysql.connector` module for database interactions.
- Uses threading for parallel processing of data batches.

## Features

- **MySQL DDL:** Creates tables using MySQL's Data Definition Language.
- **Data Ingestion:** Ingests data from an Excel file into MySQL tables.
- **Threading:** Uses threading for concurrent processing of data batches.
- **Error Handling:** Includes error handling and data integrity checks.

## Setup

1. **Prerequisites:**
   - Python 3.x
   - MySQL Server
   - MySQL Connector/Python

2. **Installation:**
   ```bash
   pip install -r requirements.txt


2. **Script Execution:**
   - py main.py

### Note:
Here I am adding two scripts,
#### Parallel Processing Script: main.py
Utilizes Python's threading concept for parallel processing
Modular coding structure for efficient maintenance and scalability
Processes data in parallel using multiple threads

#### Sequnetial Processing Script: data_pipeline_row_iteration.py
Ingests data into SQL tables sequentially using raw Python script
Processes data row by row for efficient data handling
Suitable for small to medium-sized data sets or sequential processing
