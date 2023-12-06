import pandas as pd
import json
import psycopg2
import logging

# Set up logging
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_csv(file_path):
    # Read data from CSV
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error occurred during CSV extraction: {str(e)}")
        raise

def extract_json(file_path):
    # Read data from JSON
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error occurred during JSON extraction: {str(e)}")
        raise

def transform(data):
    # Perform complex transformations
    try:
        transformed_data = data.copy()
        # Example transformation: Convert 'date' column to datetime
        transformed_data['date'] = pd.to_datetime(transformed_data['date'])
        # Example transformation: Calculate a new column based on existing ones
        transformed_data['amount_usd'] = transformed_data['amount'] * 1.18
        return transformed_data
    except Exception as e:
        logging.error(f"Error occurred during data transformation: {str(e)}")
        raise

def load_to_db(data, connection):
    # Load transformed data to PostgreSQL database
    try:
        cursor = connection.cursor()
        # Replace 'table_name' with your table name
        for index, row in data.iterrows():
            cursor.execute("INSERT INTO table_name (date, amount, amount_usd) VALUES (%s, %s, %s)",
                           (row['date'], row['amount'], row['amount_usd']))
        connection.commit()
        cursor.close()
        logging.info("Data loaded to the database successfully.")
    except Exception as e:
        logging.error(f"Error occurred during data loading to the database: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Replace with your database credentials
        conn = psycopg2.connect(
            dbname="your_dbname",
            user="your_username",
            password="your_password",
            host="localhost"
        )

        # Extract data from CSV and JSON
        csv_data = extract_csv('input.csv')
        json_data = extract_json('input.json')

        # Combine data or perform additional transformations
        combined_data = pd.concat([csv_data, json_data])
        
        # Transform combined data
        transformed_data = transform(combined_data)

        # Load transformed data into the database
        load_to_db(transformed_data, conn)

        conn.close()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        raise
