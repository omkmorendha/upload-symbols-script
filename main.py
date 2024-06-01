import json
import psycopg2
import os
from dotenv import load_dotenv
import uuid

load_dotenv()

DB_NAME = os.environ.get("DB_NAME")
DB_NAME_MAIN = os.environ.get("DB_NAME_MAIN")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

try:
    # Load the JSON data
    with open('etfs_us_uk.json') as f:
        data = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Error loading JSON file: {e}")
    exit(1)

try:
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME_MAIN,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
except psycopg2.Error as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

# Define the insert query
insert_query = """
    INSERT INTO trades_security (id, xstation_symbol, name, leverage, volume_multiplier, is_virtual_trading, is_autotrading, pips, category)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

try:
    # Iterate over the data and insert each record
    for item in data:
        values = (
            str(uuid.uuid4()),  # Generate a UUID
            item['symbol'],
            item['description'][:50],
            1,  # leverage
            1,  # volume_multiplier
            True,  # is_virtual_trading
            False,  # is_autotrading
            item['tickSize'],
            item['categoryName']
        )
        
        cur.execute(insert_query, values)
    # Commit the transaction
    conn.commit()
except psycopg2.Error as e:
    print(f"Error inserting data into the database: {e}")
    conn.rollback()  # Rollback in case of error

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()
