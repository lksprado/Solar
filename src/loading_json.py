import os
import json
import psycopg2

# Database connection parameters
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'pg12345'

# Directory containing JSON files
JSON_DIR = '/media/lucas/Files/2.Projetos/0.mylake/bronze/solar_project'

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Create schema if it doesn't exist
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS dw_lcs
""")

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dw_lcs.solar_raw (
        filename VARCHAR(50),
        response JSONB,
        PRIMARY KEY (filename)
    )
""")

# Insert JSON files into the table
for filename in os.listdir(JSON_DIR):
    if filename.endswith('.json'):
        file_path = os.path.join(JSON_DIR, filename)
        with open(file_path, 'r') as file:
            response = json.load(file)
            cursor.execute(
                "SELECT 1 FROM dw_lcs.solar_raw WHERE filename = %s", (filename,)
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO dw_lcs.solar_raw (filename, response) VALUES (%s, %s)",
                    (filename, json.dumps(response))
                )

# Commit changes and close the connection
conn.commit()
cursor.close()
conn.close()
