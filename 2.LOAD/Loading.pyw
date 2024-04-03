import os
import psycopg2
import pandas as pd

def load_csv_files_to_postgres(input_folder, connection_string, log_file):
    try:
        # Establish connection to PostgreSQL database
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        # Iterate through each CSV file in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith('.csv') and filename != 'transformation_status.csv':
                file_path = os.path.join(input_folder, filename)
                table_name = 'solar'  # Table name

                # Load CSV data into PostgreSQL table
                with open(file_path, 'r') as f:
                    next(f)  # Skip header row
                    cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV", f)

                with open(log_file, 'a') as logfile:
                    logfile.write(f"Loaded {filename} into table {table_name}\n")
                
                # Move CSV file to done folder
                done_folder = 'D:\\2.Projetos\\0.mylake\\silver\\solar_project\\sql_table_done'
                os.rename(file_path, os.path.join(done_folder, filename))
                
        # Commit and close connection
        conn.commit()
        cursor.close()
        conn.close()

        with open(log_file, 'a') as logfile:
            logfile.write("All CSV files loaded into PostgreSQL successfully.\n")
    except Exception as e:
        with open(log_file, 'a') as logfile:
            logfile.write(f"Error loading CSV files into PostgreSQL: {e}\n")

def main():
    input_folder = 'D:\\2.Projetos\\0.mylake\\silver\\solar_project'
    connection_string = "postgresql://postgres:12345@localhost:5432/postgres"
    log_file = 'loading_log.txt'

    load_csv_files_to_postgres(input_folder, connection_string, log_file)

if __name__ == "__main__":
    main()
