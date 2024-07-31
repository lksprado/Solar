import os
import psycopg2
import pandas as pd
import json

class Loader:
    def __init__(self, config_file='src/config.json'):
        self.config_file = config_file
        self.config = self.read_config()
        self.silver_dir = self.config.get('silver_dir')
        self.done_folder = self.config.get('done_folder')
        self.conn = psycopg2.connect(self.config['connection_string'])
        self.cursor = self.conn.cursor()

    # READING CONFIG FILE
    def read_config(self):
        with open(self.config_file, 'r') as file:
            config_data = json.load(file)
        return config_data
    
    def create_stored_procedure(self):
        create_proc_query = """
        CREATE OR REPLACE FUNCTION process_staging_data() RETURNS VOID AS $$
        BEGIN
            -- Insert data from staging to final table
            INSERT INTO dw_lcs.tb_solar (date, energy, data_atualizacao)
            SELECT date, energy, data_atualizacao  -- Replace with actual column names
            FROM dw_lcs.tb_solar_stg;

            -- Truncate staging table
            TRUNCATE TABLE dw_lcs.tb_solar_stg;
        END;
        $$ LANGUAGE plpgsql;
        """
        self.cursor.execute(create_proc_query)
        self.conn.commit()

    def load_csv_files_to_staging(self, input_folder):
        # Iterate through each CSV file in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith('.csv') and filename != 'transformation_status.csv':
                file_path = os.path.join(input_folder, filename)
                table_name = 'dw_lcs.tb_solar_stg'  # Table name

                # Load CSV data into PostgreSQL table
                with open(file_path, 'r') as f:
                    next(f)  # Skip header row
                    self.cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV", f)

            # Commit 
            self.conn.commit()

    def trigger_stored_procedure(self):
        self.cursor.execute("CALL process_staging_data();")
        self.conn.commit()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
    
    def move_files(self):
        for filename in os.listdir(self.silver_dir):
            if filename.endswith('.csv') and filename != 'transformation_status.csv':
                file_path = os.path.join(self.silver_dir, filename)
                os.rename(file_path, os.path.join(self.done_folder, filename))

    def run(self):
        success = False

        self.create_stored_procedure()
        self.load_csv_files_to_staging()
        self.trigger_stored_procedure()
        success = True  # Mark as successful if no exceptions occurred
        if success:
            self.move_files()
        self.close_connection()


