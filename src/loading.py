import os
import psycopg2
import pandas as pd
from extraction import EMAWebScraper

class Loader:
    def __init__(self, config_file, log_file):
        scraper = EMAWebScraper(config_file)
        self.config = scraper.read_config(config_file)
        self.log_file = log_file
        self.conn = psycopg2.connect(self.config['connection_string'])
        self.cursor = self.conn.cursor()

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
        try:
            # Iterate through each CSV file in the input folder
            for filename in os.listdir(input_folder):
                if filename.endswith('.csv'):
                    file_path = os.path.join(input_folder, filename)
                    table_name = 'dw_lcs.tb_solar_stg'  # Table name

                    # Load CSV data into PostgreSQL table
                    with open(file_path, 'r') as f:
                        next(f)  # Skip header row
                        self.cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV", f)

            # Commit 
            self.conn.commit()

            with open(self.log_file, 'a') as logfile:
                logfile.write("All CSV files loaded into PostgreSQL successfully.\n")
        except Exception as e:
            with open(self.log_file, 'a') as logfile:
                logfile.write(f"Error loading CSV files into PostgreSQL: {e}\n")
    

    def trigger_stored_procedure(self):
        try:
            self.cursor.execute("CALL process_staging_data();")
            self.conn.commit()
            with open(self.log_file, 'a') as logfile:
                logfile.write("Stored procedure executed successfully.\n")
        except Exception as e:
            with open(self.log_file, 'a') as logfile:
                logfile.write(f"Error executing stored procedure: {e}\n")

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
    
    def move_files(self, input_folder,done_folder):
            for filename in os.listdir(input_folder):
                if filename.endswith('.csv') and filename != 'transformation_status.csv':
                    file_path = os.path.join(input_folder, filename)
                    os.rename(file_path, os.path.join(done_folder, filename))
            

def main():
    config_file = "/media/lucas/Files/2.Projetos/3.Solar/src/config.json"
    log_file = '/media/lucas/Files/2.Projetos/3.Solar/loading_log.txt'

    loader = Loader(config_file, log_file)
    input_folder = loader.config['input_folder']
    done_folder = loader.config['done_folder']
    success = False

    try:
        loader.create_stored_procedure()
        loader.load_csv_files_to_staging(input_folder)
        loader.trigger_stored_procedure()
        success = True  # Mark as successful if no exceptions occurred
    except Exception as e:
        with open(log_file, 'a') as logfile:
            logfile.write(f"An error occurred: {e}\n")
    finally:
        if success:
            loader.move_files(input_folder, done_folder)
        loader.close_connection()

if __name__ == "__main__":
    main()


