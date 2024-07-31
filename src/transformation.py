import os
import csv
import json
import pandas as pd
from datetime import datetime
import re 


class TransformCSV:
    def __init__(self, config_file='src/config.json'):
        self.config_file = config_file
        self.config = self.read_config()
        self.bronze_dir = self.config.get('bronze_dir')
        self.silver_dir = self.config.get('silver_dir')
        self.root_dir = self.config.get('root_dir')
        self.filename_pattern = re.compile(r'hourly24_production_(\d{4}-\d{2}-\d{2})\.csv')
        self.transformed_files_list = self.config.get('transformed_files_list')
        
    # READING CONFIG FILE
    def read_config(self):
        with open(self.config_file, 'r') as file:
            config_data = json.load(file)
        return config_data
    
    # LIST ALL SCRAPPED FILES 
    def scrapped_files(self):
        scrapped_files_list = [] 
        for file in os.listdir(self.bronze_dir):
            scrapped_files_list.append(file)
        return scrapped_files_list
    

    # LIST ALL ALREADY TRANSFORMED FILES
    def transformed_files(self):
        successful_files = []
        with open(self.transformed_files_list, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                if row[1] == 'Success':
                    successful_files.append(row[0])
        return successful_files

    # GET ONLY SCRAPPED BUT NOT TRANSFORMED
    def untransformed_files(self):
        # GET PREVIOUS RESULTS
        scrapped_files = self.scrapped_files()
        transformed_files = self.transformed_files()
        
        # CONVERT LIST TO SET FOR EASIER HANDLING
        scrapped_files_set = set(scrapped_files)
        transformed_files_set = set(transformed_files)
        
        # FIND THE DIFFERENCE BETWEEN THE TWO SETS
        untransformed_files_set = scrapped_files_set - transformed_files_set        
        untransformed_files = list(untransformed_files_set)        
        return untransformed_files        
    
    # TRANSFORMATION STEPS    
    def transform_data(self, input_file):
        # LOADING FILE
        with open(input_file, 'r') as f:
            data = json.load(f)

        # MAKE IT TO PANDAS DF
        values = data['energy']
        df = pd.DataFrame({'energy': values})

        # EXTRACT DATE FROM FILENAME
        date_part = os.path.basename(input_file).split('.json')[0]
        date_day = date_part.split('_')[-1]
        df['date_day'] = pd.to_datetime(date_day, format='%Y-%m-%d')

        # RESET INDEX AND RENAME COLUMNS
        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'hour'})
        df = df[['date_day', 'hour', 'energy']]

        # CONVERT HOUR TO DATETIME
        df['date'] = df['date_day'] + pd.to_timedelta(df['hour'], unit='h')

        # CREATE REFRESH DATE COLUMN
        df['data_atualizacao'] = datetime.now().date()

        # DROP UNNECESSARY COLUMNS
        df = df.drop(columns=['date_day', 'hour'])
        df = df[['date', 'energy', 'data_atualizacao']]

        # MAKE DATAFRAME TO CSV AND KEEP ORIGINAL FILENAME
        output_file = os.path.join(self.silver_dir, os.path.basename(input_file).replace('.json', '.csv'))
        df.to_csv(output_file, index=False)
        
        return True
    
    def run(self):
        # GET THE LIST OF UNTRANSFORMED FILENAMES
        untransformed_files = self.untransformed_files()
        
        # TRANSFORM EACH FILE
        for file in untransformed_files:
            input_file_path = os.path.join(self.bronze_dir, file)
            success = self.transform_data(input_file_path)
            
            # Read existing data
            with open(self.transformed_files_list, 'r', newline='') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)

            # Append new entry
            rows.append([file, 'Success' if success else 'Failed'])

            # Sort rows by filename (the first column)
            rows_sorted = sorted(rows[1:], key=lambda x: x[0])  # Exclude header for sorting
            rows_sorted.insert(0, rows[0])  # Reinsert header

            # Write sorted data back to CSV
            with open(self.transformed_files_list, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows_sorted)


if __name__ == "__main__":
    trans = TransformCSV()
    trans.run()