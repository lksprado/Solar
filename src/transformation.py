import os
import csv
import json
import pandas as pd
from datetime import datetime   

# TRANSFORMS JSON TO CSV
def transform_data(input_file, output_folder, log_file):
    try:
        # LOADING FILE
        with open(input_file, 'r') as f:
            data = json.load(f)

        # MAKE IT TO PANDAS DF
        values = data['energy']
        df = pd.DataFrame({'energy': values})

        # EXTRACT DATE FROM FILENAME
        date_part = os.path.basename(input_file).split('.json')[0]
        date_day = date_part.split('_')[-1]
        df['date_day'] = pd.to_datetime(date_day)

        # RESET INDEX AND RENAME COLUMNS
        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'hour'})
        df = df[['date_day', 'hour', 'energy']]

        # CONVERT HOUR TO DATETIME
        df['date'] = df['date_day'] + pd.to_timedelta(df['hour'], unit='h')
        
        #CREATE REFRESH DATE COLUMN
        df['data_atualizacao'] = datetime.now().date()

        # DROP UNECESSARY COUMNS
        df = df.drop(columns=['date_day', 'hour'])
        df = df[['date', 'energy','data_atualizacao']]

        # MAKE DATAFRAM TO CSV AND KEEP ORIGINAL FILENAME 
        output_file = os.path.join(output_folder, os.path.basename(input_file).replace('.json', '.csv'))
        df.to_csv(output_file, index=False)

        # WRITE SUCCESS MESSAGE TO LOG FILES
        with open(log_file, 'a') as logfile:
            logfile.write(f"Transformation successful for {input_file}\n")
        return True
    except Exception as e:
        # WRITE ERROR MESSAGE TO LOG FILES
        with open(log_file, 'a') as logfile:
            logfile.write(f"Error transforming file {input_file}: {e}\n")
        return False

def main():
    input_folder = '/media/lucas/Files/2.Projetos/0.mylake/bronze/solar_project'
    output_folder = '/media/lucas/Files/2.Projetos/0.mylake/silver/solar_project'
    log_file = 'transformation_log.txt'
    status_file = os.path.join(output_folder, 'transformation_status.csv')

    # OPENS STATUS CSV FILE
    if not os.path.exists(status_file):
        with open(status_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Filename', 'Status'])
            
    # GET FILENAMES MARKED AS  SUCESSFUL
    successful_files = set()
    with open(status_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            if row[1] == 'Success':
                successful_files.add(row[0])

    # PROCESS EACH FILE
    for filename in os.listdir(input_folder):
        if filename.endswith('.json'):
            input_file = os.path.join(input_folder, filename)
            
            # SKIP TRANSFORMATION IF ALREADY SUCESSFUL
            if filename in successful_files:
                continue
            
            success = transform_data(input_file, output_folder, log_file)
            
            # WRITE FILENAME AND STATUS TO STATUS CSV FILE 
            with open(status_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([filename, 'Success' if success else 'Failed'])
if __name__ == "__main__":
    main()
