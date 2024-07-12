import os
import csv
import json
import pandas as pd
from datetime import datetime   

def transform_data(input_file, output_folder, log_file):
    try:
        # Load JSON data
        with open(input_file, 'r') as f:
            data = json.load(f)

        # Perform transformations
        values = data['energy']
        df = pd.DataFrame({'energy': values})

        # Extract date from filename
        date_part = os.path.basename(input_file).split('.json')[0]
        date_day = date_part.split('_')[-1]
        df['date_day'] = pd.to_datetime(date_day)

        # Reset index and rename columns
        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'hour'})
        df = df[['date_day', 'hour', 'energy']]

        # Convert hour to datetime
        df['date'] = df['date_day'] + pd.to_timedelta(df['hour'], unit='h')
        
        #Create refresh date column
        df['data_atualizacao'] = datetime.now().date()

        # Drop unnecessary columns
        df = df.drop(columns=['date_day', 'hour'])
        df = df[['date', 'energy','data_atualizacao']]

        # Write DataFrame to CSV file with the original filename
        output_file = os.path.join(output_folder, os.path.basename(input_file).replace('.json', '.csv'))
        df.to_csv(output_file, index=False)

        # Write success message to log file
        with open(log_file, 'a') as logfile:
            logfile.write(f"Transformation successful for {input_file}\n")

        # Return success
        return True
    except Exception as e:
        # Write error message to log file
        with open(log_file, 'a') as logfile:
            logfile.write(f"Error transforming file {input_file}: {e}\n")
        return False

def main():
    input_folder = 'D:\\2.Projetos\\0.mylake\\bronze\\solar_project'
    output_folder = 'D:\\2.Projetos\\0.mylake\\silver\\solar_project'
    log_file = 'transformation_log.txt'
    status_file = os.path.join(output_folder, 'transformation_status.csv')

    # Initialize status CSV file
    if not os.path.exists(status_file):
        with open(status_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Filename', 'Status'])
            
    # Read status CSV file to get filenames marked as successful
    successful_files = set()
    with open(status_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            if row[1] == 'Success':
                successful_files.add(row[0])

    # Process each file in input folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.json'):
            input_file = os.path.join(input_folder, filename)
            
            # Skip transformation if filename is marked as successful
            if filename in successful_files:
                # Write skipping message to log file
                with open(log_file, 'a') as logfile:
                    logfile.write(f"Skipping transformation for {filename} (already marked as successful)\n")
                continue
            
            success = transform_data(input_file, output_folder, log_file)
            
            # Write filename and status to status CSV file
            with open(status_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([filename, 'Success' if success else 'Failed'])
if __name__ == "__main__":
    main()
