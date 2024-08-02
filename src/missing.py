# This script will generate a csv file marking all missing dates including the current date

import os
import re
import csv
from datetime import datetime, timedelta
import json

class MissingDatesFinder:
    def __init__(self, config_file: str = 'src/config.json'):
        self.config_file = config_file
        self.config = self.read_config()
        self.bronze_dir = self.config.get('bronze_dir')
        self.filename_pattern = re.compile(r'hourly24_production_(\d{4}-\d{2}-\d{2})\.json')
        self.dates_found = set()

        # READING CONFIG FILE
    def read_config(self):
        with open(self.config_file, 'r') as file:
            config_data = json.load(file)
        return config_data

    def extract_dates_from_filenames(self):
        for file in os.listdir(self.bronze_dir):
            match = self.filename_pattern.match(file)
            if match:
                date_str = match.group(1)
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
                self.dates_found.add(date)

    def find_missing_dates(self):
        if self.dates_found:
            start_date = min(self.dates_found)
        else:
            start_date = datetime.strptime('2021-09-16', '%Y-%m-%d').date()

        current_date = datetime.now().date()
        all_dates = set(start_date + timedelta(days=x) for x in range((current_date - start_date).days + 1))
        missing_dates = sorted(all_dates - self.dates_found)
        return missing_dates

    def generate_missing_dates_csv(self, csv_filename="missing_days.csv"):
        missing_dates = self.find_missing_dates()
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Missing Dates"])
            for missing_date in missing_dates:
                writer.writerow([missing_date])
    
    def run(self):
        self.extract_dates_from_filenames()
        self.generate_missing_dates_csv()

if __name__ == "__main__":
    finder = MissingDatesFinder()
    finder.run()
