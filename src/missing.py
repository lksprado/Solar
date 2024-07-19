# This script will generate a csv file marking all missing dates including the current date

import os
import re
import csv
from datetime import datetime, timedelta

class MissingDatesFinder:
    def __init__(self, folder_path, filename_pattern):
        self.folder_path = folder_path
        self.filename_pattern = re.compile(filename_pattern)
        self.dates_found = set()

    def extract_dates_from_filenames(self):
        for root, dirs, files in os.walk(self.folder_path):
            for file in files:
                match = self.filename_pattern.match(file)
                if match:
                    date_str = match.group(1)
                    date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    self.dates_found.add(date)

    def find_missing_dates(self):
        if self.dates_found:
            start_date = min(self.dates_found)
        else:
            start_date = datetime.strptime('2021-01-01', '%Y-%m-%d').date()

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

if __name__ == "__main__":
    folder_path = "/media/lucas/Files/2.Projetos/0.mylake/bronze/solar_project"
    filename_pattern = r'hourly24_production_(\d{4}-\d{2}-\d{2})\.json'

    finder = MissingDatesFinder(folder_path, filename_pattern)
    finder.extract_dates_from_filenames()
    finder.generate_missing_dates_csv()

