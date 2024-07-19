import os
import re

# Directory containing the files
directory = '/media/lucas/Files/2.Projetos/0.mylake/bronze/solar_project'

# Pattern to match the files that need renaming
pattern = re.compile(r'hourly24_production_(\d{8})\.json')

# Iterate over files in the directory
for filename in os.listdir(directory):
    match = pattern.match(filename)
    if match:
        date_part = match.group(1)
        # Convert date part to the desired format
        formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]}"
        new_filename = f"hourly24_production_{formatted_date}.json"
        # Construct full file paths
        old_file_path = os.path.join(directory, filename)
        new_file_path = os.path.join(directory, new_filename)
        # Rename the file
        os.rename(old_file_path, new_file_path)
        print(f"Renamed: {filename} -> {new_filename}")

print("Renaming complete.")
