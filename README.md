# HOME SOLAR PANEL DATA FROM ETL TO VIZ

### DESCRIPTION
This is a personal project that emulates and end-to-end Data project applying data engineering concepts and best practices.
It consists of 4 steps:

1. Missing.py   
It fetches file dates from my landing directory and cross check with current date to get missing dates either from several days or just today;

2. Extraction.py    
It starts a webdriver instance, logs in the EmaApp System, navigates do desired page, makes a Request for daily hourly energy production from the given days in "missing_dates.csv" 

3. Transformation.py    
Converts json to csv file, remove unecessary columns, parse date and creates csv files in a processed directory;

4. Loading.py   
Upload csv file contents from processed directory to Postgres staging table and calls function to insert in the final table, moves the loaded files to a subfolder;

5. Gather.py
Creates a singles csv with all the contents, for data visualization purposes

### RESULT
The project is schedulled to run from a Airflow from docker container, daily, at 10pm.  
The Viz can be accessed in the following link: https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1
