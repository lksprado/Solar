# HOME SOLAR PANEL DATA FROM ETL TO VIZ

### DESCRIPTION
This is a personal project that emulates and end-to-end Data project applying data engineering concepts and using python best practices for coding (OOP).
It consists of 4 steps:

1. Missing.py   
Reads my landing (local) repository to identify missing dates and create a "missing_dates.csv" file for querying further in the flow;

2. Extraction.py    
Starts a webdriver instance, logs in the EmaApp System, navigates do desired page, make a Request for daily hourly energy production from the given days in "missing_dates.csv" 

3. Transformation.py    
Converts json to csv file, remove unecessary columns, parse date and creates csv files;

4. Loading.py   
Upload csv file contents to Postgres staging table and calls procedure to insert in the final table;

### RESULT
The project is schedulled to run from a Airflow from docker container, daily, at 10pm.  
The Viz can be accessed in the following link: https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1
