import psycopg2
from datetime import datetime, timedelta
import pandas as pd 
import csv
import os 

class DatabaseConnection:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def fetch_max_date(self, query):
        self.cursor.execute(query)
        max_date = self.cursor.fetchall()
        return max_date.pop()[0] if max_date else None

class MissingDatesFinder:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.filepath = '/media/lucas/Files/2.Projetos/6.scrapping' # /media/lucas/Files/2.Projetos/6.scrapping /opt/airflow/data/solar_project

    def get_max_date_from_db(self):
        query = """
            SELECT MAX(substring(sr.filename,21,10) ::TIMESTAMP::DATE) AS DT 
            FROM dw_lcs.solar_raw sr
        """
        max_date_value = self.db_connection.fetch_max_date(query)
        return datetime.strptime(str(max_date_value), '%Y-%m-%d').date()

    def get_current_date(self):
        now = datetime.now()
        if now.hour >= 20:
            return now.date()
        else:
            return (now - timedelta(days=1)).date()

    def find_missing_dates(self):
        start_date = self.get_max_date_from_db()+ timedelta(days=1)
        current_date = self.get_current_date()
        missing_dates = pd.date_range(start_date, current_date, freq='d').strftime('%Y-%m-%d').tolist()
        return missing_dates
    
    def make_temp_file(self):
        missing_dates = self.find_missing_dates()
        full_path = os.path.join(self.filepath, 'missing_dates.csv')
        with open(full_path, mode='w') as file:
            writer = csv.writer(file)
            for missing_date in missing_dates:
                writer.writerow([missing_date])        

def main():
    # Database connection parameters
    db_params = {
        'host': 'localhost',
        'port': '5432',
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'pg12345'
    }

    db_connection = DatabaseConnection(**db_params)
    db_connection.connect()


    finder = MissingDatesFinder(db_connection)
    finder.make_temp_file()
    db_connection.disconnect()

if __name__ == "__main__":
    main()

