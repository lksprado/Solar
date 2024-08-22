import requests
import os
import logging
import time
import datetime
import json
import random
import csv 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains

class EMAWebScraper:
    def __init__(self, config_file: str = 'src/config.json'):
        self.driver = None
        self.config_file = config_file
        self.config = self.read_config()
        self.username = self.config.get('username')
        self.password = self.config.get('password')
        self.bronze_dir = self.config.get('bronze_dir')
        self.base_url = 'https://apsystemsema.com/ema/index.action'
        self.cookies = None
        self.user_id = None
        self.service_dir = self.config.get('service_dir')
        self.missing_file = self.config.get('missing_file')
        
    # READING CONFIG FILE
    def read_config(self):
        with open(self.config_file, 'r') as file:
            config_data = json.load(file)
        return config_data

    # IDENTIFY MISSING DAYS AND APPEND TO LIST
    def read_missing(self, missing_file):
        days=[]
        with open(missing_file,'r') as file:
            next(file) 
            for line in file:
                days.append(line.split(',')[0].strip())
        
        days = sorted(days, key=lambda date: datetime.datetime.strptime(date, '%Y-%m-%d'))
        return days        
        

    # SETTING UP WEBDRIVER
    def setup_driver(self):
        edge_options = Options()
        edge_options.add_argument('--headless')
        edge_options.add_argument('--disable-gpu')
        edge_options.add_argument("--remote-debugging-port=9222")
        service = Service(executable_path=self.service_dir)
        self.driver = webdriver.Edge(options=edge_options)

    # LOGGING IN
    def login(self):
        try:
            self.driver.get(self.base_url)
            username_field = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, 'username'))
            )
            password_field = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, 'password'))
            )
            username_field.send_keys(self.username)
            password_field.send_keys(self.password)
            login_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, 'Login'))
            )
            login_button.click()
            time.sleep(3)
        except TimeoutException as e:
            logging.error("Timeout occurred while waiting for an element to be clickable: %s", str(e))
        except Exception as e:
            logging.error("An unexpected error occurred: %s", str(e))
            
    # GETTING THE AJAX 
    def ajax_finder(self):
        report_button = WebDriverWait(self.driver,10).until(
            EC.element_to_be_clickable((By.ID, 'report_head'))
        )
        report_button.click()
        
        system_data_button = WebDriverWait(self.driver,10).until(
            EC.element_to_be_clickable((By.ID, 'systemDataCustomer'))
        )
        system_data_button.click()
        
        ecu_data = WebDriverWait(self.driver,15).until(
            EC.element_to_be_clickable((By.ID, 'ecuData'))
        )
        ecu_data.click()
        
        # WAIT FOR IFRAME AND SWITCH TO IT
        iframe = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="configuration_body"]'))
        )
        self.driver.switch_to.frame(iframe)
        
        select = Select(self.driver.find_element(By.ID, 'chart'))
        
        select.select_by_value('2')
        
        time.sleep(3)

        # SWITCH BACK TO DEFAULT CONTENT
        self.driver.switch_to.default_content()
        
    # REQUESTING THROUGH AJAX 
    def fetch_production_data(self, query_date):
        try:
            self.cookies = self.driver.get_cookies()
            headers = {
                'Cookie': '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in self.cookies]),
                'User-Agent': 'Mozilla/5.0 (Windowstime.sleep(3) NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            }
            url = "https://apsystemsema.com/ema/ajax/getReportApiAjax/getHourlyEnergyOnCurrentDayAjax"

            for cookie in self.cookies:
                if cookie['name'] == 'userId':
                    self.user_id = cookie['value']

            payload = {
                'selectedValue': '216200001531',
                'queryDate': query_date,
                'systemId': self.user_id,
                'userId': self.user_id
            }
            response = requests.post(url, headers=headers, data=payload)
            
            file_date = f"{query_date[:4]}-{query_date[4:6]}-{query_date[6:]}"
            
            output_file = os.path.join(self.bronze_dir, f"hourly24_production_{file_date}.json")
            
            with open(output_file, "w") as f:
                json.dump(response.json(), f)
        except requests.exceptions.RequestException as e:
            logging.error("Error making HTTP request: %s", str(e))
        except Exception as e:
            logging.error("An error occurred: %s", str(e))

    def run(self):
        # CHECK FOR DATES DO SCRAP
        days = self.read_missing(self.missing_file)
        
        if not days:
            logging.info("No missing days to process.")
            return  # EXIT IF NOTHING IS FOUND
        
        
        self.setup_driver()
        self.login()
        self.ajax_finder()
        
        max_date = max(days)
        
        for day in days:
            if day > max_date:
                break
            query_date = datetime.datetime.strptime(day, '%Y-%m-%d').strftime('%Y%m%d')
            self.fetch_production_data(query_date)
        
        logging.info("Scraping completed. Quitting the driver.")
        self.driver.quit()

if __name__ == "__main__":
    scraper = EMAWebScraper()
    scraper.run()
