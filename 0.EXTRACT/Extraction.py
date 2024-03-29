#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
import requests
import os
import config
import logging
import time
import datetime
import json
import random


# LOGIN STEP
try:
    # CREATE WEBDRIVER INSTANCE FOR GOOGLE CHROME
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument("--remote-debugging-port=9222")
    driver = webdriver.Chrome(options=chrome_options)
    # OPEN WEBSITE
    driver.get('https://apsystemsema.com/ema/index.action')
    # GET CREDENTIALS FROM CONFIG FILE
    username = config.username
    password = config.password
    # WAITS FOR [USERNAME] FIELD ELEMENT TO BE CLICKABLE FOR 10 SECONDS
    username_field = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.ID, 'username'))
    )
    # WAITS FOR [PASSWORD] FIELD ELEMENT TO BE CLICKABLE FOR 10 SECONDS
    password_field = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.ID, 'password'))
    )
    # TYPES THE[USERNAME] AND PASSWORDS FIELDS
    username_field.send_keys(username)
    password_field.send_keys(password)
    # WAITS FOR [LOGIN] FIELD ELEMENT TO BE CLICKABLE FOR 10 SECONDS
    login_button = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.ID, 'Login'))
    )
    # CLICKS ELEMENT TO LOGIN WAITS 3 SECONDS WHILE LOADNG
    login_button.click()
    time.sleep(3)
except Exception as e:
    logging.error("An unexpected error occurred: %s", str(e))
except TimeoutException as e:
    logging.error("Timeout occurred while waiting for an element to be clickable: %s", str(e))
except Exception as e:
    logging.error("An unexpected error occurred: %s", str(e))


# FETCH DAILY HOURLY PRODUCTION
driver.get('https://apsystemsema.com/ema/security/optmainmenu/intoLargeReport.action')
time.sleep(random.randint(1, 5)) # RANDOM REQUEST TIME WINDOW TO NOT RAISE SUSPICIOUS ACTIVITY IN SERVER SIDE
try:
    cookies = driver.get_cookies()
    headers = {
    'Cookie': '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies]),
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    url = "https://apsystemsema.com/ema/ajax/getReportApiAjax/getHourlyEnergyOnCurrentDayAjax"
    
    user_id = None
    for cookie in cookies:
        if cookie['name'] == 'userId':
            user_id = cookie['value']

    payload = {
        'selectedValue': '216200001531',
        'queryDate': datetime.datetime.now().strftime('%Y%m%d'),
        'systemId': user_id,
        'userId': user_id
    }
    # SENT POST REQUEST
    response = requests.post(url, headers=headers, data=payload)
    # GET CURRENT TIME
    current_time = time.time()
    # #CONVERT CURRENT DATETIME
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    # #DEFINE OUT PUT FILE LOCATION AND VARIABLE NAME
    output_file = os.path.join(config.jsondir, f"hourly24_production_{current_date}.json")
    #SAVE JSON FILE
    with open(output_file, "w") as f:
        json.dump(response.json(), f)
except requests.exceptions.RequestException as e:
    print("Error making HTTP request:", e)
except Exception as e:
    print("An error occurred:", e)
finally:
        driver.quit()

