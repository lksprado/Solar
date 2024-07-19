from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time

service = Service(executable_path="/media/lucas/Files/2.Projetos/3.Solar/.venv/bin/msedgedriver")
driver = webdriver.Edge(service=service)

driver.get("https://google.com")

time.sleep(5)

driver.quit()