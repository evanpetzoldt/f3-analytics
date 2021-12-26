import requests
#from requests.auth import HTTPBasicAuth
#from requests.auth import HTTPDigestAuth
import time
import pandas as pd
import os
from datetime import datetime
import pytz
import json

from xml.etree import ElementTree
from bs4 import BeautifulSoup, NavigableString, Tag
import lxml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)

options = Options()
#options.binary_location = "/home/epetz/drivers/chromedriver"    #chrome binary location specified here
options.add_argument("--start-maximized") #open Browser in maximized mode
options.add_argument("--no-sandbox") #bypass OS security model
options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
driver = webdriver.Chrome(options=options)


browser = webdriver.Chrome()

# Inputs
test_url = 'https://www.f3stcharles.com/ao-the-eagles-nest/'

#with requests.Session() as s:
# s = requests.Session()
# r = s.get(test_url)
driver.get(test_url)
html_source = driver.page_source

soup = BeautifulSoup(html_source, 'lxml')
# body = list(soup.children)[2]
# body2 = list(body.children)[1]
# body3 = list(body2.children)[5]
# list(body3.children)[164]
body = soup.body

body.find_all('pta-sus-calendar')

for child in list(section1.children):
    print(child)


d = body.descendants
len(d)
j = 0
for i in body.descendants:
    print(type(i))

calendar_table = body.find_all(class_="pta-sus-calendar")[0]
list(calendar_table)[1]
body.find_next("table")
print(calendar_container)
for child in list(calendar_container.children):
    p

pd.read_html(list(calendar_table)[1])

section1 = list(body.children)[1]
section2 = list(section1.children)[1]
print(section1)