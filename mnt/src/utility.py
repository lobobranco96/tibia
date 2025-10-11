from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from datetime import datetime
import os

def selenium_webdriver():
  """
  Inicializa o driver do Selenium com Chromium em modo headless.

  Returns:
      webdriver.Chrome: instância configurada do driver.
  """
  chrome_options = Options()
  chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/141.0.7390.65 Safari/537.36")
  chrome_options.add_argument('--headless')
  chrome_options.add_argument('--no-sandbox')
  chrome_options.add_argument('--disable-dev-shm-usage')
  service = Service("/usr/local/share/chrome/chromedriver")
  return webdriver.Chrome(service=service, options=chrome_options)