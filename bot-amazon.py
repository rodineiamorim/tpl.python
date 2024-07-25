#
# bot para autenticacao na amazon - fba
#
# necessario: pip install selenium
#

# import module 
from selenium import webdriver 
import chromedriver_autoinstaller
import time 

chromedriver_autoinstaller.install()

navegador = webdriver.Chrome()

navegador.get("https://google.com/")
  
time.sleep(2)

navegador.quit()