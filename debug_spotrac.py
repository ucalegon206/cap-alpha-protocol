import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_spotrac():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    
    url = "https://www.spotrac.com/nfl/arizona-cardinals/cap/2024/"
    logger.info(f"Loading {url}")
    
    try:
        driver.get(url)
        time.sleep(10)
        logger.info(f"Title: {driver.title}")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables")
        
        for i, table in enumerate(tables):
            headers = [th.text.strip() for th in table.find_all('th')][:5]
            logger.info(f"Table {i} headers: {headers}")
            
    finally:
        driver.quit()

if __name__ == "__main__":
    test_spotrac()
