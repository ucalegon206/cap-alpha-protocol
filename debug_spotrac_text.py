from src.spotrac_scraper_v2 import SpotracScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

logging.basicConfig(level=logging.INFO)

with SpotracScraper(headless=True) as scraper:
    url = "https://www.spotrac.com/nfl/rankings/player/_/year/2024/sort/age"
    print(f"Loading {url}")
    scraper.driver.get(url)
    
    # Wait using updated selector
    WebDriverWait(scraper.driver, 60).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr, .list-group-item"))
    )
    
    # Determine finding method (table vs list)
    rows = scraper.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    if not rows:
        print("Using List Group")
        rows = scraper.driver.find_elements(By.CSS_SELECTOR, ".list-group-item")
    else:
        print("Using Table")
        
    print(f"Found {len(rows)} rows.")
    for i in range(5):
         if i < len(rows):
             print(f"--- Row {i+1} Text ---")
             print(rows[i].text)
             # Also print inner HTML to see structure
             # print(rows[i].get_attribute('innerHTML'))
             print("----------------------")
