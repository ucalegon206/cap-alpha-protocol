
import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

url = "https://www.pro-football-reference.com/boxscores/202409050kan.htm"
logger.info(f"Fetching {url}")

resp = requests.get(url, headers=HEADERS)
html = resp.text.replace('<!--', '').replace('-->', '')
soup = BeautifulSoup(html, 'lxml')

print("--- Available Tables ---")
for tbl in soup.find_all('table'):
    print(f"ID: {tbl.get('id')} | Classes: {tbl.get('class')}")
