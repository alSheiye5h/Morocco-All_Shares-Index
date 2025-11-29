import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from .utils import *
import cloudscraper
import time
import random
import warnings
from datetime import datetime, timedelta
import traceback




def load_data(name, start=None, end=None, decode='utf-8', method='auto'):
    if method == "auto":
        methods = ["cloudscraper", "selenium"]
        for method_try in methods:
            try:
                print(f"Trying method: {method_try}")
                if method_try == "cloudscraper":
                    return loadata_cloudscraper(name, start, end)
                # elif method_try == "selenium":
                    # return loadata_selenium(name, start, end)
            except Exception as e:
                print(f"Method {method_try} failed: {e}")
                continue
                
        # Final fallback to original method
        print("Falling back to original method")
        
    elif method == "cloudscraper":
        return loadata_cloudscraper(name, start, end)
    # else :
    #     return loadata_selenium(name, start, end)
    

def loadata_cloudscraper(name, start=None, end=None):
    """Load data using cloudscraper with proper data handling"""
    code = get_code(name)
    
    # Build URL
    if name not in ("MASI", "MSI20"):
        if start and end:
            url = f"https://medias24.com/content/api?method=getPriceHistory&ISIN={code}&format=json&from={start}&to={end}"
        else:
            start = '2011-09-18'
            end = str(datetime.today().date())
            url = f"https://medias24.com/content/api?method=getPriceHistory&ISIN={code}&format=json&from={start}&to={end}"
    else:
        if name == "MASI":
            url = "https://medias24.com/content/api?method=getMasiHistory&periode=10y&format=json"
        else:
            url = "https://medias24.com/content/api?method=getIndexHistory&ISIN=msi20&periode=10y&format=json"

    print(f"Fetching with cloudscraper: {url}")
    resp = fetch_with_cloudscraper(url)
    
    if resp and resp.status_code == 200:
        try:
            # Try to parse as JSON
            data = resp.json()
            print(f"Successfully parsed JSON data for {name}")
            
            if name not in ("MASI", "MSI20"):
                result = get_data(data, "utf-8")
            else:
                data_all = get_index(data, "utf-8")
                result = produce_data(data_all, start, end) if (start and end) else data_all
            
            print(f"Successfully processed data for {name}")
            return result
            
        except ValueError as e:
            print(f"JSON parsing failed: {e}, trying HTML parsing...")
            # Not JSON, try HTML parsing
            soup = BeautifulSoup(resp.text, 'html.parser')
            if name not in ("MASI", "MSI20"):
                return get_data(soup, "utf-8")
            else:
                data_all = get_index(soup, "utf-8")
                return produce_data(data_all, start, end) if (start and end) else data_all
        except Exception as e:
            print(f"Error processing data: {e}")
            raise RuntimeError(f"Failed to process data for {name}")
    
    raise RuntimeError(f"Cloudscraper failed to fetch data for {name} (Status: {resp.status_code if resp else 'No response'})")

def fetch_with_cloudscraper(url, max_retries=3):
    """Use cloudscraper to bypass Cloudflare protection"""
    try:
        scraper = cloudscraper.create_scraper()
        
        for attempt in range(max_retries):
            try:
                resp = scraper.get(url, timeout=30)
                print(f"Attempt {attempt + 1}: Status {resp.status_code}")
                if resp.status_code == 200:
                    return resp
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
        
        return None
    except ImportError:
        print("cloudscraper requests Failed. (try fetch_url_with_fallback(url))")

# ----------------------------------- Selenium ---------------------------------------
    
def fetch_with_selenium(url, wait_time=10):
    """Use Selenium to bypass anti-bot protection - ADD THIS FUNCTION"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.get(url)
            
            # Wait for the page to load
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Try to find JSON data or relevant content
            page_source = driver.page_source
            return page_source
            
        except Exception as e:
            print(f"Selenium error: {e}")
            return None
        finally:
            driver.quit()
    except ImportError:
        print("Selenium not installed. Please install: pip install selenium webdriver-manager")
        return None


