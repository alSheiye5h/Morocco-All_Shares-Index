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

def loadata_selenium(name, start=None, end=None):
    """Load data using Selenium with proper data handling"""
    code = get_code(name)
    
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

    print(f"Fetching with Selenium: {url}")
    html_content = fetch_with_selenium(url)
    
    if html_content:
        try:
            # First, try to parse the entire content as JSON
            try:
                data = json.loads(html_content)
                print("Successfully parsed direct JSON response")
                if name not in ("MASI", "MSI20"):
                    return get_data(data, "utf-8")
                else:
                    data_all = get_index(data, "utf-8")
                    return produce_data(data_all, start, end) if (start and end) else data_all
            except json.JSONDecodeError:
                pass  # Not direct JSON, continue with HTML parsing
            
            # Try to extract JSON from script tags
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for JSON in pre tags (common for API responses)
            pre_tags = soup.find_all('pre')
            for pre in pre_tags:
                try:
                    data = json.loads(pre.text)
                    print("Found JSON in pre tag")
                    if name not in ("MASI", "MSI20"):
                        return get_data(data, "utf-8")
                    else:
                        data_all = get_index(data, "utf-8")
                        return produce_data(data_all, start, end) if (start and end) else data_all
                except json.JSONDecodeError:
                    continue
            
            # Look for JSON in script tags
            script_tags = soup.find_all('script')
            for script in script_tags:
                script_content = script.string
                if script_content:
                    # Try to extract JSON from JavaScript variables
                    import re
                    json_pattern = r'\{.*"result".*\}'
                    matches = re.findall(json_pattern, script_content, re.DOTALL)
                    for match in matches:
                        try:
                            data = json.loads(match)
                            print("Found JSON in script tag")
                            if name not in ("MASI", "MSI20"):
                                return get_data(data, "utf-8")
                            else:
                                data_all = get_index(data, "utf-8")
                                return produce_data(data_all, start, end) if (start and end) else data_all
                        except json.JSONDecodeError:
                            continue
            
            # Final fallback: try to parse the entire HTML
            print("Falling back to HTML parsing")
            if name not in ("MASI", "MSI20"):
                return get_data(soup, "utf-8")
            else:
                data_all = get_index(soup, "utf-8")
                return produce_data(data_all, start, end) if (start and end) else data_all
                
        except Exception as e:
            print(f"Error processing Selenium response: {e}")
            raise RuntimeError(f"Failed to process data for {name}")
    
    raise RuntimeError(f"Selenium failed to fetch data for {name}")

def loadmany(*args, start=None, end=None, feature="value", decode="utf-8", method="auto"):
    """
    Load the data of many equities  
    Inputs: 
            Input   | Type                             | Description
            =================================================================================
             *args  |strings                           | You must respect the notation. To see the notation see BVCscrap.notation
             start  |string "YYYY-MM-DD"               | starting date Must respect the notation
             end    |string "YYYY-MM-DD"               | Must respect the notation
             feature|string                            | Variable : value, min, max, variation or volume (lowercase)
             decode |string                            | type of decoder. default value is utf-8. If it is not working use utf-8-sig
             method |string                            | "auto", "cloudscraper", or "selenium"
    Outputs:
            Output | Type                                 | Description
           ================================================================================= 
                   | pandas.DataFrame (len(args) columns) | close prices of selected equities
    """
    if len(args) == 0:
        raise ValueError("No stocks or indices provided")
    
    # Handle list input
    if len(args) == 1 and isinstance(args[0], list):
        stocks = args[0]
    else:
        stocks = list(args)
    
    print(f"📊 Loading data for {len(stocks)} securities: {stocks}")
    if start and end:
        print(f"📅 Date range: {start} to {end}")
    print(f"📈 Feature: {feature}")
    
    data_dict = {}
    failed_stocks = []
    
    for stock in stocks:
        try:
            print(f"\n🔄 Loading {stock}...")
            # FIX: Call loadata directly, not bvc.loadata
            value_data = load_data(stock, start=start, end=end, decode=decode, method=method)
            
            if value_data is not None and len(value_data) > 0:
                # Check available columns (case-insensitive)
                available_columns = value_data.columns.tolist()
                print(f"   Available columns: {available_columns}")
                
                # Try to find the feature (case-insensitive)
                matching_columns = [col for col in available_columns if col.lower() == feature.lower()]
                
                if matching_columns:
                    actual_feature = matching_columns[0]
                    data_dict[stock] = value_data[actual_feature]
                    print(f"✅ {stock}: Success - {len(value_data)} records (using '{actual_feature}' column)")
                else:
                    print(f"❌ {stock}: Feature '{feature}' not found. Available: {available_columns}")
                    failed_stocks.append(stock)
            else:
                print(f"❌ {stock}: No data returned")
                failed_stocks.append(stock)
                
        except Exception as e:
            print(f"❌ {stock}: Error - {e}")
            failed_stocks.append(stock)
    
    # Create the final DataFrame
    if data_dict:
        # Align all series by index (date)
        data = pd.DataFrame(data_dict)
        
        # Sort by index if it's datetime
        if hasattr(data.index, 'sort_values'):
            data = data.sort_index()
        
        print(f"\n✅ Successfully loaded {len(data_dict)} out of {len(stocks)} securities")
        print(f"📊 Final data shape: {data.shape}")
        
        if failed_stocks:
            print(f"❌ Failed to load: {failed_stocks}")
            
        return data
    else:
        print("❌ No data was successfully loaded")
        return pd.DataFrame()

# 
#    -------- helper ---------
#

def getIntraday(name, decode="utf-8"):
    """
    Load intraday data with Cloudflare bypass
    Inputs: 
        - name: stock, index 
        - decode: default value is "utf-8", if it is not working use : "utf-8-sig"
    """
    try:
        if name not in ("MASI", "MSI20"):
            code = get_code(name)
            if not code or code == 'None':
                print(f"❌ Could not find code for {name}")
                return None
            link = f"https://medias24.com/content/api?method=getStockIntraday&ISIN={code}&format=json"
            print(f"📡 Fetching intraday data for stock {name} (code: {code})")
        elif name == "MASI":
            link = "https://medias24.com/content/api?method=getMarketIntraday&format=json"
            print(f"📡 Fetching intraday data for MASI index")
        else:
            link = "https://medias24.com/content/api?method=getIndexIntraday&ISIN=msi20&format=json"
            print(f"📡 Fetching intraday data for MSI20 index")
        
        # Use cloudscraper to bypass Cloudflare
        scraper = cloudscraper.create_scraper()
        request_data = scraper.get(link, timeout=15)
        print(f"🔗 Response status: {request_data.status_code}")
        
        if request_data.status_code == 200:
            soup = BeautifulSoup(request_data.text, "html.parser")
            data = intradata(soup, decode)
            
            if data is not None and len(data) > 0:
                print(f"✅ Intraday data loaded successfully!")
                print(f"📊 Data shape: {data.shape}")
                print(f"🏷️ Columns: {data.columns.tolist()}")
                return data
            else:
                print(f"❌ No intraday data returned")
                return None
        else:
            print(f"❌ HTTP Error {request_data.status_code}")
            if len(request_data.text) < 500:
                print(f"Response: {request_data.text}")
            else:
                print(f"Response preview: {request_data.text[:200]}...")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching intraday data for {name}: {e}")
        import traceback
        traceback.print_exc()
        return None

