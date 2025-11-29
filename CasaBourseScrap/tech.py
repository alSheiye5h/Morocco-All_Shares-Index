import requests 
from bs4 import BeautifulSoup
import pandas as pd
from .utils import *
import urllib3
import warnings
import json
import cloudscraper
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_ssl_session():
    """
    Create a session with SSL verification disabled and retry strategy
    """
    session = requests.Session()
    
    # Disable SSL verification
    session.verify = False
    
    # Add retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def getCours(name):
    """
    Load : Session data, latest transaction, best limit and data of the last 5 sessions

    Input  | Type              | Description
    ===============================================================================
    name   | String            | Name of the company. You must respect the notation.
           |                   | To get the notation : BVCscrap.notation()

    Output | Type              | Description
    =====================================================
           | Dictionary        | 
    """
    code = get_valeur(name) 
    data = {"__EVENTTARGET": "SocieteCotee1$LBIndicCle"}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.casablanca-bourse.com',
        'Referer': f'https://www.casablanca-bourse.com/bourseweb/Societe-Cote.aspx?codeValeur={code}&cat=7'
    }
    
    link = f"https://www.casablanca-bourse.com/bourseweb/Societe-Cote.aspx?codeValeur={code}&cat=7"
    
    try:
        session = create_ssl_session()
        res = session.post(link, data=data, headers=headers, timeout=15).content
        soup = BeautifulSoup(res, 'html.parser')
        result = getTables(soup)
        return result
    except Exception as e:
        print(f"❌ Error in getCours for {name}: {e}")
        return {"error": str(e)}
    
def getKeyIndicators(name, decode='utf-8'):
    """
    Load : get key indicators

    Input  | Type              | Description
    ===============================================================================
    name   | String            | Name of the company. You must respect the notation.
           |                   | To get the notation : BVCscrap.notation()

    Output | Type              | Description
    =====================================================
           | Dictionary        | 
    """
    code = get_valeur(name)
    data = {"__EVENTTARGET": "SocieteCotee1$LBFicheTech"}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.casablanca-bourse.com',
        'Referer': f'https://www.casablanca-bourse.com/bourseweb/Societe-Cote.aspx?codeValeur={code}&cat=7'
    }
    
    link = f"https://www.casablanca-bourse.com/bourseweb/Societe-Cote.aspx?codeValeur={code}&cat=7"
    
    try:
        session = create_ssl_session()
        res = session.post(link, data=data, headers=headers, timeout=15).content.decode(decode)
        soup = BeautifulSoup(res, 'html.parser')
        result = getTablesFich(soup)
        return result
    except Exception as e:
        print(f"❌ Error in getKeyIndicators for {name}: {e}")
        return {"error": str(e)}
    
def getDividend(name, decode='utf-8'):
    """
    Load : get dividends

    Input  | Type              | Description
    ===============================================================================
    name   | String            | Name of the company. You must respect the notation.
           |                   | To get the notation : BVCscrap.notation()

    Output | Type              | Description
    =====================================================
           | Dictionary        | 
    """
    code = get_valeur(name)
    data = {"__EVENTTARGET": "SocieteCotee1$LBDividende"}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.casablanca-bourse.com',
        'Referer': f'https://www.casablanca-bourse.com/bourseweb/Societe-Cote.aspx?codeValeur={code}&cat=7'
    }
    
    link = f"https://www.casablanca-bourse.com/bourseweb/Societe-Cote.aspx?codeValeur={code}&cat=7"
    
    try:
        session = create_ssl_session()
        res = session.post(link, data=data, headers=headers, timeout=15).content.decode(decode)
        soup = BeautifulSoup(res, 'html.parser')
        result = getDivi(soup)
        return result
    except Exception as e:
        print(f"❌ Error in getDividend for {name}: {e}")
        return {"error": str(e)}
    




def getIndex():
    """
    Load indexes summary using medias24 API
    Returns real data only - no mock fallbacks
    """
    return getIndex_simple()

def getIndex_simple():
    """
    Simple direct approach using the working medias24 APIs
    Returns only successful API data, empty dict if all fail
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01'
    }
    
    scraper = cloudscraper.create_scraper()
    index_data = {}
    
    # Get MASI data
    try:
        print("📡 Fetching MASI data...")
        masi_response = scraper.get(
            "https://medias24.com/content/api?method=getMasiHistory&periode=10y&format=json",
            headers=headers, 
            timeout=15
        )
        
        if masi_response.status_code == 200:
            masi_data = masi_response.json()
            if 'result' in masi_data and 'prices' in masi_data['result']:
                prices = masi_data['result']['prices']
                if prices:
                    latest = prices[-1]
                    previous = prices[-2] if len(prices) > 1 else latest
                    change_pct = ((latest - previous) / previous) * 100
                    
                    index_data['MASI'] = {
                        'value': f"{latest:,.2f}",
                        'change': f"{change_pct:+.2f}%",
                        'points': f"{latest - previous:+.2f}",
                        'raw_value': latest,
                        'source': 'medias24_masi_api',
                        'timestamp': datetime.now().isoformat()
                    }
                    print(f"✅ MASI: {index_data['MASI']['value']} ({index_data['MASI']['change']})")
                else:
                    print("❌ MASI: No price data available")
            else:
                print("❌ MASI: Invalid data structure")
        else:
            print(f"❌ MASI: API returned status {masi_response.status_code}")
    except Exception as e:
        print(f"❌ MASI API failed: {e}")
    
    # Get MSI20 data
    try:
        print("📡 Fetching MSI20 data...")
        msi20_response = scraper.get(
            "https://medias24.com/content/api?method=getIndexHistory&ISIN=msi20&periode=10y&format=json",
            headers=headers,
            timeout=15
        )
        
        if msi20_response.status_code == 200:
            msi20_data = msi20_response.json()
            if 'result' in msi20_data and 'prices' in msi20_data['result']:
                prices = msi20_data['result']['prices']
                if prices:
                    latest = prices[-1]
                    previous = prices[-2] if len(prices) > 1 else latest
                    change_pct = ((latest - previous) / previous) * 100
                    
                    index_data['MSI20'] = {
                        'value': f"{latest:,.2f}",
                        'change': f"{change_pct:+.2f}%",
                        'points': f"{latest - previous:+.2f}",
                        'raw_value': latest,
                        'source': 'medias24_msi20_api', 
                        'timestamp': datetime.now().isoformat()
                    }
                    print(f"✅ MSI20: {index_data['MSI20']['value']} ({index_data['MSI20']['change']})")
                else:
                    print("❌ MSI20: No price data available")
            else:
                print("❌ MSI20: Invalid data structure")
        else:
            print(f"❌ MSI20: API returned status {msi20_response.status_code}")
    except Exception as e:
        print(f"❌ MSI20 API failed: {e}")
    
    # Only add Resume indice if we have real data
    if index_data:
        index_data['Resume indice'] = {
            'MASI': index_data.get('MASI'),
            'MSI20': index_data.get('MSI20'),
            'last_update': datetime.now().isoformat()
        }
        print(f"✅ Successfully fetched {len(index_data) - 1} indices")
    else:
        print("❌ No index data could be fetched")
    
    return index_data

#  Fetches stock weightings/pondérations from Casablanca Stock Exchange website.
def getPond():
    """
    Load : weights (Pondération)

    Input  | Type              | Description
    ===============================================================================
           |                   |

    Output | Type              | Description
    =====================================================
           | Dictionary        | 
    """
    link = "https://www.casablanca-bourse.com/bourseweb/indice-ponderation.aspx?Cat=22&IdLink=298"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    }
    
    try:
        session = create_ssl_session()
        code_soup = session.get(link, headers=headers, timeout=15)
        soup = BeautifulSoup(code_soup.content, 'html.parser')
        return getPondval(soup)
    except Exception as e:
        print(f"❌ Error in getPond: {e}")
        return {"error": str(e)}

#Provides trading session summary using API data as fallback.
def getIndexRecap():
    """
    Load : session recap - FIXED VERSION
    """
    try:
        # Since the main website is broken, use medias24 API data
        index_data = getIndex_simple()
        
        # Create a recap structure similar to what the original function would return
        recap_data = {
            'MASI': index_data.get('MASI', {}),
            'MSI20': index_data.get('MSI20', {}),
            'timestamp': datetime.now().isoformat(),
            'source': 'medias24_api_fallback',
            'note': 'Original website structure changed, using API data'
        }
        
        return recap_data
        
    except Exception as e:
        print(f"❌ Error in getIndexRecap: {e}")
        return {"error": str(e)}
    
# Alternative functions for backward compatibility
def getIndex_alternative():
    """
    Alternative method to get index data if the main one fails
    """
    return getIndex_simple()

def extract_index_from_page(soup, index_name):
    """
    Extract index data from alternative pages
    This is a template - you'll need to customize based on actual page structure
    """
    # Look for common elements that might contain index data
    selectors = [
        f'[class*="{index_name.lower()}"]',
        f'[id*="{index_name.lower()}"]',
        '.index-value',
        '.financial-data',
        'table'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        for elem in elements:
            text = elem.get_text()
            if index_name.upper() in text.upper():
                return {"raw_data": text.strip()}
    
    return {"error": f"Could not extract {index_name} data"}

def getIndex_working():
    """
    Main working function that tries multiple methods
    """
    print("🔄 Fetching index data...")
    return getIndex_simple()

# Fix for getAllIndex to handle new website structure
def getAllIndex_fixed(soup):
    """
    Fixed version that returns proper structure for backward compatibility
    """
    # Since the website structure has changed, use API data instead
    api_data = getIndex_simple()
    
    # Return in the expected format with 'Resume indice'
    result = {
        'Resume indice': {
            'MASI': api_data.get('MASI', {}),
            'MSI20': api_data.get('MSI20', {}),
            'source': 'medias24_api_fallback'
        },
        'MASI': api_data.get('MASI', {}),
        'MSI20': api_data.get('MSI20', {})
    }
    
    return result

# Update the original getAllIndex to use the fixed version
def getAllIndex(soup):
    """
    Updated getAllIndex that uses API data as fallback
    """
    try:
        # First try to extract from the actual page
        tables = soup.find_all('table')
        if tables and len(tables) > 0:
            # If tables exist but are empty, use API fallback
            for table in tables:
                if len(table.find_all('tr')) > 1:  # If table has data
                    try:
                        from .utils import getIndexSumry
                        result = getIndexSumry(soup)
                        if result and 'error' not in result:
                            return result
                    except:
                        pass
        
        # Fallback to API data
        return getAllIndex_fixed(soup)
        
    except Exception as e:
        return getAllIndex_fixed(soup)



