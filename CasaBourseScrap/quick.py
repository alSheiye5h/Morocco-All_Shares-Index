from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time

def scrape_casablanca_bourse_js_click():
    """
    Scrape Casablanca Stock Exchange instruments using JavaScript clicks
    to bypass element interception issues
    """
    # Set up Chrome options
    chrome_options = Options()
    # Remove headless for debugging, you can add it back after testing
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # no chrome window
    chrome_options.add_argument("--disable-gpu")  # Often needed for headless
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--remote-debugging-port=9222")  # Optional: for headless debugging

    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    
    all_data = []
    
    try:
        print("🌐 Navigating to Casablanca Bourse website...")
        driver.get("https://www.casablanca-bourse.com/fr/marche-cash/instruments-actions")
        
        # Wait for page to load completely
        time.sleep(5)
        print("✅ Page loaded successfully")
        
        # ==================== PAGE 1 ====================
        print("\n📄 SCRAPING PAGE 1...")
        
        # Find all rows in the table
        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        print(f"📊 Found {len(rows)} rows on page 1")
        
        # Scrape data from page 1
        page1_count = 0
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 7:
                    data = {
                        'Ticker': cells[0].text.strip(),
                        'Code_ISIN': cells[1].text.strip(),
                        'Emetteur': cells[2].text.strip(),
                        'Instrument': cells[3].text.strip(),
                        'Categorie': cells[4].text.strip(),
                        'Compartiment': cells[5].text.strip(),
                        'Nombre_titres': cells[6].text.strip(),
                        'Page': 1
                    }
                    all_data.append(data)
                    page1_count += 1
                    print(f"  ✅ {data['Ticker']} - {data['Instrument']}")
            except Exception as e:
                print(f"  ❌ Error scraping row: {e}")
                continue
        
        print(f"🎯 Successfully scraped {page1_count} instruments from page 1")
        
        # ==================== PAGE 2 ====================
        print("\n🔄 NAVIGATING TO PAGE 2...")
        
        # Find the page 2 button using multiple selector strategies
        page2_button = None
        
        # Strategy 1: XPath with class and text
        page2_buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'text-sm') and contains(@class, 'leading-5') and text()='2']")
        
        if page2_buttons:
            page2_button = page2_buttons[0]
            print("✅ Found page 2 button using XPath")
        else:
            # Strategy 2: CSS selector with text content
            all_buttons = driver.find_elements(By.CSS_SELECTOR, "button.text-sm.leading-5")
            for button in all_buttons:
                if button.text.strip() == "2":
                    page2_button = button
                    print("✅ Found page 2 button using CSS selector")
                    break
        
        if page2_button:
            # Scroll the button into view
            print("🖱️ Scrolling to page 2 button...")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", page2_button)
            time.sleep(2)
            
            # Click using JavaScript to bypass element interception
            print("🖱️ Clicking page 2 button using JavaScript...")
            driver.execute_script("arguments[0].click();", page2_button)
            print("✅ Page 2 button clicked successfully")
            
            # Wait for the page to load and data to refresh
            print("⏳ Waiting for page 2 to load...")
            time.sleep(5)
            
            # Verify we're on page 2 by checking if the active page indicator changed
            try:
                active_buttons = driver.find_elements(By.CSS_SELECTOR, "button.bg-primary-500")
                if active_buttons:
                    print("✅ Successfully navigated to page 2 (active button found)")
            except:
                print("⚠️ Could not verify active page, but continuing...")
            
            # ==================== SCRAPE PAGE 2 ====================
            print("\n📄 SCRAPING PAGE 2...")
            
            # Find all rows in the table on page 2
            rows_page2 = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
            print(f"📊 Found {len(rows_page2)} rows on page 2")
            
            # Scrape data from page 2
            page2_count = 0
            for row in rows_page2:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 7:
                        data = {
                            'Ticker': cells[0].text.strip(),
                            'Code_ISIN': cells[1].text.strip(),
                            'Emetteur': cells[2].text.strip(),
                            'Instrument': cells[3].text.strip(),
                            'Categorie': cells[4].text.strip(),
                            'Compartiment': cells[5].text.strip(),
                            'Nombre_titres': cells[6].text.strip(),
                            'Page': 2
                        }
                        all_data.append(data)
                        page2_count += 1
                        print(f"  ✅ {data['Ticker']} - {data['Instrument']}")
                except Exception as e:
                    print(f"  ❌ Error scraping row on page 2: {e}")
                    continue
            
            print(f"🎯 Successfully scraped {page2_count} instruments from page 2")
            
        else:
            print("❌ Page 2 button not found. Only page 1 data will be saved.")
            
        return final_data(all_data)
        
    except Exception as e:
        print(f"❌ Fatal error during scraping: {e}")
        import traceback
        traceback.print_exc()
        return final_data(all_data)  # Return whatever data we collected so far
        
    finally:
        # Close the browser
        print("\n🔚 Closing browser...")
        driver.quit()

def final_data(data):
    if not data:
        print("❌ No data to save")
        return
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Remove duplicates based on Ticker
    initial_count = len(df)
    df = df.drop_duplicates(subset=['Ticker'])
    final_count = len(df)
    
    # REMOVE THE PAGE COLUMN
    if 'Page' in df.columns:
        df = df.drop('Page', axis=1)
        print("✅ Removed 'Page' column from final data")
    
    print(f"\n📈 DATA ANALYSIS:")
    print(f"   Total records scraped: {initial_count}")
    print(f"   Unique instruments: {final_count}")
    
    if initial_count != final_count:
        print(f"   Duplicates removed: {initial_count - final_count}")
    
    # Show distribution by compartment
    compartment_counts = df['Compartiment'].value_counts()
    print(f"\n🏢 Distribution by compartment:")
    for compartment, count in compartment_counts.items():
        print(f"   {compartment}: {count} instruments")
    
    # Show distribution by category
    category_counts = df['Categorie'].value_counts()
    print(f"\n📑 Distribution by category:")
    for category, count in category_counts.items():
        print(f"   {category}: {count} instruments")

    # Save to CSV
    # output_file = 'casablanca_bourse_instruments_complete.csv'
    # df.to_csv(output_file, index=False, encoding='utf-8')
    # print(f"\n💾 Data saved to: {output_file}")
    
    return df




