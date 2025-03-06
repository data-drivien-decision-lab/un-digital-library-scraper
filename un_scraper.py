import os
import time
import re
import json
import csv
import logging
import platform
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementNotInteractableException,
    StaleElementReferenceException
)
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging for minimal output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# Constants
BASE_SEARCH_URL = (
    "https://digitallibrary.un.org/search?cc=Voting%20Data&ln=en&p=&f=&rm=&sf=&so=d"
    "&rg=50&c=Voting%20Data&c=&of=hb&fti=1&fct__9=Vote&fti=1"
)
CSV_FILE = "data/UN_VOTING_DATA.csv"  # Single master CSV file
MAX_PAGES_PER_YEAR = 50  # Adjust as needed

# Parallelism settings
MAX_WORKERS = 2  # Number of parallel browser sessions (be careful not to use too many)

# Fixed columns for CSV output
FIXED_COLUMNS = [
    "Council", "Date", "Title", "Resolution", "TOTAL VOTES", "NO-VOTE COUNT",
    "ABSENT COUNT", "NO COUNT", "YES COUNT", "Link", "token", "Scrape_Year"
]

# Anti-blocking measures - minimized but effective
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
]

def prevent_sleep():
    """Prevent system sleep if running on Windows."""
    if platform.system() == 'Windows':
        import ctypes
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        except Exception as e:
            logging.warning(f"Error preventing sleep: {e}")

def get_driver():
    """Configure and return a Chrome webdriver instance with minimal anti-blocking."""
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Essential anti-blocking measures
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # Performance optimizations
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--js-flags=--expose-gc")
    options.add_argument("--aggressive-cache-discard")
    options.add_argument("--disable-site-isolation-trials")
    
    # Get driver path using webdriver_manager
    driver_path = ChromeDriverManager().install()
    try:
        os.chmod(driver_path, 0o755)
    except Exception as e:
        logging.warning(f"Could not set executable permissions for {driver_path}: {e}")
        
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(45)  # Slightly reduced timeout for faster failure detection
    
    # Basic stealth
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def check_for_next_button(driver):
    """Return the next page button element if found, otherwise None."""
    next_button = None
    xpaths = [
        "//a[.//img[@alt='next'] or .//img[@aria-label='Next page']]",
        "//a[@class='img' and contains(@href, 'jrec=')]",
        "//a[contains(text(), '›') or contains(text(), 'next') or contains(text(), 'Next')]"
    ]
    for xp in xpaths:
        try:
            next_button = driver.find_element(By.XPATH, xp)
            if next_button:
                break
        except NoSuchElementException:
            continue
    if not next_button:
        pagination_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'jrec=')]")
        highest_jrec = 0
        for link in pagination_links:
            try:
                href = link.get_attribute("href")
                match = re.search(r'jrec=(\d+)', href)
                if match:
                    jrec = int(match.group(1))
                    if jrec > highest_jrec:
                        highest_jrec = jrec
                        next_button = link
            except Exception:
                continue
    return next_button

def get_available_years(driver):
    """Extract available years and their counts from the date facet."""
    date_facets = []
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//ul[contains(@class, 'option-fct')]"))
        )
        
        date_headers = driver.find_elements(By.XPATH, "//h2[text()='Date']")
        for header in date_headers:
            try:
                facet_section = header.find_element(By.XPATH, "./following-sibling::ul[contains(@class, 'option-fct')]")
                if "expanded" not in facet_section.get_attribute("class"):
                    try:
                        show_more = header.find_element(By.XPATH, "./following-sibling::span[contains(@class, 'showmore')]")
                        driver.execute_script("arguments[0].click();", show_more)
                        time.sleep(0.5)
                    except (NoSuchElementException, ElementNotInteractableException):
                        pass
                
                year_inputs = facet_section.find_elements(By.XPATH, ".//input[@type='checkbox']")
                for inp in year_inputs:
                    year_value = inp.get_attribute("value")
                    try:
                        label = driver.find_element(By.XPATH, f"//label[@for='{inp.get_attribute('id')}']")
                        year_text = label.text.strip()
                        match = re.match(r'(\d{4})\s*\((\d+)\)', year_text)
                        if match:
                            year, count = match.group(1), int(match.group(2))
                            date_facets.append({
                                'year': year,
                                'count': count,
                                'input_id': inp.get_attribute('id'),
                                'input_value': year_value
                            })
                    except NoSuchElementException:
                        continue
            except Exception as e:
                logging.error(f"Error processing date header: {e}")
                continue
        return sorted(date_facets, key=lambda x: x['year'], reverse=True)
    except Exception as e:
        logging.error(f"Error getting available years: {e}")
        return []

def select_year_facet(driver, year_data, max_retries=2):
    """Select a specific year - optimized for speed."""
    for retry in range(max_retries):
        try:
            logging.info(f"Selecting year: {year_data['year']} (Attempt {retry+1}/{max_retries})")
            
            # Find and select the checkbox
            checkbox = driver.find_element(By.ID, year_data['input_id'])
            driver.execute_script("arguments[0].scrollIntoView();", checkbox)
            time.sleep(0.2)
            
            # Click with JavaScript
            driver.execute_script("arguments[0].click();", checkbox)
            time.sleep(1)  # Minimum wait
            
            # Wait for results to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/record/')]"))
            )
            
            # Simple verification - just check if we have results
            records = driver.find_elements(By.XPATH, "//a[contains(@href, '/record/')]")
            if records and len(records) > 0:
                logging.info(f"Selected year {year_data['year']} with {len(records)} visible records")
                return True
            else:
                logging.warning(f"Year {year_data['year']} selected but no records visible")
                if retry < max_retries - 1:
                    clear_filters(driver)
        except TimeoutException:
            logging.warning(f"Timeout on attempt {retry+1} for year {year_data['year']}")
            if retry < max_retries - 1:
                clear_filters(driver)
        except Exception as e:
            logging.error(f"Error selecting year {year_data['year']}: {e}")
            if retry < max_retries - 1:
                clear_filters(driver)
    
    # Quick fallback approach
    try:
        logging.warning(f"Trying fallback for year {year_data['year']}...")
        driver.get(BASE_SEARCH_URL)
        time.sleep(1.5)
        
        # Try explicit wait for facets
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//h2[text()='Date']"))
        )
        
        checkbox = driver.find_element(By.ID, year_data['input_id'])
        driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(1.5)
        
        records = driver.find_elements(By.XPATH, "//a[contains(@href, '/record/')]")
        if records and len(records) > 0:
            logging.info(f"Fallback: Selected year {year_data['year']} with {len(records)} visible records")
            return True
    except Exception as e:
        logging.error(f"Fallback selection failed: {e}")
    
    logging.error(f"Failed to select year {year_data['year']} after multiple attempts")
    return False

def collect_links_for_year(driver, year):
    """Collect links for the given year - optimized for speed."""
    all_links = set()
    page_count = 0
    consecutive_no_new = 0
    max_no_new = 3

    while page_count < MAX_PAGES_PER_YEAR:
        prevent_sleep()
        page_count += 1
        logging.info(f"[Year {year}] Processing page {page_count}...")
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/record/')]"))
            )
        except TimeoutException:
            logging.warning(f"[Year {year}] Timeout waiting for results on page {page_count}")
            break

        links_before = len(all_links)
        
        try:
            # More efficient link collection
            links = set()
            elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/record/')]")
            
            for elem in elements:
                try:
                    href = elem.get_attribute("href")
                    if href and '/record/' in href:
                        base_url = href.split('?')[0]
                        if '?' in href:
                            params = href.split('?')[1].split('&')
                            ln_param = [p for p in params if p.startswith('ln=')]
                            if ln_param:
                                base_url = f"{base_url}?{ln_param[0]}"
                        links.add(base_url)
                except StaleElementReferenceException:
                    continue
            
            all_links.update(links)
        except Exception as e:
            logging.error(f"[Year {year}] Error collecting links: {e}")

        new_count = len(all_links) - links_before
        logging.info(f"[Year {year}] Found {new_count} new links on page {page_count} (Total: {len(all_links)})")
        
        if new_count == 0:
            consecutive_no_new += 1
            if consecutive_no_new >= max_no_new:
                logging.info(f"[Year {year}] No new links on {max_no_new} consecutive pages; stopping pagination.")
                break
        else:
            consecutive_no_new = 0

        next_button = check_for_next_button(driver)
        if next_button:
            try:
                driver.execute_script("arguments[0].scrollIntoView();", next_button)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(1)  # Minimum wait after clicking next
            except Exception as e:
                logging.error(f"[Year {year}] Error clicking next button: {e}")
                break
        else:
            logging.info(f"[Year {year}] No next button found; reached last page.")
            break
    
    return list(all_links)

def process_resolution(link, driver, year):
    """Process a single resolution and return row data."""
    try:
        # Extract record ID from the link
        record_id = link.split('/record/')[1].split('?')[0] if '/record/' in link else link.split('/')[-1]
        short_link = f".../record/{record_id}"
        
        # Log with shorter link for readability
        logging.info(f"Loading: {short_link}")
        
        # Load the page
        driver.get(link)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'metadata-row')]"))
        )
        
        # Create base row data
        row_data = {
            "Link": link,
            "token": record_id,
            "Scrape_Year": year
        }
        
        # Process HTML content
        html_content = driver.page_source
        extracted = extract_vote_data_from_html(html_content)
        
        if extracted:
            if extracted.get('Title'):
                row_data['Title'] = extracted['Title']
                row_data['Council'] = determine_council(extracted['Title'])
            if extracted.get('Resolution'):
                row_data['Resolution'] = extracted['Resolution']
            if extracted.get('Vote date'):
                row_data['Date'] = extracted['Vote date']
            if extracted.get('Vote summary'):
                summary = extracted['Vote summary']
                if m := re.search(r'Yes:\s*(\d+)', summary):
                    row_data['YES COUNT'] = m.group(1)
                if m := re.search(r'No:\s*(\d+)', summary):
                    row_data['NO COUNT'] = m.group(1)
                if m := re.search(r'Abstentions:\s*(\d+)', summary):
                    row_data['ABSENT COUNT'] = m.group(1)
                if m := re.search(r'Non-Voting:\s*(\d+)', summary):
                    row_data['NO-VOTE COUNT'] = m.group(1)
                if m := re.search(r'Total voting membership:\s*(\d+)', summary):
                    row_data['TOTAL VOTES'] = m.group(1)
            if 'Vote Data' in extracted:
                for country, vote in extracted['Vote Data'].items():
                    if vote == 'Y':
                        row_data[country] = 'YES'
                    elif vote == 'N':
                        row_data[country] = 'NO'
                    elif vote == 'A':
                        row_data[country] = 'ABSTAIN'
        
        # Return data if we have meaningful fields
        if row_data.get('Title') or row_data.get('Resolution'):
            return row_data
        return None
    except Exception as e:
        logging.error(f"Error processing link {link}: {e}")
        return None

def batch_scrape_resolutions(links, driver, year, batch_size=15):
    """Scrape resolutions in batches and return rows ready for CSV."""
    batch_rows = []
    total_links = len(links)
    
    for i in range(0, total_links, batch_size):
        batch = links[i:i+batch_size]
        logging.info(f"Processing batch {i//batch_size + 1}/{(total_links + batch_size - 1)//batch_size} ({len(batch)} links)")
        
        for link in batch:
            row_data = process_resolution(link, driver, year)
            if row_data:
                batch_rows.append(row_data)
            time.sleep(0.2)  # Minimal delay between links
        
        # Very small delay between batches
        time.sleep(0.5)
    
    return batch_rows

def parallel_scrape_resolutions(links, year, num_workers=2, batch_size=15):
    """Process resolutions in parallel using multiple browser instances."""
    if not links:
        return []
    
    all_rows = []
    
    # Split links into chunks for each worker
    chunks = [links[i::num_workers] for i in range(num_workers)]
    
    def worker_task(worker_id, worker_links):
        worker_rows = []
        worker_driver = get_driver()
        
        try:
            worker_rows = batch_scrape_resolutions(worker_links, worker_driver, year, batch_size)
            logging.info(f"Worker {worker_id} completed processing {len(worker_links)} links, found {len(worker_rows)} records")
        except Exception as e:
            logging.error(f"Worker {worker_id} error: {e}")
        finally:
            worker_driver.quit()
        
        return worker_rows
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_task, i, chunk) for i, chunk in enumerate(chunks)]
        for future in futures:
            worker_results = future.result()
            all_rows.extend(worker_results)
    
    return all_rows

def extract_vote_data_from_html(html_content):
    """Extract vote data from HTML using JSON-LD or scraping metadata rows."""
    soup = BeautifulSoup(html_content, "html.parser")
    data = {}
    try:
        script_tag = soup.find('script', {'type': 'application/ld+json', 'id': 'detailed-schema-org'})
        if script_tag and script_tag.string:
            json_data = json.loads(script_tag.string)
            data['Title'] = json_data.get('name', '')
            data['Date'] = json_data.get('datePublished', '')
    except Exception:
        pass
        
    for row in soup.find_all('div', class_='metadata-row'):
        try:
            title_elem = row.find('span', class_='title')
            value_elem = row.find('span', class_='value')
            if title_elem and value_elem:
                title_text = title_elem.text.strip()
                if title_text == 'Vote':
                    value = value_elem.get_text('\n').strip()
                    vote_data = {}
                    for line in value.split('\n'):
                        line = line.strip()
                        if line:
                            parts = re.match(r'^\s*([YNA])\s+(.+)$', line)
                            if parts:
                                vote_data[parts.group(2).strip().upper()] = parts.group(1).upper()
                    data['Vote Data'] = vote_data
                else:
                    data[title_text] = value_elem.text.strip()
        except Exception:
            continue
    return data

def determine_council(title):
    """Determine the council type based on the resolution title."""
    if not title:
        return "Unknown"
    lower_title = title.lower()
    if "security council" in lower_title or "S/RES/" in title:
        return "Security Council"
    if "general assembly" in lower_title or "A/RES/" in title:
        return "General Assembly"
    return "Unknown"

def save_to_csv(rows, append=False):
    """Save collected data rows to single master CSV with deduplication."""
    if not rows:
        logging.info("No data to save.")
        return False
    
    try:
        output_file = CSV_FILE
        
        # Fast deduplication using dictionaries
        existing_records = {}
        if append and os.path.exists(output_file):
            try:
                # Use pandas for efficient CSV loading
                df = pd.read_csv(output_file)
                if 'token' in df.columns and 'Link' in df.columns:
                    # Create a composite key for each record
                    for _, row in df.iterrows():
                        key = f"{row['token']}_{row['Link']}"
                        existing_records[key] = True
                elif 'Link' in df.columns:
                    for _, row in df.iterrows():
                        existing_records[row['Link']] = True
            except Exception as e:
                logging.warning(f"Error reading existing CSV: {e}")
        
        # Filter out duplicates efficiently
        unique_rows = []
        duplicates = 0
        
        for row in rows:
            # Create a unique key
            key = f"{row.get('token')}_{row.get('Link')}" if row.get('token') else row.get('Link')
            
            if key not in existing_records:
                unique_rows.append(row)
                existing_records[key] = True
            else:
                duplicates += 1
        
        if duplicates > 0:
            logging.info(f"Filtered out {duplicates} duplicate records")
        
        if not unique_rows:
            logging.info("No unique records to save")
            return True
        
        # Prepare column list
        all_cols = set(FIXED_COLUMNS)
        for row in unique_rows:
            all_cols.update(row.keys())
        extra_cols = sorted(list(all_cols - set(FIXED_COLUMNS)))
        column_order = FIXED_COLUMNS + extra_cols
        
        # Efficient CSV writing
        mode = "a" if append and os.path.exists(output_file) else "w"
        with open(output_file, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=column_order)
            if mode == "w":  # Only write header for new files
                writer.writeheader()
            writer.writerows(unique_rows)
        
        # Print progress update
        if os.path.exists(output_file):
            try:
                # Use faster method to count lines
                with open(output_file, 'r', encoding='utf-8') as f:
                    total_rows = sum(1 for _ in f) - 1  # Subtract header row
                logging.info(f"Saved {len(unique_rows)} new records. Total in {output_file}: {total_rows}")
            except:
                logging.info(f"Saved {len(unique_rows)} new records")
            
        return True
    except Exception as e:
        logging.error(f"Error saving CSV: {e}")
        return False

def clear_filters(driver):
    """Clear all filters to reset the search."""
    try:
        # Try to find a clear button first
        clear_buttons = driver.find_elements(
            By.XPATH, "//a[contains(text(), 'Clear') or contains(@class, 'clear') or contains(@onclick, 'clear')]"
        )
        
        if clear_buttons:
            for button in clear_buttons:
                try:
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(0.5)
                    return True
                except Exception:
                    continue
        
        # If no clear button worked, reload the base page
        driver.get(BASE_SEARCH_URL)
        time.sleep(1)
        
        # Wait for the page to load with standard results
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/record/')]"))
        )
        
        return True
    except Exception as e:
        logging.error(f"Error clearing filters: {e}")
        
        # Try one more time with a direct page load
        try:
            driver.get(BASE_SEARCH_URL)
            time.sleep(1)
            return True
        except:
            pass
            
        return False

def refresh_browser_session(driver):
    """Refresh the browser session - optimized for speed."""
    try:
        logging.info("Refreshing browser session...")
        driver.quit()
        time.sleep(1)  # Shorter cooldown
        
        # Create a new driver
        new_driver = get_driver()
        
        # Load the base page
        new_driver.get(BASE_SEARCH_URL)
        time.sleep(1)
        
        WebDriverWait(new_driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//ul[contains(@class, 'option-fct')]"))
        )
        
        logging.info("Browser session refreshed")
        return new_driver
    except Exception as e:
        logging.error(f"Error refreshing browser session: {e}")
        try:
            return get_driver()
        except:
            raise

def main():
    """Main function with parallel processing for maximum speed."""
    logging.info(f"Starting UN Resolution Vote Scraper - writing all data to {CSV_FILE}")
    driver = get_driver()
    
    # Session management
    session_request_count = 0
    SESSION_RESET_THRESHOLD = 150  # Increased for fewer resets
    
    try:
        prevent_sleep()
        logging.info(f"Loading base search page...")
        driver.get(BASE_SEARCH_URL)
        time.sleep(1)
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//ul[contains(@class, 'option-fct')]"))
        )
        
        years_data = get_available_years(driver)
        if not years_data:
            logging.error("No year facets found; exiting.")
            return
        
        logging.info(f"Found {len(years_data)} years to process")

        processed_years_file = "processed_years.txt"
        processed_years = set()
        
        if os.path.exists(processed_years_file):
            with open(processed_years_file, "r") as f:
                processed_years = set(line.strip() for line in f)
            logging.info(f"Previously processed years: {', '.join(processed_years)}")
        
        # Create a fresh CSV file if none exists yet
        if not os.path.exists(CSV_FILE) and not processed_years:
            # Initialize an empty file with just headers
            empty_rows = []
            save_to_csv(empty_rows, append=False)
            logging.info(f"Created new master CSV file: {CSV_FILE}")
        
        # Process each year independently
        for year_data in years_data:
            year = year_data['year']
            if year in processed_years:
                logging.info(f"Skipping already processed year: {year}")
                continue
                
            logging.info(f"\n{'='*60}\nProcessing year {year} ({year_data['count']} records)\n{'='*60}")
            
            # Reset browser session between years (but not too often)
            if session_request_count > SESSION_RESET_THRESHOLD:
                driver = refresh_browser_session(driver)
                session_request_count = 0
            
            # Select the year facet
            if not select_year_facet(driver, year_data, max_retries=2):
                logging.error(f"Failed to select facet for {year}; skipping to next year.")
                continue
            
            # Collect links for this year
            session_request_count += 1
            year_links = collect_links_for_year(driver, year)
            
            if not year_links:
                logging.warning(f"No links found for year {year}.")
                continue
                
            logging.info(f"Collected {len(year_links)} links for year {year}")
            
            # Process resolutions in batches with parallel processing
            BATCH_SIZE = 40  # Increased batch size for very fast processing
            
            if len(year_links) > 50 and MAX_WORKERS > 1:
                # For large link sets, use parallel processing
                logging.info(f"Using parallel processing with {MAX_WORKERS} workers")
                batch_rows = parallel_scrape_resolutions(year_links, year, num_workers=MAX_WORKERS, batch_size=BATCH_SIZE)
                
                # Save all rows from parallel processing
                if batch_rows:
                    save_to_csv(batch_rows, append=True)
            else:
                # For smaller link sets, process in the main thread in batches
                for i in range(0, len(year_links), BATCH_SIZE):
                    prevent_sleep()
                    session_request_count += 1
                    
                    batch_links = year_links[i:i+BATCH_SIZE]
                    logging.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(year_links) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch_links)} links)")
                    
                    # Check if we need to reset the session
                    if session_request_count >= SESSION_RESET_THRESHOLD:
                        logging.info(f"Session reset threshold reached ({SESSION_RESET_THRESHOLD} requests)")
                        driver = refresh_browser_session(driver)
                        session_request_count = 0
                    
                    # Process this batch and get rows directly
                    batch_rows = batch_scrape_resolutions(batch_links, driver, year, batch_size=15)
                    
                    # Save directly to the master CSV
                    if batch_rows:
                        save_to_csv(batch_rows, append=True)
            
            # Mark year as processed
            with open(processed_years_file, "a") as f:
                f.write(f"{year}\n")
            processed_years.add(year)
            
            # Small break between years
            time.sleep(1)
            
        logging.info("Data collection complete!")
        
        # Print final statistics
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                total_records = len(df)
                year_counts = df['Scrape_Year'].value_counts().to_dict()
                
                logging.info(f"Final statistics:")
                logging.info(f"Total records collected: {total_records}")
                logging.info(f"Records by year: {year_counts}")
                
                # Count distinct councils
                if 'Council' in df.columns:
                    council_counts = df['Council'].value_counts().to_dict()
                    logging.info(f"Records by council: {council_counts}")
            except Exception as e:
                logging.error(f"Error generating statistics: {e}")
                
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        try:
            driver.quit()
        except:
            pass
        logging.info("Scraper finished.")

if __name__ == "+_main__":
    main()