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

# Configure logging
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
CSV_FILE = "data/UN_VOTING_DATA_RAW.csv"  # Master CSV file
MAX_PAGES_PER_YEAR = 50  # Maximum pages to paginate per year

# Parallelism settings
MAX_WORKERS = 2

# Fixed columns for CSV output
FIXED_COLUMNS = [
    "Council", "Date", "Title", "Resolution", "TOTAL VOTES", "NO-VOTE COUNT",
    "ABSENT COUNT", "NO COUNT", "YES COUNT", "Link", "token", "Scrape_Year"
]

# Anti-blocking measures
# User agents list
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.100 Safari/537.36"
]

# Global counter to rotate user agents
user_agent_index = 0

def reset_user_agent_rotation():
    """Reset user agent rotation to start from the first user agent."""
    global user_agent_index
    user_agent_index = 0

# ---------------------- Utility & Browser Functions ------------------------

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
    """Configure and return a Chrome webdriver instance with alternating user agent."""
    global user_agent_index

    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Rotate user agent each time a new driver is created
    user_agent = USER_AGENTS[user_agent_index]
    user_agent_index = (user_agent_index + 1) % len(USER_AGENTS)  # Rotate to next

    options.add_argument(f"user-agent={user_agent}")

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--js-flags=--expose-gc")
    options.add_argument("--aggressive-cache-discard")
    options.add_argument("--disable-site-isolation-trials")

    driver_path = ChromeDriverManager().install()

    try:
        os.chmod(driver_path, 0o755)
    except Exception as e:
        logging.warning(f"Could not set permissions for {driver_path}: {e}")

    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(45)

    # Hide automation flags
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    logging.info(f"Initialized browser with user-agent: {user_agent}")
    return driver


def check_for_next_button(driver):
    """Find the 'Next' page button."""
    try:
        next_button = driver.find_element(By.XPATH, "//a[img[@alt='next']]")
        return next_button
    except NoSuchElementException:
        return None

# ---------------------- CSV Links Handling (Regex-Based) ------------------------

def normalize_link(href):
    """
    Normalize the URL by extracting the record ID if possible,
    returning a standardized URL.
    """
    if not href:
        return None
    if '/record/' in href:
        try:
            record_part = href.split('/record/')[1]
            record_id = record_part.split('?')[0].split('/')[0].strip()
            if record_id.isdigit():
                return f"https://digitallibrary.un.org/record/{record_id}"
        except (IndexError, ValueError):
            pass
    base_url = href.split('?')[0]
    if '?' in href:
        params = href.split('?')[1].split('&')
        ln_param = [p for p in params if p.startswith('ln=')]
        if ln_param:
            base_url = f"{base_url}?{ln_param[0]}"
    return base_url

def get_links_from_csv_regex(csv_file):
    """
    Open the CSV file line by line and extract all UN digital library links
    using a regex. Return a list of unique normalized links.
    """
    links = set()
    # Regex pattern for a UN record URL: https://digitallibrary.un.org/record/<digits>
    pattern = re.compile(r'https://digitallibrary\.un\.org/record/\d+')
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            for line in f:
                found = pattern.findall(line)
                for link in found:
                    norm = normalize_link(link)
                    if norm:
                        links.add(norm)
        logging.info(f"Regex extracted {len(links)} unique links from CSV")
    except Exception as e:
        logging.error(f"Error reading CSV with regex: {e}")
    return list(links)

# ---------------------- Year-Based Scraping Functions ------------------------

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

def retry_failed_links(failed_links, year):
    """Retry processing failed links with a fresh browser session."""
    if not failed_links:
        return []

    logging.info(f"Retrying {len(failed_links)} failed links for year {year}...")

    retry_driver = get_driver()  # New session with rotated user-agent
    retried_rows = []

    try:
        for link in failed_links:
            try:
                row_data = process_resolution(link, retry_driver, year)
                if row_data:
                    retried_rows.append(row_data)
                time.sleep(0.2)
            except Exception as e:
                logging.error(f"Retry failed for link {link}: {e}")
    finally:
        retry_driver.quit()

    logging.info(f"Retried {len(failed_links)} links, successfully recovered {len(retried_rows)} records.")
    return retried_rows


def select_year_facet(driver, year_data, max_retries=10):
    """Select a specific year by clicking its checkbox."""
    for retry in range(max_retries):
        try:
            logging.info(f"Selecting year: {year_data['year']} (Attempt {retry+1}/{max_retries})")
            checkbox = driver.find_element(By.ID, year_data['input_id'])
            driver.execute_script("arguments[0].scrollIntoView();", checkbox)
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", checkbox)
            time.sleep(1)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/record/')]"))
            )
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
    try:
        logging.warning(f"Trying fallback for year {year_data['year']}...")
        driver.get(BASE_SEARCH_URL)
        time.sleep(1.5)
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

def collect_links_for_year(driver, year, existing_links):
    """
    Collect links for the given year. Normalize each link and skip it if it's already in existing_links.
    """
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
            new_links = set()
            elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/record/')]")
            for elem in elements:
                try:
                    href = elem.get_attribute("href")
                    norm_link = normalize_link(href)
                    if norm_link:
                        if norm_link in existing_links:
                            continue  # Skip already scraped links
                        new_links.add(norm_link)
                except StaleElementReferenceException:
                    continue
            all_links.update(new_links)
        except Exception as e:
            logging.error(f"[Year {year}] Error collecting links: {e}")

        new_count = len(all_links) - links_before
        logging.info(f"[Year {year}] Found {new_count} new links on page {page_count} (Total new: {len(all_links)})")
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
                time.sleep(1)
            except Exception as e:
                logging.error(f"[Year {year}] Error clicking next button: {e}")
                break
        else:
            logging.info(f"[Year {year}] No next button found; reached last page.")
            break

    # Update the in-memory set with links from this year
    existing_links.update(all_links)
    logging.info(f"[Year {year}] Collected {len(all_links)} unique new links")
    return list(all_links)

# ---------------------- Resolution Processing Functions ------------------------

def extract_vote_data_from_html(html_content):
    """Extract vote data from HTML using JSON-LD or by scraping metadata rows."""
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

def process_resolution(link, driver, year):
    """Process a single resolution page and return row data."""
    try:
        record_id = link.split('/record/')[1].split('?')[0] if '/record/' in link else link.split('/')[-1]
        short_link = f".../record/{record_id}"
        logging.info(f"Loading: {short_link}")
        driver.get(link)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'metadata-row')]"))
        )
        row_data = {
            "Link": link,
            "token": record_id,
            "Scrape_Year": year
        }
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
        if row_data.get('Title') or row_data.get('Resolution'):
            return row_data
        return None
    except Exception as e:
        logging.error(f"Error processing link {link}: {e}")
        return None

def batch_scrape_resolutions(links, driver, year, batch_size=15):
    """Scrape resolution pages in batches and return rows ready for CSV, tracking failures."""
    batch_rows = []
    failed_links = []  # Collect failed links here

    total_links = len(links)
    for i in range(0, total_links, batch_size):
        batch = links[i:i+batch_size]
        logging.info(f"Processing batch {i//batch_size + 1}/{(total_links + batch_size - 1)//batch_size} ({len(batch)} links)")

        for link in batch:
            try:
                row_data = process_resolution(link, driver, year)
                if row_data:
                    batch_rows.append(row_data)
                else:
                    # If no data was returned but no exception was thrown, still count as a failure
                    logging.warning(f"No data returned for link, added to failed links for retry: {link}")
                    failed_links.append(link)
                time.sleep(0.2)
            except Exception as e:
                logging.error(f"Error processing link {link}: {e}", exc_info=False)
                failed_links.append(link)  # Capture failed link
                logging.info(f"Added to failed links: {link}")

        time.sleep(0.5)
    
    logging.info(f"Batch scraping completed: {len(batch_rows)} successful, {len(failed_links)} failed")
    return batch_rows, failed_links


def parallel_scrape_resolutions(links, year, num_workers=2, batch_size=15):
    """Process resolution pages in parallel using multiple browser instances."""
    if not links:
        return [], []  # Return empty lists for both rows and failed links
    
    all_rows = []
    all_failed_links = []  # Add collection for failed links
    
    chunks = [links[i::num_workers] for i in range(num_workers)]
    
    def worker_task(worker_id, worker_links):
        worker_driver = get_driver()
        try:
            # Properly unpack both return values from batch_scrape_resolutions
            worker_rows, worker_failed_links = batch_scrape_resolutions(worker_links, worker_driver, year, batch_size)
            logging.info(f"Worker {worker_id} processed {len(worker_links)} links, found {len(worker_rows)} records, failed: {len(worker_failed_links)}")
            return worker_rows, worker_failed_links  # Return both as a tuple
        except Exception as e:
            logging.error(f"Worker {worker_id} error: {e}")
            # If an exception occurs, consider all links as failed
            return [], worker_links
        finally:
            worker_driver.quit()
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_task, i, chunk) for i, chunk in enumerate(chunks)]
        for future in futures:
            # Unpack both values from the worker's result
            worker_rows, worker_failed_links = future.result()
            all_rows.extend(worker_rows)
            all_failed_links.extend(worker_failed_links)
    
    logging.info(f"Parallel processing complete: {len(all_rows)} records found, {len(all_failed_links)} links failed")
    
    # Return both collections as a tuple
    return all_rows, all_failed_links

def get_all_columns_from_csv(filepath):
    """
    Extract all columns from an existing CSV, ensuring all are preserved.
    This is used to preserve any historical country columns already in the file.
    """
    if not os.path.exists(filepath):
        return []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, [])
            extra_columns = [col for col in header if col not in FIXED_COLUMNS]
            return extra_columns
    except Exception as e:
        logging.error(f"Error reading CSV headers: {e}")
        return []
        


def save_to_csv(new_rows):
    """
    Save collected resolution data to the master CSV file.
    Updates the header to include new country columns if they appear.
    Rewrites the CSV file completely so that every row has the same columns.
    """
    output_file = CSV_FILE

    # Load existing data if the file exists.
    existing_rows = []
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)
                existing_header = reader.fieldnames
        except Exception as e:
            logging.warning(f"Error reading existing CSV: {e}")
            existing_header = FIXED_COLUMNS
    else:
        existing_header = FIXED_COLUMNS

    # Combine existing rows with new rows.
    combined_rows = existing_rows + new_rows

    # Determine the full set of columns.
    # Start with fixed columns, add any extra columns found in the rows.
    all_columns = set(FIXED_COLUMNS)
    for row in combined_rows:
        all_columns.update(row.keys())

    # Order columns: fixed columns first, then any extra (e.g., country columns) sorted alphabetically.
    country_columns = sorted([col for col in all_columns if col not in FIXED_COLUMNS])
    ordered_columns = FIXED_COLUMNS + country_columns

    # Ensure every row has all the columns.
    for row in combined_rows:
        for col in ordered_columns:
            if col not in row:
                row[col] = ""

    # Write out the entire CSV file with the updated header.
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_columns)
            writer.writeheader()
            writer.writerows(combined_rows)
        logging.info(f"Saved {len(new_rows)} new records (total now {len(combined_rows)}) to {output_file}")
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")
        return False

    return True



def clear_filters(driver):
    """Clear all filters to reset the search."""
    try:
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
        driver.get(BASE_SEARCH_URL)
        time.sleep(1)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/record/')]"))
        )
        return True
    except Exception as e:
        logging.error(f"Error clearing filters: {e}")
        try:
            driver.get(BASE_SEARCH_URL)
            time.sleep(1)
            return True
        except:
            pass
        return False

def refresh_browser_session(driver):
    """Refresh the browser session."""
    try:
        logging.info("Refreshing browser session...")
        driver.quit()
        time.sleep(1)
        new_driver = get_driver()
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

def clean_existing_csv():
    """
    Clean the existing CSV file by removing duplicate entries.
    Create a backup before writing the cleaned file.
    """
    if not os.path.exists(CSV_FILE):
        logging.info(f"No CSV file {CSV_FILE} exists yet. Nothing to clean.")
        return False
    try:
        logging.info(f"Cleaning CSV file {CSV_FILE} to remove duplicates...")
        df = pd.read_csv(CSV_FILE, encoding='utf-8', dtype=str, on_bad_lines='skip')
        initial_rows = len(df)
        if initial_rows == 0:
            logging.info("CSV file is empty. Nothing to clean.")
            return False
        logging.info(f"CSV loaded with {initial_rows} rows")
        if 'token' in df.columns and 'Link' in df.columns:
            df['dedup_key'] = df.apply(lambda row: f"{row.get('token', '').strip()}_{row.get('Link', '').strip()}", axis=1)
            df = df.drop_duplicates(subset=['dedup_key'], keep='first')
            df = df.drop(columns=['dedup_key'])
        elif 'token' in df.columns:
            df = df.drop_duplicates(subset=['token'], keep='first')
        elif 'Link' in df.columns:
            df = df.drop_duplicates(subset=['Link'], keep='first')
        final_rows = len(df)
        duplicates_removed = initial_rows - final_rows
        if duplicates_removed > 0:
            backup_file = f"{CSV_FILE}.backup"
            os.rename(CSV_FILE, backup_file)
            df.to_csv(CSV_FILE, index=False)
            logging.info(f"Cleaned CSV file: Removed {duplicates_removed} duplicates. Original backed up to {backup_file}")
            return True
        else:
            logging.info("No duplicates found in CSV file.")
            return False
    except Exception as e:
        logging.error(f"Error cleaning CSV: {e}")
        return False

# ---------------------- Main Function ------------------------

def main():
    """Main function with improved deduplication and link comparison."""
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)

    reset_user_agent_rotation()
    
    logging.info(f"Starting UN Resolution Vote Scraper - writing all data to {CSV_FILE}")
    
    if os.path.exists(CSV_FILE):
        clean_existing_csv()
    else:
        save_to_csv([])
        logging.info(f"Created new master CSV file: {CSV_FILE}")
    
    # Load all links from CSV using regex into an in-memory set (no file writing)
    csv_links = set(get_links_from_csv_regex(CSV_FILE))
    logging.info(f"Loaded {len(csv_links)} unique links from CSV for deduplication")
    
    driver = get_driver()
    session_request_count = 0
    SESSION_RESET_THRESHOLD = 150

    try:
        prevent_sleep()
        logging.info("Loading base search page...")
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
        
        for year_data in years_data:
            year = year_data['year']
            logging.info(f"\n{'='*60}\nProcessing year {year} ({year_data['count']} records)\n{'='*60}")
            
            if session_request_count > SESSION_RESET_THRESHOLD:
                driver = refresh_browser_session(driver)
                session_request_count = 0
            
            if not select_year_facet(driver, year_data, max_retries=10):
                logging.error(f"Failed to select facet for {year}; skipping to next year.")
                continue
            
            session_request_count += 1
            
            # Collect links for the year; check against the in-memory csv_links set
            year_links = collect_links_for_year(driver, year, csv_links)
            if not year_links:
                logging.warning(f"No new links found for year {year}.")
                continue
            
            logging.info(f"Collected {len(year_links)} new links for year {year}")
            
            BATCH_SIZE = 40
            if len(year_links) > 50 and MAX_WORKERS > 1:
                logging.info(f"Using parallel processing with {MAX_WORKERS} workers")
                # Fix: Use year_links instead of undefined batch_links
                batch_rows, failed_links = parallel_scrape_resolutions(year_links, year, MAX_WORKERS)

                if batch_rows:
                    save_to_csv(batch_rows)
                
                # Check for failed_links here and retry if needed
                if failed_links:
                    retry_rows = retry_failed_links(failed_links, year)
                    if retry_rows:
                        save_to_csv(retry_rows)
            else:
                for i in range(0, len(year_links), BATCH_SIZE):
                    prevent_sleep()
                    session_request_count += 1
                    batch_links = year_links[i:i+BATCH_SIZE]
                    logging.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(year_links) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch_links)} links)")
                    if session_request_count >= SESSION_RESET_THRESHOLD:
                        logging.info(f"Session reset threshold reached ({SESSION_RESET_THRESHOLD} requests)")
                        driver = refresh_browser_session(driver)
                        session_request_count = 0
                    
                    # Fix: Correctly capture both return values from batch_scrape_resolutions
                    batch_rows, failed_links = batch_scrape_resolutions(batch_links, driver, year, batch_size=15)
                    
                    if batch_rows:
                        save_to_csv(batch_rows)

                    if failed_links:
                        retry_rows = retry_failed_links(failed_links, year)
                        if retry_rows:
                            save_to_csv(retry_rows)
            
            csv_links.update(year_links)
            time.sleep(1)
            
        logging.info("Data collection complete!")
        
        if os.path.exists(CSV_FILE):
            clean_existing_csv()
            
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        logging.info("Scraper finished.")


if __name__ =="__main__":
    main()
