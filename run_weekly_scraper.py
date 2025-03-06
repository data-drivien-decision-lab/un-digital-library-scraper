import os
import time
import logging
import schedule
import pandas as pd
from datetime import datetime
import un_scraper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging for the scheduler
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCHEDULER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="scheduler.log",
    filemode="a"
)

# Add console output
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [SCHEDULER] %(message)s", datefmt="%H:%M:%S")
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def check_most_recent_year():
    """
    Check if the most recent year's data already exists in the CSV.
    Returns True if the data exists (no need to run scraper), False otherwise.
    """
    logging.info("Performing quick check for most recent year's data...")
    
    try:
        # Use the get_driver and other functions from un_scraper
        driver = un_scraper.get_driver()
        
        try:
            # Load the search page
            driver.get(un_scraper.BASE_SEARCH_URL)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//ul[contains(@class, 'option-fct')]"))
            )
            
            # Get available years
            years_data = un_scraper.get_available_years(driver)
            if not years_data:
                logging.error("No year data found")
                return False
                
            # Get most recent year
            years_data = sorted(years_data, key=lambda x: x['year'], reverse=True)
            most_recent_year = years_data[0]['year']
            logging.info(f"Most recent year available: {most_recent_year}")
            
            # Check if CSV exists and contains this year
            csv_file = un_scraper.CSV_FILE
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
                if 'Scrape_Year' in df.columns:
                    years_in_csv = df['Scrape_Year'].astype(str).unique()
                    if str(most_recent_year) in years_in_csv:
                        logging.info(f"Year {most_recent_year} already exists in dataset")
                        return True
                    else:
                        logging.info(f"Year {most_recent_year} not found in dataset, need to run scraper")
                        return False
            
            logging.info("CSV not found or no year data, need to run scraper")
            return False
            
        except Exception as e:
            logging.error(f"Error checking for most recent year: {e}")
            return False
        finally:
            # Always close the driver
            driver.quit()
            
    except Exception as e:
        logging.error(f"Error in check_most_recent_year: {e}")
        return False

def run_scraper_job():
    """Run the UN Resolution scraper as a scheduled job."""
    logging.info("Starting scheduled UN resolution scraper check")
    
    # First check if we need to run the scraper
    if check_most_recent_year():
        logging.info("Most recent year's data already exists - skipping full scraper run")
        return
    
    # If we get here, we need to run the full scraper
    start_time = datetime.now()
    logging.info(f"Starting full UN resolution scraper job at {start_time}")
    
    try:
        # Create an instance of your main function and run it
        un_scraper.main()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed scheduled job. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in scheduled scraper job: {e}", exc_info=True)

if __name__ == "__main__":
    # Show startup message
    print("\n" + "="*80)
    print("UN Resolution Weekly Scraper with Early Termination")
    print("="*80)
    print("\nThis script will run the UN Resolution scraper:")
    print("1. Immediately when started (with pre-check)")
    print("2. Every Monday at 2:00 AM thereafter (with pre-check)")
    print("\nThe script will first check if the most recent year's data")
    print("already exists, and only run the full scraper if needed.")
    print("\nKeep this window open for the scheduler to work.")
    print("Press Ctrl+C to stop the scheduler.")
    print("="*80 + "\n")
    
    # Run immediately on startup
    logging.info("Running initial scraper job...")
    run_scraper_job()
    
    # Schedule to run every Monday at 2:00 AM
    schedule.every().monday.at("02:00").do(run_scraper_job)
    logging.info("Scheduler set up. Next run scheduled for Monday at 2:00 AM")
    
    # Keep the script running to maintain the schedule
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user")
        print("\nScheduler stopped. Goodbye!")