from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
import os
import psutil
import gc

class InvestingParallelScraper:
    def __init__(self, base_url="https://www.investing.com/news/forex-news/", csv_file="investing_forex_news_batched.csv", max_tabs=3):
        self.base_url = base_url
        self.data = []
        self.csv_file = csv_file
        self.max_tabs = max_tabs
        self.driver = None
        self.failed_pages = []
        self.successful_pages = []
        self.timeout = 20

        if os.path.exists(self.csv_file):
            existing_df = pd.read_csv(self.csv_file)
            self.data = existing_df.to_dict("records")
            logging.info(f"Loaded {len(self.data)} records from existing CSV.")

    def initialize_driver(self):
        options = webdriver.EdgeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.javascript": 1,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--disable-application-cache")
        options.add_argument("--disk-cache-size=0")
        options.add_argument("--media-cache-size=0")
        options.add_argument("--aggressive-cache-discard")
        options.add_argument("--disable-cache")
        options.add_argument("--disable-offline-load-stale-cache")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-renderer-backgrounding")
        options.page_load_strategy = 'eager'
        driver = webdriver.Edge(options=options)
        driver.set_page_load_timeout(self.timeout)
        driver.set_script_timeout(self.timeout)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            """
        })
        return driver

    def check_memory(self):
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        if memory_percent > 80:
            logging.warning(f"High memory usage: {memory_percent:.1f}% - forcing cleanup")
            gc.collect()
            time.sleep(2)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            logging.info(f"After cleanup: {memory_percent:.1f}%")
        return memory_percent

    def open_tab_safe(self, page_num):
        try:
            url = f"{self.base_url}{page_num}"
            script = f"""
                var newTab = window.open('about:blank', '_blank');
                setTimeout(function() {{
                    newTab.location.href = '{url}';
                }}, 100);
            """
            self.driver.execute_script(script)
            time.sleep(0.5)
            if len(self.driver.window_handles) > 0:
                return self.driver.window_handles[-1]
            else:
                raise Exception("Failed to get window handle")
        except Exception as e:
            logging.error(f"Failed to open tab for page {page_num}: {e}")
            raise

    def wait_for_articles(self, timeout=20):
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-test='article-item']"))
            )
            return True
        except Exception:
            return False

    def scrape_tab(self, window_handle, page_num):
        try:
            if window_handle not in self.driver.window_handles:
                raise Exception("Window handle no longer exists")
            self.driver.switch_to.window(window_handle)
            if not self.wait_for_articles(timeout=self.timeout):
                raise Exception("Articles didn't load in time")
            time.sleep(random.uniform(0.5, 1))
            html = self.driver.page_source
            articles = self.parse_articles(html)
            self.driver.close()
            if len(articles) == 0:
                self.failed_pages.append({"page": page_num, "error": "No articles found"})
                return [], page_num, False
            self.successful_pages.append(page_num)
            logging.info(f"Page {page_num}: {len(articles)} articles")
            return articles, page_num, True
        except Exception as e:
            error_msg = str(e)[:100]
            self.failed_pages.append({"page": page_num, "error": error_msg})
            logging.error(f"Page {page_num} error: {error_msg}")
            try:
                if window_handle in self.driver.window_handles:
                    self.driver.switch_to.window(window_handle)
                    self.driver.close()
            except:
                pass
            return [], page_num, False

    def parse_articles(self, html):
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all("article", {"data-test": "article-item"})
        page_data = []
        for article in articles:
            a_tag = article.find("a", href=True)
            title = a_tag.text.strip() if a_tag else None
            link = a_tag["href"].strip() if a_tag else None
            if link and not link.startswith("http"):
                link = f"https://www.investing.com{link}"
            desc_tag = article.find("p", {"data-test": "article-description"})
            description = desc_tag.text.strip() if desc_tag else None
            time_tag = article.find("time", {"data-test": "article-publish-date"})
            published_at = time_tag["datetime"].strip() if time_tag and time_tag.has_attr("datetime") else None
            page_data.append({
                "title": title,
                "link": link,
                "description": description,
                "published_at": published_at
            })
        return page_data

    def scrape_batch(self, start_page, end_page):
        memory_percent = self.check_memory()
        logging.info(f"Memory: {memory_percent:.1f}%")
        self.driver = self.initialize_driver()
        self.driver.get("about:blank")
        time.sleep(2)
        batch_data = []
        page_range = list(range(start_page, end_page + 1))
        for i, page_num in enumerate(page_range, 1):
            try:
                window_handle = self.open_tab_safe(page_num)
                time.sleep(random.uniform(1, 2))
                articles, _, success = self.scrape_tab(window_handle, page_num)
                batch_data.extend(articles)
                if len(self.driver.window_handles) > 0:
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except Exception as e:
                logging.error(f"Failed page {page_num}: {e}")
                self.failed_pages.append({"page": page_num, "error": str(e)[:100]})
            if i < len(page_range):
                time.sleep(random.uniform(1, 2))
        self.data.extend(batch_data)
        self.save_to_csv()
        logging.info(f"Saved batch: {len(batch_data)} articles")
        try:
            self.driver.quit()
        except:
            pass
        self.driver = None
        gc.collect()
        time.sleep(2)

    def scrape_all(self, start=4, end=286):
        total_batches = ((end - start + 1) + self.max_tabs - 1) // self.max_tabs
        current_batch = 0
        try:
            for batch_start in range(start, end + 1, self.max_tabs):
                current_batch += 1
                batch_end = min(batch_start + self.max_tabs - 1, end)
                logging.info(f"\n{'='*60}")
                logging.info(f"BATCH {current_batch}/{total_batches}: Pages {batch_start}-{batch_end}")
                logging.info(f"{'='*60}")
                try:
                    self.scrape_batch(batch_start, batch_end)
                except Exception as e:
                    logging.error(f"Batch {current_batch} failed: {e}")
                    for page in range(batch_start, batch_end + 1):
                        if page not in self.successful_pages:
                            self.failed_pages.append({"page": page, "error": f"Batch failure: {e}"})
                if batch_end < end:
                    wait_time = random.uniform(8, 15)
                    logging.info(f"Cooling down {wait_time:.1f}s...")
                    time.sleep(wait_time)
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            self.print_summary()

    def print_summary(self):
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Successful: {len(self.successful_pages)} pages")
        print(f"Failed: {len(self.failed_pages)} pages")
        print(f"Total articles: {len(self.data)}")
        if self.failed_pages:
            print("\nFAILED PAGES:")
            failed_nums = sorted(set([f['page'] for f in self.failed_pages]))
            print(f"   {failed_nums}")
        else:
            print("\nAll pages scraped successfully!")
        print("="*60 + "\n")

    def save_to_csv(self):
        df = pd.DataFrame(self.data)
        df.to_csv(self.csv_file, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    scraper = InvestingParallelScraper(
        csv_file="investing_forex_news_headless.csv", 
        max_tabs=3  
    )
    scraper.scrape_all(start=1, end=286)
