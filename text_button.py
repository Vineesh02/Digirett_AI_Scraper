"""
Quick test — scrape 2 Lovdata URLs and write XML files.
Run from your project root:
    python test_scrape_two_urls.py

Outputs XML files to ./test_output/
"""

import os
import sys
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── adjust this if your project layout is different ──────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from scraper import LovdataScraper, _extract_doc_id
from xml_handler import XMLHandler

# ── the two URLs to test ─────────────────────────────────────────────────────
TEST_URLS = [
    "https://lovdata.no/pro/#document/NL/lov/2016-06-17-73",
    "https://lovdata.no/pro/#document/NLO/lov/1999-07-16-69",
]

OUTPUT_DIR = "./test_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def make_file_name(url: str) -> str:
    doc_id = _extract_doc_id(url)          # e.g. "NL_lov_2016-06-17-73"
    return doc_id.replace("/", "_") + ".xml"


def run_test():
    # ── start browser ────────────────────────────────────────────────────────
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    options = Options()
    # Remove headless if you want to watch the browser
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,900")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        # Try explicit chromedriver path if auto-detect fails
        service = Service("/usr/bin/chromedriver")
        driver  = webdriver.Chrome(service=service, options=options)

    scraper = LovdataScraper(driver)

    # ── login ────────────────────────────────────────────────────────────────
    logger.info("Logging in...")
    if not scraper.login():
        logger.error("Login failed — check config.LOVDATA_USERNAME / PASSWORD")
        driver.quit()
        return

    logger.info("Login OK\n")

    # ── scrape each URL ──────────────────────────────────────────────────────
    for i, url in enumerate(TEST_URLS, 1):
        logger.info("=" * 60)
        logger.info("[%s/%s] %s", i, len(TEST_URLS), url)
        logger.info("=" * 60)

        result = scraper.scrape_content_from_url(url)

        logger.info("  title        : %s", result["title"] or "(none)")
        logger.info("  date         : %s", result["date"]  or "(none)")
        logger.info("  year         : %s", result["year"])
        logger.info("  content chars: %s", len(result["content"]))
        logger.info("  source       : %s", result["content_source"])
        logger.info("  meta fields  : %s", list(result["page_meta"].keys()))
        logger.info("")
        logger.info("  META VALUES:")
        for k, v in result["page_meta"].items():
            logger.info("    %-25s = %s", k, v[:80] if v else "")
        logger.info("")

        if not result["content"]:
            logger.warning("  No content extracted — XML will be empty")

        # ── build document dict for XMLHandler ───────────────────────────────
        file_name = make_file_name(url)
        document  = {
            "file_name":      file_name,
            "url":            url,
            "document_type":  "LAWS",
            "title":          result["title"],
            "date":           result["date"],
            "year":           result["year"],
            "content":        result["content"],
            "content_source": result["content_source"],
            "page_meta":      result["page_meta"],
        }

        file_path, size, md5, preview = XMLHandler.save(document, OUTPUT_DIR)

        if file_path:
            logger.info("  XML saved  : %s", file_path)
            logger.info("  Size       : %s bytes", size)
            logger.info("  MD5        : %s", md5)
            logger.info("  Preview    : %s", (preview or "")[:120])

            # Print the full XML so you can inspect it in the terminal
            logger.info("\n--- XML CONTENT ---")
            with open(file_path, encoding="utf-8") as f:
                print(f.read())
            logger.info("--- END XML ---\n")
        else:
            logger.error("  XMLHandler.save failed")

        time.sleep(2)

    driver.quit()
    logger.info("Done. XML files in: %s", os.path.abspath(OUTPUT_DIR))


if __name__ == "__main__":
    run_test()