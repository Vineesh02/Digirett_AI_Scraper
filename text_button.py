"""
TEST: Scrape two known-problematic Lovdata URLs
================================================
URL 1: https://lovdata.no/pro/#document/INNST/forarbeid/inns-244-l-201516  (should PASS)
URL 2: https://lovdata.no/pro/#document/PROP/forarbeid/otprp-55-197576     (was FAILING)

Run:
    python test_empty_urls.py

Put this file in your project folder (same level as scraper.py, config.py).
Uses the FIXED scraper.py with /pro/document/ direct URL fallback.
"""

import sys
import time
import logging
import hashlib
import re
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import config
from scraper import LovdataScraper

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("test_empty_urls.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("test_xml_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── URLs under test ───────────────────────────────────────────────────────────
TEST_URLS = [
    {
        "url":      "https://lovdata.no/pro/#document/INNST/forarbeid/inns-244-l-201516",
        "expect":   "PASS",
        "desc":     "Innst. 244 L (2015-2016) — normal modern document",
    },
    {
        "url":      "https://lovdata.no/pro/#document/PROP/forarbeid/otprp-55-197576",
        "expect":   "PASS",
        "desc":     "Ot.prp.nr.55 (1975-1976) — old document, iframe=about:blank",
    },
]


def make_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    if getattr(config, "HEADLESS", False):
        opts.add_argument("--headless=new")
    return webdriver.Chrome(options=opts)


def save_xml(url, title, date, year, content, source, meta):
    m = re.search(r"#document/(.+)", url)
    doc_id = m.group(1).replace("/", "_") if m else "unknown"
    file_path = OUTPUT_DIR / f"{doc_id}.xml"
    meta_xml = "\n".join(
        f"    <{k}>{str(v)[:500]}</{k}>" for k, v in meta.items()
    )
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<document>
  <metadata>
    <source_url>{url}</source_url>
    <title>{title}</title>
    <date>{date}</date>
    <year>{year}</year>
    <content_source>{source}</content_source>
    <content_hash>{hashlib.md5(content.encode()).hexdigest()}</content_hash>
{meta_xml}
  </metadata>
  <content><![CDATA[{content}]]></content>
</document>
"""
    file_path.write_text(xml, encoding="utf-8")
    return str(file_path)


def run_test():
    driver = make_driver()
    scraper = LovdataScraper(driver)

    try:
        logger.info("=" * 65)
        logger.info("Logging in to Lovdata Pro...")
        if not scraper.login():
            logger.error("LOGIN FAILED — check config.py credentials")
            return
        logger.info("Login OK")

        results = []

        for i, item in enumerate(TEST_URLS, 1):
            url    = item["url"]
            expect = item["expect"]
            desc   = item["desc"]

            logger.info("")
            logger.info("=" * 65)
            logger.info("[%s/%s] %s", i, len(TEST_URLS), desc)
            logger.info("        %s", url)
            logger.info("=" * 65)

            scraped = scraper.scrape_content_from_url(url)
            content = scraped.get("content", "").strip()
            title   = scraped.get("title",  "") or ""
            date    = scraped.get("date",   "") or ""
            year    = scraped.get("year")
            source  = scraped.get("content_source", "") or ""
            meta    = scraped.get("page_meta", {})

            passed = len(content) > 0
            status = "PASS ✓" if passed else "FAIL ✗"
            match  = (passed and expect == "PASS") or (not passed and expect == "FAIL")
            verdict = "✓ expected" if match else "✗ UNEXPECTED"

            logger.info("")
            logger.info("  Result   : %s  [%s]", status, verdict)
            logger.info("  Title    : %s", (title or "(none)")[:80])
            logger.info("  Date     : %s   Year: %s", date or "(none)", year)
            logger.info("  Source   : %s", source or "(none)")
            logger.info("  Content  : %s chars", len(content))

            if content:
                logger.info("  Preview  : %s", content[:300].replace("\n", " "))
                xml_path = save_xml(url, title, date, year, content, source, meta)
                logger.info("  XML      : %s  (%s bytes)",
                    xml_path, Path(xml_path).stat().st_size)
            else:
                logger.error(
                    "  EMPTY — scraper returned no content.\n"
                    "  Check above logs for:\n"
                    "    - 'SPA fallback — trying:' lines\n"
                    "    - 'direct[body_text] N chars' lines\n"
                    "    - 'direct page_src len=N head:' to see raw HTML\n"
                    "  The iframe src=about:blank fix should have triggered."
                )
                xml_path = None

            results.append({
                "url":    url,
                "desc":   desc,
                "passed": passed,
                "match":  match,
                "chars":  len(content),
                "source": source,
                "xml":    xml_path,
            })
            time.sleep(1)

        # ── Final summary ─────────────────────────────────────────────────────
        logger.info("")
        logger.info("=" * 65)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 65)
        for r in results:
            status  = "PASS ✓" if r["passed"] else "FAIL ✗"
            verdict = "expected" if r["match"] else "UNEXPECTED"
            logger.info(
                "  %s [%s chars | %-22s | %s]",
                status, r["chars"], r["source"], r["desc"]
            )
            if r["xml"]:
                logger.info("       XML → %s", r["xml"])

        all_match = all(r["match"] for r in results)
        all_pass  = all(r["passed"] for r in results)
        logger.info("")
        if all_pass:
            logger.info("ALL PASS — fix is working correctly")
            logger.info("XMLs saved to: %s/", OUTPUT_DIR)
        else:
            failed = [r for r in results if not r["passed"]]
            logger.warning("%s / %s URLs still empty:", len(failed), len(results))
            for r in failed:
                logger.warning("  → %s", r["url"])
            logger.warning(
                "\nTo debug: look in the logs above for the SPA fallback section.\n"
                "The key log lines to check:\n"
                "  'SPA fallback — trying: https://lovdata.no/pro/document/...'\n"
                "  'direct[body_text] N chars  is_nav=False'  ← should be >1000\n"
                "  'direct page_src len=N head: ...'          ← check if page has content\n"
            )
        logger.info("=" * 65)
        logger.info("Full log saved to: test_empty_urls.log")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    run_test()