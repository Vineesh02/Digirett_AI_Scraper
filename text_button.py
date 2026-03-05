"""
URL COLLECTION + DUPLICATE CHECKER
====================================
Collects ALL URLs from ALL sections of a given root/branch/leaf,
then exports to CSV so you can inspect duplicates manually with pandas or Excel.

Usage:
    python -m experiments.check_duplicates

Output:
    logs/url_collection_YYYYMMDD_HHMMSS.csv   <- all URLs with section info
    logs/url_duplicates_YYYYMMDD_HHMMSS.csv   <- only the duplicate URLs
"""

import logging
import time
import csv
from pathlib import Path
from datetime import datetime
from collections import Counter

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import config
from scraper import LovdataScraper, _slugify

# =============================================================================
# CONFIGURATION — change these to match what you want to check
# =============================================================================

# The exact root name (must match the tree label on Lovdata)
TARGET_ROOT_NAME   = "Anskaffelser, avtaler, bygg og entrepriser"

# Branch name — set to "" to stay at root level
TARGET_BRANCH_NAME = "Anskaffelser"

# Leaf name — set to "" to stay at branch level
TARGET_LEAF_NAME   = ""

# =============================================================================

LOGS_DIR = Path(config.LOGS_DIR)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
all_urls_csv = LOGS_DIR / f"url_collection_{timestamp}.csv"
dupes_csv    = LOGS_DIR / f"url_duplicates_{timestamp}.csv"
log_file     = LOGS_DIR / f"check_duplicates_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Driver
# -----------------------------------------------------------------------------

def build_driver() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    if config.HEADLESS:
        chrome_options.add_argument("--headless=new")
    return webdriver.Chrome(options=chrome_options)


# -----------------------------------------------------------------------------
# Wait helpers
# -----------------------------------------------------------------------------

def _wait_for_legal_area(driver, timeout=20) -> bool:
    """Wait until the legal-area-header AND at least one section tab are visible."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.legal-area-header")
            )
        )
        # Also wait for section tab links to appear inside the header
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.legal-area-header a.gwt-Anchor")
            )
        )
        time.sleep(1.0)   # let GWT finish rendering tab labels
        return True
    except TimeoutException:
        return False


# -----------------------------------------------------------------------------
# Navigate to the target legal area page
# -----------------------------------------------------------------------------

def navigate_to_area(driver, scraper: LovdataScraper) -> bool:
    """
    Navigate tree: Root -> Branch (-> Leaf if set).
    Returns True when the legal-area page with section tabs is loaded.
    """
    # Step 1: go to legal areas tree
    scraper.go_to_legal_areas()
    roots = scraper.discover_legal_area_links()

    # Step 2: find the root node
    root_el   = None
    root_slug = None
    for slug, info in roots.items():
        if TARGET_ROOT_NAME.strip().lower() == info["text"].strip().lower():
            root_el   = info["element"]
            root_slug = slug
            break
        # fallback: partial match
        if TARGET_ROOT_NAME.lower() in info["text"].lower():
            root_el   = info["element"]
            root_slug = slug

    if not root_el:
        logger.error(
            "Root '%s' not found. Available roots:\n%s",
            TARGET_ROOT_NAME,
            "\n".join(f"  '{info['text']}'" for info in roots.values()),
        )
        return False

    logger.info("Clicking root: '%s'", roots[root_slug]["text"])
    scraper._click_node(root_el)
    time.sleep(1.5)
    scraper._expand_node(root_el)
    time.sleep(1.0)

    # If no branch needed, wait for legal area to load and return
    if not TARGET_BRANCH_NAME:
        logger.info("No branch specified — checking root level page")
        if _wait_for_legal_area(driver):
            logger.info("Root level legal area loaded OK")
            return True
        logger.error("Legal area header not found at root level")
        return False

    # Step 3: find and click the branch
    branches = scraper._get_children(root_el)
    logger.info("Branches found: %s", len(branches))
    for b in branches:
        logger.info("  branch: '%s'", b.text or "")

    branch_el = None
    for b in branches:
        if TARGET_BRANCH_NAME.strip().lower() in (b.text or "").strip().lower():
            branch_el = b
            break

    if not branch_el:
        logger.error(
            "Branch '%s' not found. Available:\n%s",
            TARGET_BRANCH_NAME,
            "\n".join(f"  '{(b.text or '')}'" for b in branches),
        )
        return False

    logger.info("Clicking branch: '%s'", branch_el.text)
    scraper._click_node(branch_el)
    time.sleep(2.0)                    # give GWT time to render the page
    scraper._expand_node(branch_el)
    time.sleep(1.0)

    # If no leaf needed, wait for legal area and return
    if not TARGET_LEAF_NAME:
        logger.info("No leaf specified — waiting for branch level page")
        if _wait_for_legal_area(driver):
            logger.info("Branch level legal area loaded OK")
            # Log what section tabs are visible for debugging
            try:
                header = driver.find_element(
                    By.CSS_SELECTOR, "div.legal-area-header"
                )
                tabs = header.find_elements(By.CSS_SELECTOR, "a.gwt-Anchor")
                logger.info("Section tabs visible (%s):", len(tabs))
                for t in tabs:
                    logger.info("  tab: '%s'", (t.text or "").strip())
            except Exception:
                pass
            return True
        logger.error("Legal area header not found at branch level")
        return False

    # Step 4: find and click the leaf
    leaves = scraper._get_children(branch_el)
    logger.info("Leaves found: %s", len(leaves))

    leaf_el = None
    for l in leaves:
        if TARGET_LEAF_NAME.strip().lower() in (l.text or "").strip().lower():
            leaf_el = l
            break

    if not leaf_el:
        logger.error(
            "Leaf '%s' not found. Available:\n%s",
            TARGET_LEAF_NAME,
            "\n".join(f"  '{(l.text or '')}'" for l in leaves),
        )
        return False

    logger.info("Clicking leaf: '%s'", leaf_el.text)
    scraper._click_node(leaf_el)
    time.sleep(2.0)

    if _wait_for_legal_area(driver):
        logger.info("Leaf level legal area loaded OK")
        return True

    logger.error("Legal area header not found at leaf level")
    return False


# -----------------------------------------------------------------------------
# Analyse and export
# -----------------------------------------------------------------------------

def analyse_and_export(sections: list):
    """
    Build a flat DataFrame from raw sections, show duplicate stats, save CSVs.
    """
    # Build flat rows — one row per URL per section (raw, before dedup)
    rows = []
    for sec in sections:
        for url in sec["urls"]:
            rows.append({
                "section":        sec["document_type"],
                "expected_count": sec["expected_count"],
                "url":            url,
            })

    df_all = pd.DataFrame(rows, columns=["section", "expected_count", "url"])

    total_raw    = len(df_all)
    total_unique = df_all["url"].nunique()
    total_dupes  = total_raw - total_unique

    # ── Section summary ───────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("SECTION SUMMARY")
    logger.info("=" * 70)
    for sec in sections:
        logger.info(
            "  %-55s  expected=%-6s  found=%s",
            sec["document_type"],
            sec["expected_count"] if sec["expected_count"] is not None else "?",
            len(sec["urls"]),
        )

    # ── Duplicate summary ─────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("DUPLICATE ANALYSIS")
    logger.info("=" * 70)
    logger.info("  Total raw URLs (all sections combined) : %s", total_raw)
    logger.info("  Unique URLs                            : %s", total_unique)
    logger.info("  Duplicate occurrences removed          : %s", total_dupes)
    logger.info("")

    # Which URLs appear in more than one section
    url_section_map = (
        df_all.groupby("url")["section"]
        .apply(list)
        .reset_index()
    )
    url_section_map.columns          = ["url", "sections_list"]
    url_section_map["n_sections"]     = url_section_map["sections_list"].apply(len)
    url_section_map["sections_str"]   = url_section_map["sections_list"].apply(
        lambda x: " | ".join(x)
    )

    dupes_df = url_section_map[url_section_map["n_sections"] > 1].sort_values(
        "n_sections", ascending=False
    )

    logger.info("  URLs appearing in 2+ sections : %s", len(dupes_df))
    logger.info("")

    if not dupes_df.empty:
        logger.info("  Top 10 most duplicated URLs:")
        for _, row in dupes_df.head(10).iterrows():
            logger.info(
                "    x%s  %s\n         sections: %s",
                row["n_sections"],
                row["url"],
                row["sections_str"],
            )

    # ── Section overlap matrix ────────────────────────────────────────────────
    logger.info("")
    logger.info("  Section overlap (which sections share URLs):")
    section_names    = df_all["section"].unique().tolist()
    section_url_sets = {
        sec: set(df_all[df_all["section"] == sec]["url"])
        for sec in section_names
    }
    any_overlap = False
    for i, s1 in enumerate(section_names):
        for s2 in section_names[i + 1:]:
            overlap = len(section_url_sets[s1] & section_url_sets[s2])
            if overlap > 0:
                any_overlap = True
                logger.info(
                    "    %-40s  x  %-40s  =  %s shared",
                    s1[:40], s2[:40], overlap
                )
    if not any_overlap:
        logger.info("    No overlap between sections — all URLs are unique per section")

    # ── Save CSVs ─────────────────────────────────────────────────────────────
    df_all.to_csv(all_urls_csv, index=False, encoding="utf-8")
    logger.info("")
    logger.info("  Saved all URLs to   : %s  (%s rows)", all_urls_csv, len(df_all))

    dupes_export = dupes_df[["url", "n_sections", "sections_str"]]
    dupes_export.to_csv(dupes_csv, index=False, encoding="utf-8")
    logger.info("  Saved duplicates to : %s  (%s rows)", dupes_csv, len(dupes_export))

    # ── Pandas snippet for manual work ───────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("MANUAL PANDAS ANALYSIS — paste into a notebook:")
    logger.info("=" * 70)
    snippet = f"""
import pandas as pd

df = pd.read_csv(r'{all_urls_csv}')

# All URLs per section
print(df.groupby('section')['url'].count())

# Find URLs that appear in more than one section
dupes = df[df.duplicated('url', keep=False)].sort_values('url')
print(f"Duplicate rows: {{len(dupes)}}")
print(dupes.head(20))

# How many times does each URL appear across sections?
print(df['url'].value_counts().head(20))

# How many URLs appear in exactly N sections?
print(df.groupby('url')['section'].nunique().value_counts().sort_index())
"""
    for line in snippet.strip().splitlines():
        logger.info("  %s", line)

    return df_all, dupes_df


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    logger.info("=" * 70)
    logger.info("URL COLLECTION + DUPLICATE CHECKER")
    logger.info("  Root   : %s", TARGET_ROOT_NAME)
    logger.info("  Branch : %s", TARGET_BRANCH_NAME or "(root level)")
    logger.info("  Leaf   : %s", TARGET_LEAF_NAME   or "(branch level)")
    logger.info("=" * 70)

    driver  = build_driver()
    scraper = LovdataScraper(driver)

    try:
        # Login
        if not scraper.login():
            logger.error("Login failed — aborting")
            return

        # Navigate to target area
        if not navigate_to_area(driver, scraper):
            logger.error("Navigation failed — aborting")
            return

        # Collect ALL raw URLs from ALL sections (no dedup)
        logger.info("")
        logger.info("Collecting URLs from all sections (this may take a while)...")
        sections = scraper.collect_urls_from_current_view(max_pages=500)

        if not sections:
            logger.error("No sections found — aborting")
            return

        # Analyse and export
        df_all, dupes_df = analyse_and_export(sections)

        logger.info("")
        logger.info("Done!")
        logger.info("  Open '%s' in Excel/pandas to inspect all URLs", all_urls_csv)
        logger.info("  Open '%s' to see only duplicate URLs", dupes_csv)

        if not config.HEADLESS:
            time.sleep(3)

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()