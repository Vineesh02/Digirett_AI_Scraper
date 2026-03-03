"""
LOVDATA PRO SCRAPER — MAIN
Full pipeline:
  1. Login
  2. Navigate tree: Root → Branch → Leaf
  3. For each area page:
       a. Discover section tab links (LAWS, REGULATIONS etc.)
       b. Visit each section tab → click Show All → paginate → collect URLs
  4. Visit each URL → scrape content → save as local .xml file
  5. Every 10 files → upload batch to Supabase bucket
                    → insert metadata row
                    → delete local XML file
"""

import sys
import time
import logging
import hashlib
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import config
from scraper import (
    LovdataScraper,
    _extract_doc_id,
    _extract_year_from_doc_url,
    _slugify,
)
from xml_handler import XMLHandler
from storage_handler import StorageHandler

# =============================================================================
# LOGGING
# =============================================================================
LOGS_DIR = Path(config.LOGS_DIR)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOGS_DIR / f"lovdata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# APP
# =============================================================================

class LovdataScraperApp:

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        if config.HEADLESS:
            chrome_options.add_argument("--headless=new")

        self.driver  = webdriver.Chrome(options=chrome_options)
        self.scraper = LovdataScraper(self.driver)
        self.storage = StorageHandler()

        self.batch_items: list = []
        self.stats = {"success": 0, "failed": 0, "skipped": 0}

        logger.info("=" * 70)
        logger.info("ENV / CONFIG")
        logger.info("  SUPABASE_TABLE  : %s", config.SUPABASE_TABLE)
        logger.info("  SUPABASE_BUCKET : %s", config.SUPABASE_BUCKET)
        logger.info("  BASE_DIR        : %s", config.BASE_DIR)
        logger.info("  TIMEOUT         : %s", config.TIMEOUT)
        logger.info("  HEADLESS        : %s", config.HEADLESS)
        logger.info("  YEAR RANGE      : %s – %s", config.START_YEAR, config.END_YEAR)
        logger.info("  BATCH SIZE      : %s", config.BATCH_UPLOAD_SIZE)
        logger.info("=" * 70)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _sanitize(text: str) -> str:
        t = (text or "unknown").strip()
        for old, new in [
            ("ø","o"),("Ø","O"),("å","a"),("Å","A"),
            ("æ","ae"),("Æ","AE"),("/","_"),("\\","_"),
            (":","_"),(",",""),(".",""),
        ]:
            t = t.replace(old, new)
        return "_".join(t.split())[:120] or "unknown"

    def _is_valid_year(self, year) -> bool:
        try:
            return config.START_YEAR <= int(year) <= config.END_YEAR
        except Exception:
            return False

    def _local_folder(self, root: str, branch: str, leaf: str) -> str:
        parts = [
            config.BASE_DIR,
            self._sanitize(root),
            self._sanitize(branch) if branch else "root",
            self._sanitize(leaf)   if leaf   else "leaf",
        ]
        return str(Path(*parts))

    def _bucket_path(self, root: str, branch: str, leaf: str, file_name: str) -> str:
        return "/".join([
            self._sanitize(root),
            self._sanitize(branch) if branch else "root",
            self._sanitize(leaf)   if leaf   else "leaf",
            file_name,
        ])

    # -------------------------------------------------------------------------
    # Process ONE document URL
    # -------------------------------------------------------------------------

    def _process_one_document(
        self,
        doc_url:       str,
        root:          str,
        branch:        str,
        leaf:          str,
        document_type: str,
    ):
        try:
            doc_id = _extract_doc_id(doc_url)
            if not doc_id:
                logger.warning("⚠️  Cannot extract doc_id: %s", doc_url)
                self.stats["failed"] += 1
                return

            year = _extract_year_from_doc_url(doc_url)
            if year is None or not self._is_valid_year(year):
                logger.info("⏭️  Year %s out of range — skip: %s", year, doc_url)
                self.stats["skipped"] += 1
                return

            file_name = f"{doc_id}.xml"

            if self.storage.record_exists(file_name):
                logger.info("⏭️  Already in DB — skip: %s", file_name)
                self.stats["skipped"] += 1
                return

            # ── Step 1: Scrape content from the document page ──────────
            logger.info("🌐 Visiting: %s", doc_url)
            scraped = self.scraper.scrape_content_from_url(doc_url)

            content        = scraped.get("content", "").strip()
            title          = scraped.get("title", "")
            date           = scraped.get("date", "")
            content_source = scraped.get("content_source", "html_text")

            if not content:
                logger.warning("⚠️  Empty content — skip: %s", doc_url)
                self.stats["failed"] += 1
                return

            content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
            if self.storage.hash_exists(content_hash):
                logger.info("⏭️  Duplicate content — skip: %s", file_name)
                self.stats["skipped"] += 1
                return

            # ── Step 2: Save as local .xml file ───────────────────────
            local_folder = self._local_folder(root, branch, leaf)
            document = {
                "file_name":         file_name,
                "url":               doc_url,
                "title":             title,
                "date":              date,
                "content":           content,
                "content_source":    content_source,
                "legal_area_root":   root,
                "legal_area_branch": branch,
                "legal_area_leaf":   leaf if leaf else "",
                "document_type":     document_type,
            }

            file_path, file_size, file_hash, content_preview = XMLHandler.save(
                document, local_folder
            )

            if not file_path:
                logger.error("❌ XML save failed: %s", file_name)
                self.stats["failed"] += 1
                return

            bucket_pth = self._bucket_path(root, branch, leaf, file_name)

            # ── Step 3: Queue for batch upload ─────────────────────────
            self.batch_items.append({
                "local_path":  file_path,
                "bucket_path": bucket_pth,
                "metadata": {
                    "file_name":         file_name,
                    "file_hash":         file_hash,
                    "file_size":         file_size,
                    "legal_area_root":   root,
                    "legal_area_branch": branch,
                    "legal_area_leaf":   leaf if leaf else None,
                    "document_type":     document_type,
                    "source_url":        doc_url,
                    "content_source":    content_source,
                    "content_preview":   content_preview,
                    "bucket_path":       bucket_pth,
                },
            })

            self.stats["success"] += 1
            logger.info(
                "✅ Queued [%s/%s] %-40s type=%-30s source=%s",
                len(self.batch_items), config.BATCH_UPLOAD_SIZE,
                file_name, document_type, content_source,
            )

            # ── Step 4: Flush when batch is full ───────────────────────
            if len(self.batch_items) >= config.BATCH_UPLOAD_SIZE:
                self._flush_batch()

        except Exception as e:
            logger.error("❌ Error processing %s: %s", doc_url, e, exc_info=True)
            self.stats["failed"] += 1

    # -------------------------------------------------------------------------
    # Batch flush → upload + insert + delete local
    # -------------------------------------------------------------------------

    def _flush_batch(self):
        if not self.batch_items:
            return

        count = len(self.batch_items)
        logger.info("")
        logger.info("━" * 70)
        logger.info("⬆️  BATCH UPLOAD — %s XML files", count)
        logger.info("━" * 70)

        for i, item in enumerate(self.batch_items, 1):
            file_name  = item["metadata"]["file_name"]
            logger.info("  [%s/%s] Uploading: %s → bucket: %s",
                        i, count, file_name, item["bucket_path"])

            public_url = self.storage.upload_xml_and_delete_local(
                local_path=item["local_path"],
                bucket_path=item["bucket_path"],
            )

            if not public_url:
                logger.error("  ❌ Upload failed: %s", file_name)
                continue

            item["metadata"]["public_uri"] = public_url
            self.storage.insert_metadata(item["metadata"])

        self.batch_items.clear()
        self.storage.cleanup_empty_folders(config.BASE_DIR)

        logger.info("✅ Batch complete — all %s files uploaded & local copies removed", count)
        logger.info("━" * 70)
        logger.info("")

    # -------------------------------------------------------------------------
    # Scrape all sections on currently open legal-area page
    # -------------------------------------------------------------------------

    def _scrape_current_area(self, root: str, branch: str, leaf: str):
        # Capture the current page URL — this is the area overview page
        area_url = self.driver.current_url
        logger.info(
            "🔍 Scraping  root='%s'  branch='%s'  leaf='%s'",
            root, branch, leaf,
        )
        logger.info("   Area URL: %s", area_url)

        sections = self.scraper.collect_urls_by_section(
            area_url=area_url, max_pages=500
        )

        if not sections:
            logger.warning("   ⚠️  No sections / URLs found.")
            return

        for sec in sections:
            doc_type = sec["document_type"]
            expected = sec["expected_count"]
            urls     = sec["urls"]

            logger.info(
                "   📂 SECTION | %-55s | expected=%-5s | found=%s",
                doc_type, expected, len(urls),
            )

            for url in urls[: config.MAX_DOCS_PER_LEGAL_AREA]:
                self._process_one_document(
                    url,
                    root=root,
                    branch=branch,
                    leaf=leaf,
                    document_type=doc_type,
                )
                time.sleep(config.DELAY_BETWEEN_REQUESTS)

    # -------------------------------------------------------------------------
    # Tree navigation: Root → Branch → Leaf
    # -------------------------------------------------------------------------

    def _run_legal_areas(self):
        self.scraper.go_to_legal_areas()
        roots = self.scraper.discover_legal_area_links()

        if not roots:
            logger.error("❌ No ROOT legal areas found.")
            return

        root_slugs = list(roots.keys())
        logger.info("📊 Total ROOTs: %s", len(root_slugs))

        for r_idx, root_slug in enumerate(root_slugs, 1):

            self.scraper.go_to_legal_areas()
            fresh = self.scraper.discover_legal_area_links()
            if root_slug not in fresh:
                logger.warning("⚠️  Root missing after refresh: %s", root_slug)
                continue

            root_el   = fresh[root_slug]["element"]
            root_name = fresh[root_slug]["text"].strip()

            logger.info("\n" + "=" * 70)
            logger.info("[%s/%s] ROOT: %s", r_idx, len(root_slugs), root_name)
            logger.info("=" * 70)

            self.scraper._click_node(root_el)
            time.sleep(1.5)
            self.scraper._expand_node(root_el)

            branches = self.scraper._get_children(root_el)
            logger.info("   Branch count: %s", len(branches))

            if not branches:
                self._scrape_current_area(root_name, "", "")
                continue

            for b_idx in range(len(branches)):
                self.scraper.go_to_legal_areas()
                fresh = self.scraper.discover_legal_area_links()
                if root_slug not in fresh:
                    continue

                root_el = fresh[root_slug]["element"]
                self.scraper._click_node(root_el)
                time.sleep(1.0)
                self.scraper._expand_node(root_el)

                branches_now = self.scraper._get_children(root_el)
                if b_idx >= len(branches_now):
                    continue

                b_el        = branches_now[b_idx]
                branch_name = (b_el.text or "").strip()
                if not branch_name:
                    continue

                logger.info("   [%s/%s] BRANCH: %s", b_idx + 1, len(branches_now), branch_name)

                self.scraper._click_node(b_el)
                time.sleep(1.0)
                self.scraper._expand_node(b_el)

                leaves = self.scraper._get_children(b_el)
                logger.info("      Leaf count: %s", len(leaves))

                if not leaves:
                    self._scrape_current_area(root_name, branch_name, "")
                    continue

                for l_idx in range(len(leaves)):
                    self.scraper.go_to_legal_areas()
                    fresh = self.scraper.discover_legal_area_links()
                    if root_slug not in fresh:
                        continue

                    root_el = fresh[root_slug]["element"]
                    self.scraper._click_node(root_el)
                    time.sleep(1.0)
                    self.scraper._expand_node(root_el)

                    branches_now = self.scraper._get_children(root_el)
                    if b_idx >= len(branches_now):
                        continue
                    b_el = branches_now[b_idx]
                    self.scraper._click_node(b_el)
                    time.sleep(1.0)
                    self.scraper._expand_node(b_el)

                    leaves_now = self.scraper._get_children(b_el)
                    if l_idx >= len(leaves_now):
                        continue

                    l_el      = leaves_now[l_idx]
                    leaf_name = (l_el.text or "").strip()
                    if not leaf_name:
                        continue

                    logger.info(
                        "      [%s/%s] LEAF: %s", l_idx + 1, len(leaves_now), leaf_name
                    )

                    self.scraper._click_node(l_el)
                    time.sleep(1.5)

                    self._scrape_current_area(root_name, branch_name, leaf_name)

        logger.info("\n✅ All legal areas processed.")

    # -------------------------------------------------------------------------
    # Entry point
    # -------------------------------------------------------------------------

    def run(self):
        try:
            if not self.scraper.login():
                logger.error("❌ LOGIN FAILED — aborting.")
                return
            self._run_legal_areas()

        finally:
            if self.batch_items:
                logger.info("🔄 Flushing remaining %s items …", len(self.batch_items))
                self._flush_batch()
            try:
                self.driver.quit()
            except Exception:
                pass
            self._print_summary()

    def _print_summary(self):
        logger.info("\n" + "=" * 70)
        logger.info("SCRAPING SUMMARY")
        logger.info("  ✅ Success : %s", self.stats["success"])
        logger.info("  ❌ Failed  : %s", self.stats["failed"])
        logger.info("  ⏭️  Skipped : %s", self.stats["skipped"])
        logger.info("=" * 70)


# =============================================================================
# CLI
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("  LOVDATA PRO SCRAPER")
    print("=" * 70)
    if input("\nProceed? (yes/no): ").strip().lower() != "yes":
        print("❌ Cancelled.")
        return
    LovdataScraperApp().run()


if __name__ == "__main__":
    main()