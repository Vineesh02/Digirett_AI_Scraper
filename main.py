"""
LOVDATA PRO SCRAPER — MAIN
==========================
Pipeline:
  1. Login
  2. Navigate tree: Root -> Branch -> Leaf
  3. For each legal area:
     a. Detect section tabs (LAWS, REGULATIONS, etc.)
     b. For each section: click tab -> Vis alle -> paginate -> collect URLs
  4. All URLs collected across all sections -> deduplicated -> process in batches
  5. Per batch item:
     a. Scrape document content from URL
     b. Generate XML with canonical URL and exact document year
     c. Upload to Supabase Storage
     d. Insert metadata into Supabase table
     e. Delete local XML file

MULTI-LAPTOP USAGE:
  - Run choice "2" on any laptop first to list all category names
  - Copy desired category names into config.TARGET_ROOT_CATEGORIES on each laptop
  - Each laptop scrapes different categories, all upload to the same Supabase
  - Duplicate prevention is automatic via record_exists() + hash_exists()
"""

import sys
import time
import logging
import hashlib
import re
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
# APPLICATION
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

        # ── Detailed stats — every skip reason tracked separately ─────────────
        self.stats = {
            "success":            0,
            "failed_no_id":       0,   # could not extract doc ID from URL
            "failed_empty":       0,   # page loaded but no content extracted
            "failed_xml":         0,   # XMLHandler.save() returned no path
            "failed_other":       0,   # unexpected exception
            "skip_in_supabase":   0,   # file_name already exists in DB
            "skip_duplicate_hash":0,   # identical content hash already in DB
        }

        logger.info("=" * 70)
        logger.info("LOVDATA PRO SCRAPER")
        logger.info("  Table   : %s", config.SUPABASE_TABLE)
        logger.info("  Bucket  : %s", config.SUPABASE_BUCKET)
        logger.info("  Base dir: %s", config.BASE_DIR)
        logger.info("  Timeout : %s", config.TIMEOUT)
        logger.info("  Headless: %s", config.HEADLESS)
        logger.info("  Years   : %s - %s", config.START_YEAR, config.END_YEAR)
        logger.info("  Batch   : %s", config.BATCH_UPLOAD_SIZE)
        logger.info("  Max docs: %s", config.MAX_DOCS_PER_LEGAL_AREA)

        targets = getattr(config, "TARGET_ROOT_CATEGORIES", None)
        if targets:
            logger.info("  Target categories (%s):", len(targets))
            for cat in targets:
                logger.info("    - %s", cat)
        else:
            logger.info("  Target categories: ALL")
        logger.info("=" * 70)

    # -------------------------------------------------------------------------
    # List all categories
    # -------------------------------------------------------------------------

    def list_all_categories(self):
        self.scraper.go_to_legal_areas()
        roots = self.scraper.discover_legal_area_links()

        print("\n" + "=" * 70)
        print("  ALL AVAILABLE ROOT CATEGORIES")
        print("=" * 70)
        print(f"  Total: {len(roots)}\n")

        for i, (slug, info) in enumerate(roots.items(), 1):
            print(f"  [{i:>2}] name : '{info['text']}'")
            print(f"        slug : '{slug}'")
            print()

        print("=" * 70)
        print("  Copy the 'name' values into config.TARGET_ROOT_CATEGORIES")
        print("  Split them across laptops so each laptop scrapes different categories.")
        print("=" * 70)
        print()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _sanitize(text: str) -> str:
        t = (text or "unknown").strip()
        for old, new in [
            ("ø", "o"), ("Ø", "O"), ("å", "a"), ("Å", "A"),
            ("æ", "ae"), ("Æ", "AE"), ("/", "_"), ("\\", "_"),
            (":", "_"), (",", ""), (".", ""),
        ]:
            t = t.replace(old, new)
        return "_".join(t.split())[:120] or "unknown"

    def _local_folder(self, root: str, branch: str, leaf: str) -> str:
        parts = [config.BASE_DIR, self._sanitize(root)]
        if branch:
            parts.append(self._sanitize(branch))
        if leaf:
            parts.append(self._sanitize(leaf))
        return str(Path(*parts))

    def _bucket_path(self, root: str, branch: str, leaf: str, file_name: str) -> str:
        parts = [self._sanitize(root)]
        if branch:
            parts.append(self._sanitize(branch))
        if leaf:
            parts.append(self._sanitize(leaf))
        parts.append(file_name)
        return "/".join(parts)

    # -------------------------------------------------------------------------
    # Multi-laptop category filter
    # -------------------------------------------------------------------------

    def _filter_root_slugs(self, root_slugs: list, roots: dict) -> list:
        targets = getattr(config, "TARGET_ROOT_CATEGORIES", None)

        if not targets:
            logger.info("No category filter — scraping ALL %s categories", len(root_slugs))
            return root_slugs

        target_slug_set = {_slugify(name)[:60] for name in targets}
        filtered = [s for s in root_slugs if s in target_slug_set]

        for name in targets:
            found = any(
                name.strip().lower() in info["text"].lower()
                for info in roots.values()
                if _slugify(info["text"])[:60] in target_slug_set
            )
            if not found:
                logger.warning(
                    "  Configured category NOT FOUND in tree: '%s'  "
                    "(check spelling in config.TARGET_ROOT_CATEGORIES)", name,
                )

        if not filtered:
            logger.error(
                "No categories matched TARGET_ROOT_CATEGORIES! "
                "Run choice '2' to list available category names."
            )
        else:
            logger.info(
                "Category filter active — %s/%s categories will be scraped:",
                len(filtered), len(root_slugs),
            )
            for slug in filtered:
                logger.info("    ✓ %s", roots.get(slug, {}).get("text", slug))
            logger.info(
                "  Skipping %s categories (assigned to other laptops)",
                len(root_slugs) - len(filtered),
            )

        return filtered

    # -------------------------------------------------------------------------
    # Process one document URL
    # -------------------------------------------------------------------------

    def _process_one_document(
        self,
        doc_url:       str,
        root:          str,
        branch:        str,
        leaf:          str,
        document_type: str,
        doc_index:     int,
        doc_total:     int,
    ):
        try:
            doc_id = _extract_doc_id(doc_url)
            if not doc_id:
                logger.warning(
                    "  [%s/%s] SKIP — cannot extract doc ID: %s",
                    doc_index, doc_total, doc_url,
                )
                self.stats["failed_no_id"] += 1
                return

            file_name = f"{doc_id}.xml"

            # ── Skip: already in Supabase ─────────────────────────────────────
            if self.storage.record_exists(file_name):
                logger.info(
                    "  [%s/%s] SKIP (already in Supabase): %s",
                    doc_index, doc_total, file_name,
                )
                self.stats["skip_in_supabase"] += 1
                return

            logger.info("  [%s/%s] Scraping: %s", doc_index, doc_total, doc_url)
            scraped = self.scraper.scrape_content_from_url(doc_url)

            content        = scraped.get("content", "").strip()
            title          = scraped.get("title", "")
            date           = scraped.get("date", "")
            scraped_year   = scraped.get("year")
            content_source = scraped.get("content_source", "")
            page_meta      = scraped.get("page_meta", {})

            # ── Skip: empty content ───────────────────────────────────────────
            if not content:
                logger.warning(
                    "  [%s/%s] SKIP (empty content): %s",
                    doc_index, doc_total, doc_id,
                )
                self.stats["failed_empty"] += 1
                return

            url_year   = _extract_year_from_doc_url(doc_url)
            exact_year = scraped_year or url_year

            if exact_year:
                logger.info(
                    "  [%s/%s] Year=%s  title=%s",
                    doc_index, doc_total, exact_year, (title or "")[:60],
                )

            # ── Skip: duplicate content hash ──────────────────────────────────
            content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
            if self.storage.hash_exists(content_hash):
                logger.info(
                    "  [%s/%s] SKIP (duplicate content hash): %s",
                    doc_index, doc_total, file_name,
                )
                self.stats["skip_duplicate_hash"] += 1
                return

            # ── Save XML locally ──────────────────────────────────────────────
            local_folder = self._local_folder(root, branch, leaf)
            document = {
                "file_name":      file_name,
                "url":            doc_url,
                "document_type":  document_type,
                "title":          title,
                "date":           date,
                "year":           exact_year,
                "content":        content,
                "content_source": content_source,
                "page_meta":      page_meta,
            }

            file_path, file_size, file_hash, _ = XMLHandler.save(document, local_folder)

            if not file_path:
                logger.error(
                    "  [%s/%s] FAIL (XML save failed): %s",
                    doc_index, doc_total, file_name,
                )
                self.stats["failed_xml"] += 1
                return

            bucket_pth = self._bucket_path(root, branch, leaf, file_name)

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
                    "bucket_path":       bucket_pth,
                },
            })

            self.stats["success"] += 1
            logger.info(
                "  [%s/%s] Queued: %-40s  type=%s",
                doc_index, doc_total, file_name, document_type,
            )

            if len(self.batch_items) >= config.BATCH_UPLOAD_SIZE:
                self._flush_batch()

        except Exception as e:
            logger.error(
                "  [%s/%s] FAIL (exception) %s: %s",
                doc_index, doc_total, doc_url, e, exc_info=True,
            )
            self.stats["failed_other"] += 1

    # -------------------------------------------------------------------------
    # Batch flush
    # -------------------------------------------------------------------------

    def _flush_batch(self):
        if not self.batch_items:
            return

        count = len(self.batch_items)
        logger.info("")
        logger.info("-" * 70)
        logger.info("Batch upload: %s files", count)
        logger.info("-" * 70)

        for i, item in enumerate(self.batch_items, 1):
            file_name = item["metadata"]["file_name"]
            logger.info("  [%s/%s] %s -> %s", i, count, file_name, item["bucket_path"])

            public_url = self.storage.upload_xml(
                local_path=item["local_path"],
                bucket_path=item["bucket_path"],
            )

            if not public_url:
                logger.error("  Upload failed — skipping metadata insert: %s", file_name)
                continue

            item["metadata"]["public_uri"] = public_url
            inserted = self.storage.insert_metadata(item["metadata"])

            if not inserted:
                logger.error("  Metadata insert failed: %s", file_name)

            self.storage.delete_local(item["local_path"])

        self.batch_items.clear()
        self.storage.cleanup_empty_folders(config.BASE_DIR)

        logger.info("Batch complete: %s files processed", count)
        logger.info("-" * 70)
        logger.info("")

    # -------------------------------------------------------------------------
    # Scrape current legal-area page
    # -------------------------------------------------------------------------

    def _scrape_current_area(self, root: str, branch: str, leaf: str):
        logger.info(
            "Scraping area  root='%s'  branch='%s'  leaf='%s'",
            root, branch, leaf,
        )

        sections = self.scraper.collect_urls_from_current_view(max_pages=500)

        if not sections:
            logger.warning("No sections or URLs found for this area")
            return

        total_urls = sum(len(s["urls"]) for s in sections)
        logger.info("")
        logger.info("Section collection summary:")
        for sec in sections:
            logger.info(
                "  %-55s  expected=%-6s  found=%s",
                sec["document_type"],
                sec["expected_count"] if sec["expected_count"] is not None else "?",
                len(sec["urls"]),
            )
        logger.info("Total URLs collected (before dedup): %s", total_urls)
        logger.info("")

        seen_urls: set = set()
        flat_docs: list = []

        for sec in sections:
            for url in sec["urls"][:config.MAX_DOCS_PER_LEGAL_AREA]:
                if url not in seen_urls:
                    seen_urls.add(url)
                    flat_docs.append((url, sec["document_type"]))

        duplicates_removed = total_urls - len(flat_docs)
        if duplicates_removed > 0:
            logger.info(
                "After dedup: %s unique URLs (%s cross-section duplicates removed)",
                len(flat_docs), duplicates_removed,
            )

        logger.info(
            "Processing %s documents in batches of %s",
            len(flat_docs), config.BATCH_UPLOAD_SIZE,
        )

        for doc_idx, (url, doc_type) in enumerate(flat_docs, 1):
            self._process_one_document(
                url,
                root=root,
                branch=branch,
                leaf=leaf,
                document_type=doc_type,
                doc_index=doc_idx,
                doc_total=len(flat_docs),
            )
            time.sleep(config.DELAY_BETWEEN_REQUESTS)

    # -------------------------------------------------------------------------
    # Tree navigation
    # -------------------------------------------------------------------------

    def _run_legal_areas(self):
        self.scraper.go_to_legal_areas()
        roots = self.scraper.discover_legal_area_links()

        if not roots:
            logger.error("No root legal areas found")
            return

        root_slugs = list(roots.keys())
        logger.info("Total root categories found in tree: %s", len(root_slugs))

        root_slugs = self._filter_root_slugs(root_slugs, roots)
        if not root_slugs:
            logger.error("No categories to process — check TARGET_ROOT_CATEGORIES in config.py")
            return

        logger.info("Will process %s root categories", len(root_slugs))

        for r_idx, root_slug in enumerate(root_slugs, 1):
            self.scraper.go_to_legal_areas()
            fresh = self.scraper.discover_legal_area_links()
            if root_slug not in fresh:
                logger.warning("Root missing after refresh: %s", root_slug)
                continue

            root_el   = fresh[root_slug]["element"]
            root_name = fresh[root_slug]["text"].strip()

            logger.info("")
            logger.info("=" * 70)
            logger.info("[%s/%s] Root: %s", r_idx, len(root_slugs), root_name)
            logger.info("=" * 70)

            self.scraper._click_node(root_el)
            time.sleep(1.5)
            self.scraper._expand_node(root_el)

            branches = self.scraper._get_children(root_el)
            logger.info("  Branches: %s", len(branches))

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

                logger.info(
                    "  [%s/%s] Branch: %s", b_idx + 1, len(branches_now), branch_name,
                )

                self.scraper._click_node(b_el)
                time.sleep(1.0)
                self.scraper._expand_node(b_el)

                leaves = self.scraper._get_children(b_el)
                logger.info("    Leaves: %s", len(leaves))

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
                        "    [%s/%s] Leaf: %s", l_idx + 1, len(leaves_now), leaf_name,
                    )

                    self.scraper._click_node(l_el)
                    time.sleep(1.5)

                    self._scrape_current_area(root_name, branch_name, leaf_name)

        logger.info("All assigned legal areas processed")

    # -------------------------------------------------------------------------
    # Entry point
    # -------------------------------------------------------------------------

    def run(self):
        try:
            if not self.scraper.login():
                logger.error("Login failed — aborting")
                return
            self._run_legal_areas()

        finally:
            if self.batch_items:
                logger.info("Flushing remaining %s items", len(self.batch_items))
                self._flush_batch()
            try:
                self.driver.quit()
            except Exception:
                pass
            self._print_summary()

    def _print_summary(self):
        s = self.stats
        total_skipped = s["skip_in_supabase"] + s["skip_duplicate_hash"]
        total_failed  = s["failed_no_id"] + s["failed_empty"] + s["failed_xml"] + s["failed_other"]
        total         = s["success"] + total_skipped + total_failed

        logger.info("")
        logger.info("=" * 70)
        logger.info("SCRAPING SUMMARY")
        logger.info("  ✓ Success                  : %s", s["success"])
        logger.info("")
        logger.info("  — Skipped (not an error) —")
        logger.info("    Already in Supabase      : %s", s["skip_in_supabase"])
        logger.info("    Duplicate content hash   : %s", s["skip_duplicate_hash"])
        logger.info("    Subtotal skipped         : %s", total_skipped)
        logger.info("")
        logger.info("  — Failed (investigate) —")
        logger.info("    No doc ID from URL       : %s", s["failed_no_id"])
        logger.info("    Empty content            : %s", s["failed_empty"])
        logger.info("    XML save error           : %s", s["failed_xml"])
        logger.info("    Other exception          : %s", s["failed_other"])
        logger.info("    Subtotal failed          : %s", total_failed)
        logger.info("")
        logger.info("  Total processed            : %s", total)
        logger.info("=" * 70)


# =============================================================================
# CLI
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("  LOVDATA PRO SCRAPER")
    print("=" * 70)
    print()
    print("  1. Run scraper")
    print("  2. List all categories  (use this first to set up multi-laptop split)")
    print()
    choice = input("Choice (1/2): ").strip()

    if choice == "2":
        print("\nStarting browser to fetch category list...")
        app = LovdataScraperApp()
        try:
            if not app.scraper.login():
                print("Login failed — check credentials in config.py")
                return
            app.list_all_categories()
        finally:
            try:
                app.driver.quit()
            except Exception:
                pass
        print("Done. Copy the 'name' values into config.TARGET_ROOT_CATEGORIES")
        return

    if choice != "1":
        print("Invalid choice — exiting.")
        return

    targets = getattr(config, "TARGET_ROOT_CATEGORIES", None)
    if targets:
        print(f"\nThis laptop will scrape {len(targets)} categories:")
        for cat in targets:
            print(f"  - {cat}")
    else:
        print("\nThis laptop will scrape ALL categories (TARGET_ROOT_CATEGORIES = None)")

    print()
    if input("Proceed? (yes/no): ").strip().lower() != "yes":
        print("Cancelled.")
        return

    LovdataScraperApp().run()


if __name__ == "__main__":
    main()