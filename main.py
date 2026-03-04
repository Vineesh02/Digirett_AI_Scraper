"""
LOVDATA PRO SCRAPER — MAIN
==========================
Full pipeline:
  1. Login
  2. Navigate tree: Root → Branch → Leaf
  3. For each area page (AFTER tree-node click, content already loaded):
       a. Discover section panels dynamically (no driver.get() re-navigation)
       b. Click Show All per section → paginate → collect all URLs
       c. Verify URL count matches expected count
  4. Visit each URL → scrape content → save as local .xml
  5. Every BATCH_UPLOAD_SIZE files → upload to Supabase bucket
                                   → insert metadata row
                                   → delete local XML file

PARALLEL EXECUTION (3–4 laptops simultaneously)
────────────────────────────────────────────────
Each laptop "claims" a (root, branch, leaf) area in Supabase before
processing it.  Other laptops skip already-claimed areas.
Claims expire after CLAIM_TTL_MINUTES (config) so a crashed laptop's
areas are automatically retried.

Claim table schema (create once in Supabase):
  CREATE TABLE IF NOT EXISTS scraper_claims (
      area_key       TEXT PRIMARY KEY,
      claimed_by     TEXT NOT NULL,
      claimed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      status         TEXT NOT NULL DEFAULT 'processing'
      -- status: 'processing' | 'done' | 'failed'
  );
"""

import sys
import time
import uuid
import logging
import hashlib
from datetime import datetime, timezone, timedelta
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

# Unique ID for this laptop/run so we can identify who owns a claim
WORKER_ID = str(uuid.uuid4())[:8]

# =============================================================================
# CLAIM MANAGER  (parallel execution support)
# =============================================================================

class ClaimManager:
    """
    Uses a Supabase table `scraper_claims` to coordinate parallel scraping.
    Each (root, branch, leaf) area is claimed atomically.
    Claims expire after CLAIM_TTL_MINUTES so failed workers don't block others.
    """

    CLAIMS_TABLE = "scraper_claims"

    def __init__(self, storage: StorageHandler):
        self._client = storage.client
        self._ttl    = getattr(config, "CLAIM_TTL_MINUTES", 60)
        self._worker = WORKER_ID
        # Ensure claims table exists (best-effort; may fail if no DDL rights)
        self._ensure_table()

    def _ensure_table(self):
        try:
            self._client.table(self.CLAIMS_TABLE).select("area_key").limit(1).execute()
        except Exception:
            logger.warning(
                "⚠️  scraper_claims table not found — parallel claim tracking disabled. "
                "Create it with:  CREATE TABLE scraper_claims "
                "(area_key TEXT PRIMARY KEY, claimed_by TEXT, claimed_at TIMESTAMPTZ, status TEXT);"
            )

    def _area_key(self, root: str, branch: str, leaf: str) -> str:
        return f"{_slugify(root)}/{_slugify(branch or '_')}/{_slugify(leaf or '_')}"

    def try_claim(self, root: str, branch: str, leaf: str) -> bool:
        """
        Try to claim this area.
        Returns True  → this worker owns it, proceed.
        Returns False → another worker already has it (or it's done), skip.
        """
        key = self._area_key(root, branch, leaf)
        now = datetime.now(timezone.utc)

        try:
            # Check for existing live claim
            resp = (
                self._client.table(self.CLAIMS_TABLE)
                .select("claimed_by,claimed_at,status")
                .eq("area_key", key)
                .execute()
            )
            if resp.data:
                row = resp.data[0]
                if row["status"] == "done":
                    logger.info("⏭️  Area already done by %s — skip: %s", row["claimed_by"], key)
                    return False
                # Check if the claim has expired
                claimed_at = datetime.fromisoformat(row["claimed_at"].replace("Z", "+00:00"))
                age_minutes = (now - claimed_at).total_seconds() / 60
                if age_minutes < self._ttl:
                    logger.info(
                        "⏭️  Area claimed by %s (%.1f min ago, TTL=%s min) — skip: %s",
                        row["claimed_by"], age_minutes, self._ttl, key
                    )
                    return False
                # Claim expired — take it over
                logger.info(
                    "♻️  Expired claim (%.1f min old) — reclaiming: %s", age_minutes, key
                )

            # Upsert our claim
            self._client.table(self.CLAIMS_TABLE).upsert({
                "area_key":   key,
                "claimed_by": self._worker,
                "claimed_at": now.isoformat(),
                "status":     "processing",
            }).execute()
            logger.info("🔒 Claimed area: %s (worker=%s)", key, self._worker)
            return True

        except Exception as e:
            logger.warning("⚠️  Claim check failed (%s) — proceeding anyway: %s", e, key)
            return True   # On error, allow processing rather than skipping

    def mark_done(self, root: str, branch: str, leaf: str):
        key = self._area_key(root, branch, leaf)
        try:
            self._client.table(self.CLAIMS_TABLE).update({
                "status": "done"
            }).eq("area_key", key).execute()
            logger.info("✅ Marked done: %s", key)
        except Exception as e:
            logger.warning("⚠️  mark_done failed: %s", e)

    def mark_failed(self, root: str, branch: str, leaf: str):
        key = self._area_key(root, branch, leaf)
        try:
            self._client.table(self.CLAIMS_TABLE).update({
                "status": "failed"
            }).eq("area_key", key).execute()
        except Exception as e:
            logger.warning("⚠️  mark_failed failed: %s", e)


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
        self.claims  = ClaimManager(self.storage)

        self.batch_items: list = []
        self.stats = {"success": 0, "failed": 0, "skipped": 0}

        logger.info("=" * 70)
        logger.info("ENV / CONFIG")
        logger.info("  WORKER ID       : %s", WORKER_ID)
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
            file_name = item["metadata"]["file_name"]
            logger.info(
                "  [%s/%s] Uploading: %s → bucket: %s",
                i, count, file_name, item["bucket_path"]
            )

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
    # Scrape all sections on currently loaded legal-area page
    # *** IMPORTANT: content is already loaded — do NOT navigate again ***
    # -------------------------------------------------------------------------

    def _scrape_current_area(self, root: str, branch: str, leaf: str):
        logger.info(
            "🔍 Scraping  root='%s'  branch='%s'  leaf='%s'",
            root, branch, leaf,
        )

        # Check parallel claim
        if not self.claims.try_claim(root, branch, leaf):
            self.stats["skipped"] += 1
            return

        try:
            # ── collect_urls_from_current_view works on the ALREADY-LOADED page ──
            sections = self.scraper.collect_urls_from_current_view(max_pages=500)

            if not sections:
                logger.warning("   ⚠️  No sections / URLs found.")
                self.claims.mark_failed(root, branch, leaf)
                return

            total_urls = sum(len(s["urls"]) for s in sections)
            logger.info("   📊 Total sections=%s  total_urls=%s", len(sections), total_urls)

            for sec in sections:
                doc_type = sec["document_type"]
                expected = sec["expected_count"]
                urls     = sec["urls"]

                logger.info(
                    "   📂 SECTION | %-55s | expected=%-5s | found=%s",
                    doc_type, expected, len(urls),
                )

                # Warn if URL count doesn't match expected
                if expected and len(urls) != expected:
                    logger.warning(
                        "   ⚠️  COUNT MISMATCH: expected %s, found %s for section '%s'",
                        expected, len(urls), doc_type
                    )

                max_docs = getattr(config, "MAX_DOCS_PER_LEGAL_AREA", None)
                url_list = urls[:max_docs] if max_docs else urls

                for url in url_list:
                    self._process_one_document(
                        url,
                        root=root,
                        branch=branch,
                        leaf=leaf,
                        document_type=doc_type,
                    )
                    time.sleep(config.DELAY_BETWEEN_REQUESTS)

            self.claims.mark_done(root, branch, leaf)

        except Exception as e:
            logger.error("❌ _scrape_current_area failed: %s", e, exc_info=True)
            self.claims.mark_failed(root, branch, leaf)

    # -------------------------------------------------------------------------
    # Tree navigation: Root → Branch → Leaf
    # After EVERY tree-node click the right-hand panel reloads dynamically.
    # We NEVER call driver.get() for the content — only for the tree base URL.
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

            # Reload tree to get fresh elements (stale-proof)
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
                # Root with no branches — content loads directly after clicking root
                self._scrape_current_area(root_name, "", "")
                continue

            for b_idx in range(len(branches)):
                # Re-fetch tree state for each branch to avoid stale refs
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
                time.sleep(1.5)   # Wait for content panel to load
                self.scraper._expand_node(b_el)

                leaves = self.scraper._get_children(b_el)
                logger.info("      Leaf count: %s", len(leaves))

                if not leaves:
                    # Branch with no leaves — content is loaded after clicking branch
                    self._scrape_current_area(root_name, branch_name, "")
                    continue

                for l_idx in range(len(leaves)):
                    # Re-fetch for each leaf
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

                    # Click leaf — content panel loads dynamically here
                    self.scraper._click_node(l_el)
                    # Wait for div.legal-area-header — _wait_for_legal_area_header
                    # inside collect_urls_from_current_view handles this, but an
                    # extra sleep here avoids race conditions on slow machines.
                    time.sleep(2.0)

                    # _scrape_current_area reads from CURRENT page — no driver.get()
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
        logger.info("  Worker ID : %s", WORKER_ID)
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