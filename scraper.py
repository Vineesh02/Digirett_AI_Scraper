import time
import re
import logging
from typing import List, Optional, Dict, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    NoSuchElementException,
    TimeoutException,
    ElementNotInteractableException,
)

import config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Section definitions:
#   (norwegian_label,  canonical_english,  stable_div_id)
# The div IDs are confirmed from DevTools — they never change.
# ─────────────────────────────────────────────────────────────────────────────
SECTION_DEFS = [
    ("Siste dokumenter",
     "LATEST DOCUMENTS",
     "saker"),
    ("Lover",
     "LAWS",
     "lover"),
    ("Forskrifter",
     "REGULATIONS",
     "forskrifter"),
    ("Avgjørelser fra Høyesterett",
     "DECISIONS FROM THE SUPREME COURT",
     "hr"),
    ("Avgjørelser fra lagmannsrettene",
     "DECISIONS FROM THE COURTS OF APPEAL",
     "lr"),
    ("Avgjørelser fra tingrettene",
     "DECISIONS FROM THE DISTRICT COURTS",
     "tr"),
    ("Artikler",
     "ARTICLES",
     "artikler"),
    ("Dokumenter fra Klagenemnda for offentlige anskaffelser",
     "DOCUMENTS FROM THE PUBLIC PROCUREMENT COMPLAINTS BOARD",
     "kofa"),
    ("Dokumenter fra Byggebransjens Faglig Juridiske Råd",
     "DOCUMENTS FROM THE CONSTRUCTION INDUSTRY LEGAL COUNCIL",
     "bfjr"),
    ("Dokumenter fra Justisdepartementet",
     "DOCUMENTS FROM THE MINISTRY OF JUSTICE",
     "jd"),
    ("Andre dokumenter",
     "OTHER DOCUMENTS",
     "otherBases"),
]

# Quick lookups
_LABEL_UPPER_TO_DEF = {d[0].upper(): d for d in SECTION_DEFS}
_DIV_ID_TO_DEF      = {d[2]: d       for d in SECTION_DEFS}

# Backward-compat mapping used by old callers
SECTION_MAP = {d[0].upper(): d[1] for d in SECTION_DEFS}
SECTION_MAP.update({d[1]: d[1] for d in SECTION_DEFS})


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _slugify(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^\w\-_.]+", "", s)
    return s[:120] if s else "unknown"


def _extract_doc_id(url: str) -> str:
    m = re.search(r"#document/([^?#]+)", url)
    if not m:
        return ""
    return m.group(1).strip("/").replace("/", "_")


def _extract_year_from_doc_url(url: str) -> Optional[int]:
    for src in (_extract_doc_id(url), url):
        m = re.search(r"(19\d{2}|20\d{2})", src)
        if m:
            return int(m.group(1))
    return None


# ─────────────────────────────────────────────────────────────────────────────
class LovdataScraper:

    _NODE_TEXT_CSS      = "span.x-tree3-node-text"
    _EC_ICON_CSS        = "img.x-tree3-ec-icon"
    _SECTION_HEADER_CSS = "div.legal-area-header"
    _SECTION_LINK_CSS   = "a.gwt-Anchor"
    _SECTION_LABEL_CSS  = "span.label"

    def __init__(self, driver):
        self.driver = driver
        self.wait   = WebDriverWait(self.driver, config.TIMEOUT)

    # =========================================================================
    # LOGIN — DO NOT CHANGE
    # =========================================================================
    def login(self) -> bool:
        try:
            self.driver.get("https://lovdata.no/pro/auth/login")
            wait = WebDriverWait(self.driver, 30)
            email_input = wait.until(EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "input[type='email'], input[name='username'], input[type='text']"
            )))
            password_input = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input[type='password']")
            ))
            email_input.clear()
            password_input.clear()
            email_input.send_keys(config.LOVDATA_USERNAME)
            password_input.send_keys(config.LOVDATA_PASSWORD)
            password_input.submit()
            wait.until(lambda d: "/auth/login" not in d.current_url)
            logger.info("✅ Login successful")
            return True
        except Exception as e:
            logger.error("❌ Login failed: %s", e)
            return False

    # =========================================================================
    # NAVIGATION
    # =========================================================================

    def go_to_legal_areas(self):
        self.driver.switch_to.default_content()
        self.driver.get("https://lovdata.no/pro/#rettsomrade")
        self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)

        try:
            for a in self.driver.find_elements(By.TAG_NAME, "a"):
                if a.is_displayed() and "rettsområder" in (a.text or "").lower():
                    self.driver.execute_script("arguments[0].click();", a)
                    time.sleep(2)
                    break
        except Exception:
            pass

        try:
            self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, self._NODE_TEXT_CSS)
            ))
            logger.info("✅ Tree nodes present")
        except TimeoutException:
            logger.warning("⚠️  Tree nodes not found")
        time.sleep(1)

    # =========================================================================
    # TREE helpers  (unchanged — these work correctly)
    # =========================================================================

    def discover_legal_area_links(self) -> Dict[str, dict]:
        nodes = self.driver.find_elements(By.CSS_SELECTOR, self._NODE_TEXT_CSS)
        logger.info("Found %s tree node(s)", len(nodes))
        roots: Dict[str, dict] = {}
        for node in nodes:
            try:
                text = (node.text or "").strip()
                if not text:
                    continue
                slug = _slugify(text)[:60]
                if slug not in roots:
                    roots[slug] = {"element": node, "text": text}
            except StaleElementReferenceException:
                continue
        logger.info("📋 ROOT categories: %s", len(roots))
        return roots

    def _get_node_div(self, node_text_el):
        try:
            return node_text_el.find_element(
                By.XPATH,
                "./ancestor::div[contains(@class,'x-tree3-node')][1]"
            )
        except Exception:
            return None

    def _expand_node(self, node_text_el) -> None:
        try:
            node_div = self._get_node_div(node_text_el)
            if not node_div:
                return
            for icon in node_div.find_elements(By.CSS_SELECTOR, self._EC_ICON_CSS):
                cls = icon.get_attribute("class") or ""
                if "plus" in cls or "collapsed" in cls or "elbow-plus" in cls:
                    self.driver.execute_script("arguments[0].click();", icon)
                    time.sleep(0.8)
                    return
        except Exception:
            pass

    def _get_children(self, node_text_el) -> List:
        try:
            node_div = self._get_node_div(node_text_el)
            if not node_div:
                return []
            ct = node_div.find_elements(
                By.XPATH,
                "./following-sibling::div[contains(@class,'x-tree3-node-ct')][1]"
            )
            if not ct:
                ct = node_div.find_elements(By.XPATH, "./div[2]")
            if not ct:
                return []
            return ct[0].find_elements(By.CSS_SELECTOR, self._NODE_TEXT_CSS)
        except Exception:
            return []

    def _click_node(self, el) -> None:
        try:
            el.click()
        except Exception:
            try:
                self.driver.execute_script("arguments[0].click();", el)
            except Exception:
                pass

    # =========================================================================
    # WAIT FOR RIGHT-PANEL TO LOAD
    # =========================================================================

    def _wait_for_legal_area_header(self, timeout: int = 15) -> bool:
        """
        Wait for div.legal-area-header after a tree-node click.
        This is the section navigation bar — its presence = page loaded.
        Falls back to checking for div#saker (first section content div).
        """
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, self._SECTION_HEADER_CSS)
                )
            )
            time.sleep(0.5)
            logger.info("    ✅ div.legal-area-header present")
            return True
        except TimeoutException:
            pass

        logger.warning(
            "    ⚠️  div.legal-area-header not found in %ss — "
            "trying fallback div#saker", timeout
        )
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "saker"))
            )
            time.sleep(0.5)
            logger.info("    ✅ Fallback: div#saker found")
            return True
        except TimeoutException:
            logger.error("    ❌ Right-panel content did not load")
            return False

    # =========================================================================
    # FIND SECTION LINKS in div.legal-area-header
    # =========================================================================

    def _get_section_links(self) -> List[Tuple[str, str, str]]:
        """
        Returns list of (norwegian_label, canonical_english, div_id).
        We store ONLY text/strings — never Selenium elements — so nothing
        can go stale between sections.  We re-find the element fresh each
        time we need to click it (see _click_section_tab).
        """
        results = []
        seen_canonical: set = set()

        try:
            header = self.driver.find_element(By.CSS_SELECTOR, self._SECTION_HEADER_CSS)
        except NoSuchElementException:
            logger.error("    ❌ div.legal-area-header not found")
            return results

        links = header.find_elements(By.CSS_SELECTOR, self._SECTION_LINK_CSS)
        logger.info("    Found %s anchor links in legal-area-header", len(links))

        for link in links:
            try:
                try:
                    label_text = link.find_element(
                        By.CSS_SELECTOR, self._SECTION_LABEL_CSS
                    ).text.strip()
                except NoSuchElementException:
                    label_text = (link.text or "").strip()

                if not label_text:
                    continue

                upper = label_text.upper()
                defn  = _LABEL_UPPER_TO_DEF.get(upper)

                if not defn:
                    for key, d in _LABEL_UPPER_TO_DEF.items():
                        if key in upper or upper in key:
                            defn = d
                            break

                if not defn:
                    logger.debug("    ℹ️  No mapping for: '%s'", label_text)
                    continue

                canonical = defn[1]
                div_id    = defn[2]

                if canonical in seen_canonical:
                    continue

                seen_canonical.add(canonical)
                # Store STRINGS only — no element references
                results.append((label_text, canonical, div_id))
                logger.info(
                    "    ✓ Section: '%s'  →  %s  (div#%s)",
                    label_text, canonical, div_id
                )

            except StaleElementReferenceException:
                continue

        return results

    def _click_section_tab(self, label_text: str) -> bool:
        """
        Re-find the section tab link FRESH each time and click it.
        Never stores the element — always looks it up by span.label text.
        This prevents StaleElementReferenceException between sections.
        """
        try:
            header = self.driver.find_element(By.CSS_SELECTOR, self._SECTION_HEADER_CSS)
            links  = header.find_elements(By.CSS_SELECTOR, self._SECTION_LINK_CSS)
            for link in links:
                try:
                    try:
                        lbl = link.find_element(
                            By.CSS_SELECTOR, self._SECTION_LABEL_CSS
                        ).text.strip()
                    except NoSuchElementException:
                        lbl = (link.text or "").strip()

                    if lbl == label_text:
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center'});", link
                        )
                        time.sleep(0.2)
                        self.driver.execute_script("arguments[0].click();", link)
                        logger.info("    👆 Clicked section tab: '%s'", label_text)
                        return True
                except StaleElementReferenceException:
                    continue
        except Exception as e:
            logger.debug("    _click_section_tab error: %s", e)
        logger.warning("    ⚠️  Could not find section tab: '%s'", label_text)
        return False

    # =========================================================================
    # FIND SECTION BLOCK
    # =========================================================================
    # From DevTools (confirmed):
    #
    #   div#saker  class="viewTitle ..."      ← heading div (NOT the grid)
    #   div.gwt-Label                          ← subtitle text
    #   div.x-grid-panel  tabindex="0"         ← ACTUAL document grid
    #   table.x-btn.x-btn-noicon               ← "Vis alle (N)" button
    #   div#lover  class="viewTitle ..."       ← next section heading
    #
    # The section id (#saker, #lover …) is on the HEADING div only.
    # The grid and Vis-alle button are SIBLINGS that follow the heading.
    # We must collect all siblings between this heading and the next one.
    # =========================================================================

    def _get_section_siblings(self, div_id: str) -> List:
        """
        Find the heading div by id (in JS, never passing element as arg),
        then collect all following siblings up to the next viewTitle div.
        Using document.getElementById() inside JS avoids StaleElementReferenceException
        because we never hold a Python WebElement reference across DOM updates.
        """
        # Try the id and common variants
        actual_id = None
        for id_try in (div_id, div_id + "s", div_id + "Base", div_id + "Bases",
                       "third" + div_id.capitalize()):
            try:
                self.driver.find_element(By.ID, id_try)
                actual_id = id_try
                break
            except NoSuchElementException:
                continue

        if actual_id is None:
            logger.warning("    ⚠️  Heading div not found for id='%s'", div_id)
            return []

        try:
            siblings = self.driver.execute_script("""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return [];
                var siblings = [];
                var sibling = heading.nextElementSibling;
                while (sibling) {
                    if (sibling.classList.contains('viewTitle')) break;
                    siblings.push(sibling);
                    sibling = sibling.nextElementSibling;
                }
                return siblings;
            """, actual_id)
            return siblings or []
        except Exception as e:
            logger.debug("    _get_section_siblings JS error: %s", e)
            return []

    # =========================================================================
    # CLICK "VIS ALLE" — find button among section siblings
    # =========================================================================

    def _click_vis_alle(self, div_id: str) -> Optional[int]:
        """
        Find and click the "Vis alle (N)" button using pure JS getElementById.
        Never passes WebElement to JS — fully stale-proof.
        """
        total = None

        # Try id variants
        actual_id = None
        for id_try in (div_id, div_id + "s", div_id + "Base", div_id + "Bases",
                       "third" + div_id.capitalize()):
            try:
                self.driver.find_element(By.ID, id_try)
                actual_id = id_try
                break
            except NoSuchElementException:
                continue

        if actual_id is None:
            logger.info("    ℹ️  No 'Vis alle' button found for div#%s", div_id)
            return total

        try:
            result = self.driver.execute_script("""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return null;
                var sib = heading.nextElementSibling;
                while (sib) {
                    if (sib.classList.contains('viewTitle')) break;
                    var txt = sib.innerText || sib.textContent || '';
                    if (txt.toLowerCase().indexOf('vis alle') >= 0) {
                        // Extract count
                        var m = txt.match(/\((\d[\d\s]*)\)/);
                        if (!m) m = txt.toLowerCase().match(/vis alle\s+(\d+)/);
                        var count = m ? parseInt(m[1].replace(/\s/g,'')) : null;
                        // Click the button or the element itself
                        var btn = sib.querySelector('button');
                        if (btn) btn.click(); else sib.click();
                        return count;
                    }
                    sib = sib.nextElementSibling;
                }
                return null;
            """, actual_id)

            if result is not None:
                total = int(result)
                logger.info("    ✅ Vis alle clicked — expected=%s", total)
                time.sleep(2.5)
            else:
                logger.info("    ℹ️  No 'Vis alle' button found for div#%s", div_id)

        except Exception as e:
            logger.debug("    _click_vis_alle JS error: %s", e)

        return total

    # =========================================================================
    # CLICK NEXT PAGE — find toolbar in section siblings
    # =========================================================================

    def _click_next_page(self, div_id: str) -> bool:
        """
        Find and click the Next button using pure JS getElementById.
        Fully stale-proof — no WebElement passed to JS.
        Toolbar: table.x-toolbar-ct / tbody / tr[2] / td[3] / em / button
        """
        actual_id = None
        for id_try in (div_id, div_id + "s", div_id + "Base", div_id + "Bases",
                       "third" + div_id.capitalize()):
            try:
                self.driver.find_element(By.ID, id_try)
                actual_id = id_try
                break
            except NoSuchElementException:
                continue

        if actual_id is None:
            logger.info("    ⏹  No toolbar found for div#%s", div_id)
            return False

        try:
            result = self.driver.execute_script("""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return 'no_heading';
                var sib = heading.nextElementSibling;
                while (sib) {
                    if (sib.classList.contains('viewTitle')) break;
                    // Find toolbar table in this sibling
                    var toolbar = sib.matches('table.x-toolbar-ct') ? sib
                                : sib.querySelector('table.x-toolbar-ct');
                    if (toolbar) {
                        var rows = toolbar.querySelectorAll('tbody tr');
                        var btnRow = rows.length >= 2 ? rows[1] : rows[0];
                        if (!btnRow) return 'no_btn_row';
                        var tds = btnRow.querySelectorAll('td');
                        // td[3] = Next (0-indexed: td[2])
                        var nextTd = tds.length >= 3 ? tds[2] : null;
                        if (!nextTd) return 'no_next_td';
                        var cls = nextTd.className || '';
                        if (cls.indexOf('disabled') >= 0) return 'disabled';
                        var btn = nextTd.querySelector('button');
                        if (!btn) return 'no_btn';
                        if (btn.disabled) return 'disabled';
                        btn.click();
                        return 'clicked';
                    }
                    sib = sib.nextElementSibling;
                }
                return 'no_toolbar';
            """, actual_id)

            if result == 'clicked':
                logger.info("    ➡️  Next page clicked")
                time.sleep(2.0)
                return True
            elif result in ('disabled',):
                logger.info("    ⏹  Next is disabled — last page")
                return False
            else:
                logger.info("    ⏹  No Next button for div#%s (%s)", div_id, result)
                return False

        except Exception as e:
            logger.debug("    _click_next_page JS error: %s", e)
            return False

    # =========================================================================
    # COLLECT DOCUMENT LINKS — from x-grid-panel sibling of section heading
    # =========================================================================

    def _collect_links_in_section(self, div_id: str, seen: set) -> List[str]:
        """
        Collect searchResultLink hrefs using pure JS getElementById.
        Confirmed link class from DevTools: a.searchResultLink
        href format: #document/EUR/eur-2026-03-06
        Fully stale-proof.
        """
        new_urls = []

        actual_id = None
        for id_try in (div_id, div_id + "s", div_id + "Base", div_id + "Bases",
                       "third" + div_id.capitalize()):
            try:
                self.driver.find_element(By.ID, id_try)
                actual_id = id_try
                break
            except NoSuchElementException:
                continue

        if actual_id is None:
            return new_urls

        try:
            hrefs = self.driver.execute_script("""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return [];
                var hrefs = [];
                var sib = heading.nextElementSibling;
                while (sib) {
                    if (sib.classList.contains('viewTitle')) break;
                    // Collect searchResultLink anchors
                    var links = sib.querySelectorAll('a.searchResultLink, a[href*="#document/"]');
                    links.forEach(function(a) {
                        var h = (a.getAttribute('href') || '').split('?')[0].trim();
                        if (h) hrefs.push(h);
                    });
                    sib = sib.nextElementSibling;
                }
                return hrefs;
            """, actual_id)

            for href in (hrefs or []):
                if href and href not in seen:
                    seen.add(href)
                    new_urls.append(href)

        except Exception as e:
            logger.debug("    _collect_links_in_section JS error: %s", e)

        return new_urls

    # =========================================================================
    # MAIN ENTRY: collect all section URLs from the currently-loaded page
    # =========================================================================

    def collect_urls_from_current_view(self, max_pages: int = 500) -> List[dict]:
        """
        MUST be called AFTER a tree node has been clicked and page has loaded.
        Never call driver.get() before this — it destroys dynamic content.

        Processes ONE section fully before moving to the next.
        Verifies URL count matches expected count for each section.
        """
        if not self._wait_for_legal_area_header():
            logger.error("❌ Page not ready — cannot collect sections")
            return []

        section_links = self._get_section_links()
        if not section_links:
            logger.warning("    ⚠️  No section links found")
            return []

        logger.info(
            "\n    ════════════════════════════════════════════════\n"
            "    %s SECTIONS FOUND — processing one by one\n"
            "    ════════════════════════════════════════════════",
            len(section_links)
        )

        results = []

        for s_idx, (label, canonical, div_id) in enumerate(section_links, 1):

            logger.info(
                "\n    ── [%s/%s] %s ──",
                s_idx, len(section_links), canonical
            )

            seen: set            = set()
            section_urls: List[str] = []

            try:
                # ── 1. Re-find and click section tab FRESH (no stale refs) ──
                if not self._click_section_tab(label):
                    logger.warning("    ⚠️  Could not click tab '%s' — skipping", label)
                    results.append({
                        "document_type":  canonical,
                        "expected_count": None,
                        "urls":           [],
                    })
                    continue
                time.sleep(1.5)

                # ── 2. Verify heading div exists ──────────────────────
                try:
                    self.driver.find_element(By.ID, div_id)
                    logger.info("    ✅ Heading div#%s found", div_id)
                except NoSuchElementException:
                    logger.warning(
                        "    ⚠️  Heading div#%s not found — skipping", div_id
                    )
                    results.append({
                        "document_type":  canonical,
                        "expected_count": None,
                        "urls":           [],
                    })
                    continue

                # ── 3. Click Vis alle → get total count ───────────────
                expected = self._click_vis_alle(div_id)

                # ── 4. Collect all documents — paginate until done ────
                page_num = 0
                while page_num < max_pages:
                    before = len(seen)

                    new = self._collect_links_in_section(div_id, seen)
                    section_urls.extend(new)
                    after = len(seen)

                    logger.info(
                        "      page %s: +%s new  (total: %s / expected: %s)",
                        page_num + 1, after - before, after,
                        expected if expected else "?"
                    )

                    if expected and after >= expected:
                        logger.info(
                            "      ✅ All %s docs collected for '%s'",
                            expected, canonical
                        )
                        break

                    if not self._click_next_page(div_id):
                        break

                    time.sleep(0.5)

                    if len(seen) == before:
                        logger.info("      No new links after Next — stopping")
                        break

                    page_num += 1

                # ── 5. Count verification ─────────────────────────────
                if expected and len(section_urls) != expected:
                    logger.warning(
                        "    ⚠️  COUNT MISMATCH '%s': expected=%s  collected=%s",
                        canonical, expected, len(section_urls)
                    )
                else:
                    logger.info(
                        "    ✅ COMPLETE '%s'  expected=%s  collected=%s",
                        canonical, expected, len(section_urls)
                    )

                results.append({
                    "document_type":  canonical,
                    "expected_count": expected,
                    "urls":           section_urls,
                })

            except Exception as e:
                logger.error(
                    "    ❌ Section '%s' crashed: %s", canonical, e, exc_info=True
                )
                results.append({
                    "document_type":  canonical,
                    "expected_count": None,
                    "urls":           section_urls,
                })

        # Summary
        total_collected = sum(len(r["urls"]) for r in results)
        logger.info(
            "\n    ════════════════════════════════════════════════\n"
            "    SECTION COLLECTION COMPLETE\n"
            "    sections=%s  total_docs=%s\n"
            "    ════════════════════════════════════════════════",
            len(results), total_collected
        )

        return results

    # Backward-compat alias
    def collect_urls_by_section(self, area_url: str = "", max_pages: int = 500) -> List[dict]:
        """area_url is ignored — content is read from the current page (SPA)."""
        return self.collect_urls_from_current_view(max_pages=max_pages)

    # =========================================================================
    # CONTENT SCRAPING — visit individual document URL
    # =========================================================================

    def scrape_content_from_url(self, doc_url: str) -> dict:
        """
        Navigate to a document URL and extract content.

        Individual document pages use an iframe for rendering content.
        The hidden LovdataPro utility iframe (0×0 px) is skipped.
        We check iframe dimensions and only enter visible iframes.
        """
        result = {
            "title":          "",
            "date":           "",
            "content":        "",
            "content_source": "",
        }

        try:
            self.driver.switch_to.default_content()
            if doc_url.startswith("#"):
                doc_url = "https://lovdata.no/pro/" + doc_url

            self.driver.get(doc_url)
            try:
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except TimeoutException:
                return result
            time.sleep(3)

            best_content = ""
            best_title   = ""
            best_date    = ""
            best_source  = ""

            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            logger.debug("  iframes found: %s", len(iframes))

            for idx, iframe in enumerate(iframes):
                try:
                    # Skip hidden utility iframes (width:0 / height:0)
                    style = (iframe.get_attribute("style") or "").lower()
                    if "width: 0" in style or "height: 0" in style:
                        logger.debug("  ⏭️  Skip hidden iframe[%s]", idx)
                        continue

                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(iframe)
                    time.sleep(1.0)

                    # Title
                    title = ""
                    for sel in ["h1", ".tittel", "[class*='tittel']",
                                ".navn", "span.bold", "[class*='title']"]:
                        try:
                            t = self.driver.find_element(
                                By.CSS_SELECTOR, sel
                            ).text.strip()
                            if t and len(t) > 3:
                                title = t
                                break
                        except Exception:
                            continue

                    # Date
                    date = ""
                    for sel in [".dato", "[class*='dato']", ".ikraftdato",
                                ".kunngjort", "[class*='kunngjort']",
                                "[class*='date']", "time"]:
                        try:
                            el = self.driver.find_element(By.CSS_SELECTOR, sel)
                            d  = (
                                el.text or el.get_attribute("datetime") or ""
                            ).strip()
                            if d:
                                date = d
                                break
                        except Exception:
                            continue

                    # Content (priority order)
                    content = ""
                    source  = ""

                    for tag in ("pre", "code"):
                        try:
                            t = self.driver.find_element(
                                By.TAG_NAME, tag
                            ).text.strip()
                            if len(t) > 100:
                                content = t
                                source  = "xml_pre"
                                break
                        except Exception:
                            continue

                    if not content:
                        for sel in [
                            "div.lov-content", "div.lovtekst",
                            "div.paragraf",    "div.avsnitt",
                            "div#document",    "div.document",
                            "div[class*='lovtekst']",
                            "div[class*='dokument']",
                            "div[class*='document']",
                            "article", "main", "#content",
                        ]:
                            try:
                                t = self.driver.find_element(
                                    By.CSS_SELECTOR, sel
                                ).text.strip()
                                if len(t) > 200:
                                    content = t
                                    source  = "iframe_content"
                                    break
                            except Exception:
                                continue

                    if not content:
                        try:
                            t = self.driver.find_element(
                                By.TAG_NAME, "body"
                            ).text.strip()
                            if len(t) > 50:
                                content = t
                                source  = "iframe_body"
                        except Exception:
                            pass

                    if len(content) > len(best_content):
                        best_content = content
                        best_title   = title
                        best_date    = date
                        best_source  = source

                except Exception as e:
                    logger.debug("  iframe[%s] error: %s", idx, e)
                finally:
                    self.driver.switch_to.default_content()

            # Fallback: main window body
            if not best_content:
                try:
                    best_content = self.driver.find_element(
                        By.TAG_NAME, "body"
                    ).text.strip()
                    best_source = "body_text"
                except Exception:
                    pass

            result["title"]          = best_title
            result["date"]           = best_date
            result["content"]        = best_content
            result["content_source"] = best_source

            logger.info(
                "  📄 title='%s'  date='%s'  chars=%s  src=%s",
                (best_title or "—")[:60],
                best_date or "—",
                len(best_content),
                best_source,
            )

        except Exception as e:
            logger.error("❌ scrape_content_from_url [%s]: %s", doc_url, e)

        return result