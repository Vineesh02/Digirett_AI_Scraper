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

_LABEL_UPPER_TO_DEF = {d[0].upper(): d for d in SECTION_DEFS}
_DIV_ID_TO_DEF      = {d[2]: d       for d in SECTION_DEFS}

SECTION_MAP = {d[0].upper(): d[1] for d in SECTION_DEFS}
SECTION_MAP.update({d[1]: d[1] for d in SECTION_DEFS})


# ─────────────────────────────────────────────────────────────────────────────
# Document ID prefix → Lovdata path segment mapping
# Used to construct #document/ URLs from short IDs like FOR-2002-11-15-1288
# ─────────────────────────────────────────────────────────────────────────────
_DOC_PREFIX_MAP = {
    "LOV":  "NL/lov",
    "FOR":  "SF/forskrift",
    "AVT":  "NL/lov",        # treaty/agreement — fallback
    "HR":   "HR",
    "LB":   "LB",
    "LG":   "LG",
    "LE":   "LE",
    "LA":   "LA",
    "LF":   "LF",
    "RG":   "RG",
    "TOSLO": "TOSLO",
    "NAV":  "NAV",
    "KOFA": "KOFA",
    "JD":   "JD",
    "BFJR": "BFJR",
}


def _construct_doc_url(doc_ref: str) -> Optional[str]:
    """
    Construct a Lovdata #document/ URL from a short document reference.
    E.g. 'FOR-2002-11-15-1288' -> '#document/SF/forskrift/2002-11-15-1288'
         'LOV-1918-05-31-4'    -> '#document/NL/lov/1918-05-31-4'
    """
    if not doc_ref:
        return None
    doc_ref = doc_ref.strip()

    # Already a full URL or fragment
    if doc_ref.startswith("#document/") or doc_ref.startswith("http"):
        return doc_ref

    # Pattern: PREFIX-YYYY-MM-DD-NUM  or  PREFIX-YYYY-NUM
    m = re.match(r"^([A-ZÆØÅ]+)-(.+)$", doc_ref, re.IGNORECASE)
    if not m:
        return None

    prefix = m.group(1).upper()
    rest   = m.group(2)

    path = _DOC_PREFIX_MAP.get(prefix)
    if path:
        return f"#document/{path}/{rest}"

    # Unknown prefix — use lowercase prefix as best guess
    return f"#document/{prefix.lower()}/{rest}"


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

    # Advanced Search page selectors (confirmed from DevTools)
    _SEARCH_WIDGET_CSS      = "div.searchResultWidget"
    _RESULT_ITEM_CSS        = "a.placeHistoryItem"          # actual result links
    _RESULT_COUNT_CSS       = "span#resultInfoNumberOfHits font"
    _RESULT_COUNT_ALT_CSS   = "span.resultInfoValue#resultInfoNumberOfHits"

    def __init__(self, driver):
        self.driver = driver
        self.wait   = WebDriverWait(self.driver, config.TIMEOUT)

    # =========================================================================
    # LOGIN
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
    # PAGE TYPE DETECTION
    # =========================================================================

    def _is_advanced_search_page(self) -> bool:
        try:
            self.driver.find_element(By.CSS_SELECTOR, self._SEARCH_WIDGET_CSS)
            return True
        except NoSuchElementException:
            return False

    def _is_legal_area_page(self) -> bool:
        try:
            self.driver.find_element(By.CSS_SELECTOR, self._SECTION_HEADER_CSS)
            return True
        except NoSuchElementException:
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
    # TREE helpers
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
    # WAIT FOR LEGAL AREA HEADER
    # =========================================================================

    def _wait_for_legal_area_header(self, timeout: int = 15) -> bool:
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
    # SECTION LINKS — legal area header
    # =========================================================================

    def _get_section_links(self) -> List[Tuple[str, str, str]]:
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
                results.append((label_text, canonical, div_id))
                logger.info(
                    "    ✓ Section: '%s'  →  %s  (div#%s)",
                    label_text, canonical, div_id
                )

            except StaleElementReferenceException:
                continue

        return results

    def _click_section_tab(self, label_text: str) -> bool:
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
    # LEGAL AREA PAGE — small section helpers
    # =========================================================================

    def _collect_links_in_section(self, div_id: str, seen: set) -> List[str]:
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
    # VIS ALLE
    # =========================================================================

    def _click_vis_alle(self, div_id: str) -> Optional[int]:
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
            return None

        try:
            result = self.driver.execute_script("""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return null;
                var sib = heading.nextElementSibling;
                while (sib) {
                    if (sib.classList.contains('viewTitle')) break;
                    var txt = sib.innerText || sib.textContent || '';
                    if (txt.toLowerCase().indexOf('vis alle') >= 0) {
                        var m = txt.match(/[(]([0-9][0-9 ]*)[)]/);
                        if (!m) m = txt.toLowerCase().match(/vis alle [0-9]+/);
                        var count = m ? parseInt(m[0].replace(/[^0-9]/g,'')) : null;
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
                time.sleep(3.0)
                return total
            else:
                logger.info("    ℹ️  No 'Vis alle' button found for div#%s", div_id)
                return None

        except Exception as e:
            logger.debug("    _click_vis_alle JS error: %s", e)
            return None

    # =========================================================================
    # ADVANCED SEARCH PAGE
    # =========================================================================

    def _wait_for_advanced_search(self, timeout: int = 15) -> bool:
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, self._SEARCH_WIDGET_CSS)
                )
            )
            time.sleep(1.5)
            logger.info("    ✅ Advanced Search page loaded")
            return True
        except TimeoutException:
            logger.warning("    ⚠️  Advanced Search page did not load in %ss", timeout)
            return False

    def _get_advanced_search_total(self) -> Optional[int]:
        for css in (self._RESULT_COUNT_CSS, self._RESULT_COUNT_ALT_CSS):
            try:
                el = self.driver.find_element(By.CSS_SELECTOR, css)
                text = (el.text or "").strip().replace(" ", "").replace("\xa0", "")
                if text.isdigit():
                    total = int(text)
                    logger.info("    📊 Advanced Search total: %s", total)
                    return total
            except NoSuchElementException:
                continue

        try:
            container = self.driver.find_element(
                By.CSS_SELECTOR, "div.gwt-HTML.numberOfHits"
            )
            text = (container.text or "").strip()
            m = re.search(r"(\d[\d\s]+)", text)
            if m:
                total = int(m.group(1).replace(" ", ""))
                logger.info("    📊 Advanced Search total (fallback): %s", total)
                return total
        except Exception:
            pass

        logger.warning("    ⚠️  Could not read Advanced Search total count")
        return None

    # =========================================================================
    # ADVANCED SEARCH — collect URLs from placeHistoryItem links
    # =========================================================================

    def _collect_advanced_search_urls(
        self, expected: Optional[int], max_pages: int = 500
    ) -> List[str]:
        """
        Collect all document URLs from Advanced Search results.

        The result links are <a class="placeHistoryItem"> elements.
        They have NO href — navigation is GWT internal.
        The document reference (e.g. FOR-2002-11-15-1288) is on the
        second line of the link text.

        Strategy:
          1. Read all placeHistoryItem elements on the current page
          2. Extract doc reference from text (line 2)
          3. Construct #document/ URL via _construct_doc_url()
          4. Click Next button to go to next results page
          5. Repeat until all pages collected
        """
        all_urls: List[str] = []
        seen:     set       = set()

        # Read true total from page
        page_total = self._get_advanced_search_total()
        if page_total is not None:
            expected = page_total

        # Save search result page URL for fallback navigation
        search_url = self.driver.current_url

        page_num = 0
        while page_num < max_pages:
            # ── Collect items on current page ─────────────────────────
            new_urls = self._extract_urls_from_result_items(seen)
            all_urls.extend(new_urls)

            logger.info(
                "      page %s: +%s new  (total: %s / expected: %s)",
                page_num + 1, len(new_urls), len(all_urls),
                expected if expected else "?"
            )

            if expected and len(all_urls) >= expected:
                logger.info("      ✅ All %s docs collected", expected)
                break

            if not new_urls and page_num > 0:
                logger.info("      ⏹  No new URLs on page %s — stopping", page_num + 1)
                break

            # ── Try to click Next ─────────────────────────────────────
            next_result = self._click_next_advanced_search()
            if next_result == "clicked":
                time.sleep(2.5)
                page_num += 1
            else:
                logger.info("      ⏹  No more pages (%s)", next_result)
                break

        return all_urls

    def _extract_urls_from_result_items(self, seen: set) -> List[str]:
        """
        Extract document URLs from placeHistoryItem elements on current page.

        Each real result item has 2 lines of text:
            Line 1: document title  (e.g. "Regulations on the Appeals Board for...")
            Line 2: doc ref OR source name  (e.g. "FOR-2002-11-15-1288" or "Center for European Law")

        Breadcrumb items have only 1 line ("My page", "Home", "Procurement", etc.)
        and are skipped.

        For items whose last line matches PREFIX-YYYY-... we construct the URL
        directly (fast, no click needed).

        For items whose last line does NOT match (e.g. articles with publisher name)
        we click the item, capture window.location.hash, then navigate back to the
        search results page.
        """
        # Save the current search results URL so we can return after clicking
        search_url = self.driver.current_url

        new_urls = []
        try:
            # Collect all item texts + indices first (avoids stale refs after click/back)
            item_data = self.driver.execute_script("""
                var items = document.querySelectorAll('a.placeHistoryItem');
                var result = [];
                items.forEach(function(a, i) {
                    result.push({
                        index: i,
                        text: a.innerText || ''
                    });
                });
                return result;
            """)

            if not item_data:
                return new_urls

            # Identify which indices are real docs (2+ lines) vs breadcrumbs (1 line)
            doc_items = []
            for item in item_data:
                lines = [l.strip() for l in item["text"].split("\n") if l.strip()]
                if len(lines) < 2:
                    continue  # breadcrumb — skip
                last_line = lines[-1]
                doc_items.append({
                    "index":     item["index"],
                    "last_line": last_line,
                    "title":     lines[0],
                })

            logger.debug("      %s real doc items on page", len(doc_items))

            for doc in doc_items:
                last_line = doc["last_line"]

                # ── Fast path: standard doc ref (PREFIX-YYYY-...) ────────
                if re.match(r"^[A-ZÆØÅ]+-\d{4}", last_line, re.IGNORECASE):
                    url = _construct_doc_url(last_line)
                    if url and url not in seen:
                        seen.add(url)
                        new_urls.append(url)
                        logger.debug("      + (fast) %s", url)
                    continue

                # ── Slow path: click item, capture hash, go back ─────────
                try:
                    # Re-find items fresh (DOM may have changed)
                    items_fresh = self.driver.find_elements(
                        By.CSS_SELECTOR, self._RESULT_ITEM_CSS
                    )
                    if doc["index"] >= len(items_fresh):
                        logger.debug(
                            "      ⚠️  item index %s out of range", doc["index"]
                        )
                        continue

                    target = items_fresh[doc["index"]]
                    self.driver.execute_script("arguments[0].click();", target)
                    time.sleep(1.5)

                    # Capture the hash — gives us #document/EUR/eur-2026-03-06 etc.
                    fragment = self.driver.execute_script(
                        "return window.location.hash;"
                    ) or ""
                    fragment = fragment.strip()

                    if fragment and fragment.startswith("#document/"):
                        if fragment not in seen:
                            seen.add(fragment)
                            new_urls.append(fragment)
                            logger.debug(
                                "      + (click) %s  [%s]",
                                fragment, doc["title"][:50]
                            )
                    else:
                        logger.debug(
                            "      ⚠️  No #document/ hash after clicking '%s': got '%s'",
                            doc["title"][:50], fragment
                        )

                    # Navigate back to search results
                    self.driver.get(search_url)
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, self._SEARCH_WIDGET_CSS)
                            )
                        )
                        time.sleep(1.0)
                    except TimeoutException:
                        logger.warning("      ⚠️  Search page did not reload after back")
                        break

                except StaleElementReferenceException:
                    # Try to recover by reloading search page
                    self.driver.get(search_url)
                    time.sleep(2.0)
                    continue
                except Exception as e:
                    logger.debug("      click-capture error: %s", e)
                    try:
                        self.driver.get(search_url)
                        time.sleep(2.0)
                    except Exception:
                        pass
                    continue

        except Exception as e:
            logger.debug("    _extract_urls_from_result_items error: %s", e)

        return new_urls

    def _click_next_advanced_search(self) -> str:
        """
        Click the Next button on the Advanced Search results page.
        Structure confirmed from DevTools:
          td.x-btn-mc > em > button.x-btn-text
        The Next button is the last non-disabled x-btn-mc in the toolbar.
        """
        try:
            result = self.driver.execute_script("""
                var widget = document.querySelector('div.searchResultWidget');
                if (!widget) return 'no_widget';

                var toolbars = widget.querySelectorAll('table.x-toolbar-ct');
                if (!toolbars.length) {
                    toolbars = document.querySelectorAll('table.x-toolbar-ct');
                }
                if (!toolbars.length) return 'no_toolbar';

                for (var t = 0; t < toolbars.length; t++) {
                    var toolbar = toolbars[t];
                    var rows = toolbar.querySelectorAll('tbody tr');
                    var btnRow = rows.length >= 2 ? rows[1] : rows[0];
                    if (!btnRow) continue;

                    var btnCells = btnRow.querySelectorAll('td.x-btn-mc');
                    if (!btnCells.length) continue;

                    // Next button = last non-disabled x-btn-mc
                    for (var i = btnCells.length - 1; i >= 0; i--) {
                        var cell = btnCells[i];
                        var btn = cell.querySelector('button.x-btn-text');
                        if (!btn) continue;

                        var parentTd = cell.parentElement;
                        var tdCls = (parentTd ? parentTd.className : '') || '';
                        if (tdCls.indexOf('disabled') >= 0) continue;
                        if (btn.disabled) continue;
                        if (btn.getAttribute('aria-disabled') === 'true') continue;
                        if (i === 0 && btnCells.length > 1) continue;

                        btn.click();
                        return 'clicked';
                    }
                }
                return 'no_next';
            """)
            return result or "no_next"
        except Exception as e:
            logger.debug("    _click_next_advanced_search error: %s", e)
            return "error"

    # =========================================================================
    # NAVIGATE BACK TO LEGAL AREA PAGE
    # =========================================================================

    def _back_to_legal_area(self, legal_area_url: str) -> bool:
        try:
            self.driver.back()
            time.sleep(2.0)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, self._SECTION_HEADER_CSS)
                    )
                )
                logger.info("    ↩️  Back to legal area page — tabs restored")
                return True
            except TimeoutException:
                pass

            logger.warning("    ⚠️  Back() failed — navigating directly to legal area")
            self.driver.get(legal_area_url)
            time.sleep(3.0)
            return self._is_legal_area_page()

        except Exception as e:
            logger.error("    ❌ _back_to_legal_area failed: %s", e)
            return False

    # =========================================================================
    # SMALL SECTION — no Vis alle
    # =========================================================================

    def _collect_small_section(self, div_id: str) -> Tuple[List[str], Optional[int]]:
        seen: set = set()
        urls = self._collect_links_in_section(div_id, seen)
        logger.info(
            "    ✅ Small section div#%s — collected %s URLs", div_id, len(urls)
        )
        return urls, None

    # =========================================================================
    # MAIN ENTRY: collect all section URLs from the currently-loaded page
    # =========================================================================

    def collect_urls_from_current_view(self, max_pages: int = 500) -> List[dict]:
        """
        MUST be called AFTER a tree node has been clicked and page has loaded.
        Never call driver.get() before this — it destroys dynamic content.
        """
        if not self._wait_for_legal_area_header():
            logger.error("❌ Page not ready — cannot collect sections")
            return []

        legal_area_url = self.driver.current_url

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

            section_urls: List[str] = []
            expected:     Optional[int] = None

            try:
                # ── STEP 1: Ensure we are on the legal area page ──────
                if not self._is_legal_area_page():
                    logger.info("    ↩️  Not on legal area page — navigating back")
                    if not self._back_to_legal_area(legal_area_url):
                        logger.error(
                            "    ❌ Could not return to legal area — skipping '%s'",
                            canonical
                        )
                        results.append({
                            "document_type":  canonical,
                            "expected_count": None,
                            "urls":           [],
                        })
                        continue
                    time.sleep(1.0)

                # ── STEP 2: Click the section tab ─────────────────────
                if not self._click_section_tab(label):
                    logger.warning("    ⚠️  Could not click tab '%s' — skipping", label)
                    results.append({
                        "document_type":  canonical,
                        "expected_count": None,
                        "urls":           [],
                    })
                    continue
                time.sleep(1.5)

                # ── STEP 3: Verify heading div exists ─────────────────
                heading_found = False
                for id_try in (div_id, div_id + "s", div_id + "Base",
                               div_id + "Bases", "third" + div_id.capitalize()):
                    try:
                        self.driver.find_element(By.ID, id_try)
                        heading_found = True
                        logger.info("    ✅ Heading div#%s found", id_try)
                        break
                    except NoSuchElementException:
                        continue

                if not heading_found:
                    logger.warning(
                        "    ⚠️  Heading div#%s not found — skipping", div_id
                    )
                    results.append({
                        "document_type":  canonical,
                        "expected_count": None,
                        "urls":           [],
                    })
                    continue

                # ── STEP 4: Check for Vis alle button ─────────────────
                vis_alle_count = self._click_vis_alle(div_id)

                if vis_alle_count is not None or self._is_advanced_search_page():
                    expected = vis_alle_count

                    if not self._wait_for_advanced_search(timeout=15):
                        logger.warning(
                            "    ⚠️  Advanced Search did not load for '%s'", canonical
                        )
                        self._back_to_legal_area(legal_area_url)
                        results.append({
                            "document_type":  canonical,
                            "expected_count": expected,
                            "urls":           [],
                        })
                        continue

                    logger.info("    🔍 Advanced Search mode — collecting all pages")
                    section_urls = self._collect_advanced_search_urls(
                        expected=expected,
                        max_pages=max_pages,
                    )

                    logger.info("    ↩️  Returning to legal area page")
                    if not self._back_to_legal_area(legal_area_url):
                        logger.warning(
                            "    ⚠️  Back failed — re-navigating to legal area URL"
                        )
                        self.driver.get(legal_area_url)
                        time.sleep(3.0)

                else:
                    logger.info("    📋 Small section mode — collecting directly")
                    section_urls, expected = self._collect_small_section(div_id)

                # ── STEP 5: Verify count ──────────────────────────────
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
                try:
                    if not self._is_legal_area_page():
                        self._back_to_legal_area(legal_area_url)
                except Exception:
                    pass

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
        return self.collect_urls_from_current_view(max_pages=max_pages)

    # =========================================================================
    # CONTENT SCRAPING — visit individual document URL
    # =========================================================================

    def scrape_content_from_url(self, doc_url: str) -> dict:
        """
        Navigate to a document URL and extract content.
        Returns dict matching the XML format:
          title, date, content, content_source
        """
        result = {
            "title":          "",
            "date":           "",
            "content":        "",
            "content_source": "",
        }

        try:
            self.driver.switch_to.default_content()

            # Normalise URL
            if doc_url.startswith("#"):
                doc_url = "https://lovdata.no/pro/" + doc_url
            elif not doc_url.startswith("http"):
                doc_url = "https://lovdata.no/pro/" + doc_url.lstrip("/")

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
                    style = (iframe.get_attribute("style") or "").lower()
                    if "width: 0" in style or "height: 0" in style:
                        continue

                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(iframe)
                    time.sleep(1.0)

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