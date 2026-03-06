"""
LOVDATA PRO SCRAPER
===================
URL collection uses two-mode operation (confirmed working):
  - Small sections (no Vis alle): collect links directly from legal-area page siblings
  - Large sections (Vis alle): click → driver.get(result_url) → Advanced Search → paginate → back()

scrape_content_from_url extracts full content + page metadata from individual document iframes.
"""

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
# Section definitions:  (norwegian_label, canonical_english, stable_div_id)
# ─────────────────────────────────────────────────────────────────────────────
SECTION_DEFS = [
    ("Siste dokumenter",                                         "LATEST DOCUMENTS",                                       "saker"),
    ("Lover",                                                    "LAWS",                                                   "lover"),
    ("Forskrifter",                                              "REGULATIONS",                                            "forskrifter"),
    ("Avgjørelser fra Høyesterett",                              "DECISIONS FROM THE SUPREME COURT",                       "hr"),
    ("Avgjørelser fra lagmannsrettene",                          "DECISIONS FROM THE COURTS OF APPEAL",                    "lr"),
    ("Avgjørelser fra tingrettene",                              "DECISIONS FROM THE DISTRICT COURTS",                     "tr"),
    ("Artikler",                                                 "ARTICLES",                                               "artikler"),
    ("Dokumenter fra Klagenemnda for offentlige anskaffelser",   "DOCUMENTS FROM THE PUBLIC PROCUREMENT COMPLAINTS BOARD", "firstOtherBase"),
    ("Dokumenter fra Byggebransjens Faglig Juridiske Råd",       "DOCUMENTS FROM THE CONSTRUCTION INDUSTRY LEGAL COUNCIL", "secondOtherBase"),
    ("Dokumenter fra Justisdepartementet",                       "DOCUMENTS FROM THE MINISTRY OF JUSTICE",                 "thirdOtherBase"),
    ("Andre dokumenter",                                         "OTHER DOCUMENTS",                                        "otherBases"),
]
_LABEL_UPPER_TO_DEF = {d[0].upper(): d for d in SECTION_DEFS}
_DIV_ID_TO_DEF      = {d[2]: d       for d in SECTION_DEFS}

# Kept for backward-compat with any callers that import _LABEL_TO_DEF
_LABEL_TO_DEF: Dict[str, tuple] = {d[0].upper(): d for d in SECTION_DEFS}

# Minimum characters for content to be considered real document text.
_MIN_CONTENT_CHARS = 300

# CSS selectors for Lovdata document content, ordered by specificity.
# #documentBody is confirmed correct from DevTools screenshot.
_CONTENT_SELECTORS = [
    "#documentBody",                  # ← confirmed by DevTools (forarbeid, artikler, etc.)
    "div#documentBody",
    "#lovdataDocument",               # wrapper around documentBody
    "#maincolOneColumn",              # outer column, last resort before body
    "div.lov-content",
    "div.lovtekst",
    "div.paragraf",
    "div.avsnitt",
    "div#document",
    "div.document",
    "div[class*='lovtekst']",
    "div[class*='dokument']",
    "div[class*='document']",
    "article",
    "main",
    "#content",
]


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
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


def _strip_metadata_header(content: str) -> str:
    """
    Strip the metadata table text from the top of extracted content.

    Lovdata documents begin with a metadata block (Dato, Departement, etc.)
    before the actual legal text. We find the first line that looks like
    real document content and discard everything before it.

    Handles all document types:
      - Lover/Forskrifter: start with '§ 1'
      - Forarbeid/Proposisjoner: start with numbered headings '1. Sammendrag'
      - Court decisions: start with 'TOSL-', 'HR-', 'LG-', or 'Saken gjelder'
      - 'Til Stortinget' / 'Til Kongen' preambles
    """
    if not content:
        return content

    lines = content.split('\n')
    first_content_idx = None

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        # § paragraph marker (lover, forskrifter)
        if stripped.startswith('§'):
            first_content_idx = i
            break
        # Preamble lines (proposisjoner, meldinger)
        if stripped.startswith('Til Stortinget') or stripped.startswith('Til Kongen'):
            first_content_idx = i
            break
        # Numbered chapter headings like "1. Sammendrag" or "1.1 Bakgrunn"
        if re.match(r'^\d+[\.\d]*\s+\S', stripped) and len(stripped) > 10:
            first_content_idx = i
            break
        # Court decision body text patterns
        if re.match(r'^(Saken gjelder|Ankende part|Saksforhold|A\s+anfører)', stripped):
            first_content_idx = i
            break

    if first_content_idx is not None and first_content_idx > 0:
        return '\n'.join(lines[first_content_idx:]).strip()

    return content


# ─────────────────────────────────────────────────────────────────────────────
class LovdataScraper:

    _NODE_TEXT_CSS      = "span.x-tree3-node-text"
    _EC_ICON_CSS        = "img.x-tree3-ec-icon"
    _SECTION_HEADER_CSS = "div.legal-area-header"
    _SECTION_LINK_CSS   = "a.gwt-Anchor"
    _SECTION_LABEL_CSS  = "span.label"

    # Advanced Search page selectors
    _SEARCH_WIDGET_CSS    = "div.searchResultWidget"
    _RESULT_COUNT_CSS     = "span#resultInfoNumberOfHits font"
    _RESULT_COUNT_ALT_CSS = "span.resultInfoValue#resultInfoNumberOfHits"

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
            logger.info("Login successful")
            return True
        except Exception as e:
            logger.error("Login failed: %s", e)
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
            logger.info("Tree nodes loaded")
        except TimeoutException:
            logger.warning("Tree nodes not found within timeout")
        time.sleep(1)

    # =========================================================================
    # TREE HELPERS
    # =========================================================================

    def discover_legal_area_links(self) -> Dict[str, dict]:
        nodes = self.driver.find_elements(By.CSS_SELECTOR, self._NODE_TEXT_CSS)
        logger.info("Found %s tree nodes", len(nodes))
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
        logger.info("Root categories: %s", len(roots))
        return roots

    def _get_node_div(self, node_text_el):
        try:
            return node_text_el.find_element(
                By.XPATH, "./ancestor::div[contains(@class,'x-tree3-node')][1]"
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
                EC.presence_of_element_located((By.CSS_SELECTOR, self._SECTION_HEADER_CSS))
            )
            time.sleep(0.5)
            return True
        except TimeoutException:
            pass
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "saker"))
            )
            time.sleep(0.5)
            return True
        except TimeoutException:
            logger.warning("Section header not found after %ss", timeout)
            return False

    # =========================================================================
    # SECTION DETECTION
    # =========================================================================

    def _get_section_links(self) -> List[Tuple[str, str, str]]:
        results = []
        seen_canonical: set = set()
        try:
            header = self.driver.find_element(By.CSS_SELECTOR, self._SECTION_HEADER_CSS)
        except NoSuchElementException:
            logger.error("div.legal-area-header not found")
            return results

        links = header.find_elements(By.CSS_SELECTOR, self._SECTION_LINK_CSS)
        logger.info("  Section tab links found: %s", len(links))

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
                    logger.debug("  No mapping for label: '%s'", label_text)
                    continue

                canonical = defn[1]
                div_id    = defn[2]

                if canonical in seen_canonical:
                    continue
                seen_canonical.add(canonical)

                results.append((label_text, canonical, div_id))
                logger.info("  Section: %-55s  div_id=%s", canonical, div_id)

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
                        logger.info("  Clicked section tab: '%s'", label_text)
                        return True
                except StaleElementReferenceException:
                    continue
        except Exception as e:
            logger.debug("_click_section_tab error: %s", e)
        logger.warning("  Could not find section tab: '%s'", label_text)
        return False

    # =========================================================================
    # SECTION ID RESOLUTION
    # =========================================================================

    def _resolve_div_id(self, div_id: str) -> Optional[str]:
        for id_try in (div_id, div_id + "s", div_id + "Base",
                       div_id + "Bases", "third" + div_id.capitalize()):
            try:
                self.driver.find_element(By.ID, id_try)
                return id_try
            except NoSuchElementException:
                continue
        return None

    # =========================================================================
    # VIS ALLE
    # =========================================================================

    def _click_vis_alle(self, div_id: str) -> Optional[int]:
        actual_id = self._resolve_div_id(div_id)
        if actual_id is None:
            logger.debug("  Section div#%s not found — no Vis alle", div_id)
            return None

        try:
            result = self.driver.execute_script(r"""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return null;
                var sib = heading.nextElementSibling;
                while (sib) {
                    if (sib.classList.contains('viewTitle')) break;
                    var txt = (sib.innerText || sib.textContent || '').trim();
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

            if result is None:
                logger.debug("  No Vis alle button for div#%s", div_id)
                return None

            total = int(result)
            logger.info("  Vis alle clicked — expected: %s", total)

            time.sleep(3.0)
            try:
                WebDriverWait(self.driver, 20).until(
                    lambda d: "result" in d.current_url
                )
            except TimeoutException:
                pass

            result_url = self.driver.current_url
            logger.info("  Advanced Search URL: %s", result_url)
            self.driver.get(result_url)

            try:
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, self._SEARCH_WIDGET_CSS)
                    )
                )
                time.sleep(2.0)
                logger.info("  Advanced Search loaded")
            except TimeoutException:
                logger.warning("  Advanced Search widget not found after driver.get()")

            return total

        except Exception as e:
            logger.debug("_click_vis_alle error [div#%s]: %s", div_id, e)
            return None

    # =========================================================================
    # ADVANCED SEARCH — collect all pages
    # =========================================================================

    def _get_advanced_search_total(self) -> Optional[int]:
        try:
            result = self.driver.execute_script("""
                var patterns = [
                    /Number of documents found[:\\s]+(\\d[\\d\\s]*)/i,
                    /Antall dokumenter[:\\s]+(\\d[\\d\\s]*)/i,
                    /Antall treff[:\\s]+(\\d[\\d\\s]*)/i,
                    /dokumenter funnet[:\\s]+(\\d[\\d\\s]*)/i,
                    /Fant\\s+(\\d[\\d\\s]*)/i,
                ];
                var walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT, null, false
                );
                var node;
                while ((node = walker.nextNode())) {
                    var t = (node.nodeValue || '').trim();
                    if (!t) continue;
                    for (var i = 0; i < patterns.length; i++) {
                        var m = t.match(patterns[i]);
                        if (m) return m[1].replace(/\\s/g, '');
                    }
                }
                return null;
            """)
            if result:
                total = int(result)
                logger.info("  Advanced Search total: %s", total)
                return total
        except Exception:
            pass

        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text or ""
            for pattern in (
                r"Number of documents found[:\s]+(\d[\d\s]*)",
                r"Antall dokumenter[:\s]+(\d[\d\s]*)",
                r"Antall treff[:\s]+(\d[\d\s]*)",
                r"dokumenter funnet[:\s]+(\d[\d\s]*)",
                r"(\d[\d\s]+)\s+dokument",
            ):
                m = re.search(pattern, body_text, re.IGNORECASE)
                if m:
                    total = int(m.group(1).replace(" ", "").replace("\xa0", ""))
                    logger.info("  Advanced Search total: %s", total)
                    return total
        except Exception:
            pass

        candidates = []
        for css in (
            "div.gwt-HTML.numberOfHits",
            "span.numberOfHits",
            "[class*='numberOfHits']",
            "[class*='resultCount']",
            "[class*='hitCount']",
            "div.searchResultInfo",
        ):
            try:
                el   = self.driver.find_element(By.CSS_SELECTOR, css)
                text = (el.text or "").strip().replace("\xa0", " ")
                nums = [int(n.replace(" ", "")) for n in re.findall(r"\d[\d ]*", text)]
                if nums:
                    candidates.extend(nums)
            except NoSuchElementException:
                continue
            except Exception:
                continue

        if candidates:
            total = max(candidates)
            logger.info("  Advanced Search total: %s (CSS fallback)", total)
            return total

        logger.warning("  Could not read Advanced Search total count")
        return None

    def _collect_advanced_search_urls(
        self, expected: Optional[int], max_pages: int = 500
    ) -> List[str]:
        all_urls: List[str] = []
        seen:     set       = set()
        page = 0

        while True:
            page += 1
            if page > max_pages:
                break

            try:
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, self._SEARCH_WIDGET_CSS)
                    )
                )
                time.sleep(2.0)
            except TimeoutException:
                logger.error("  Search widget missing on page %s — stopping", page)
                break

            hrefs = self.driver.execute_script("""
                var links = document.querySelectorAll(
                    'a.searchResultLink, a[href*="#document/"]'
                );
                var out = [];
                links.forEach(function(a) {
                    var h = (a.getAttribute('href') || '').split('?')[0].trim();
                    if (h) out.push(h);
                });
                return out;
            """) or []

            new_this_page = 0
            for h in hrefs:
                if h not in seen:
                    seen.add(h)
                    if h.startswith("#"):
                        h = "https://lovdata.no/pro/" + h.lstrip("/")
                    elif h.startswith("/"):
                        h = "https://lovdata.no" + h
                    all_urls.append(h)
                    new_this_page += 1

            logger.info(
                "  Page %s: +%s new  (total: %s / expected: %s)",
                page, new_this_page, len(all_urls),
                expected if expected is not None else "?"
            )

            if new_this_page == 0 and page > 1:
                logger.info("  No new URLs on page %s — last page", page)
                break

            next_result = self._click_next_advanced_search()
            if next_result != "clicked":
                logger.info("  No more pages (%s) — stopping", next_result)
                break

            time.sleep(3.0)

        return all_urls

    def _click_next_advanced_search(self) -> str:
        xpath_variants = [
            (
                "/html/body/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div"
                "/table/tbody/tr/td[1]/table/tbody/tr/td[8]/table/tbody/tr[2]"
                "/td[2]/em/button"
            ),
            (
                "/html/body/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div"
                "/table/tbody/tr/td[1]/table/tbody/tr/td[9]/table/tbody/tr[2]"
                "/td[2]/em/button"
            ),
            (
                "/html/body/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div"
                "/table/tbody/tr/td[1]/table/tbody/tr/td[10]/table/tbody/tr[2]"
                "/td[2]/em/button"
            ),
            (
                "/html/body/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]"
                "/table/tbody/tr/td[1]/table/tbody/tr/td[8]/table/tbody/tr[2]"
                "/td[2]/em/button"
            ),
        ]
        for xpath in xpath_variants:
            try:
                btn = WebDriverWait(self.driver, 20).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                btn.click()
                logger.info("  Next page clicked")
                return "clicked"
            except (TimeoutException, Exception):
                continue

        logger.warning("  Next button not found via any XPath variant")
        return "no_next"

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
                logger.info("  Back to legal area page")
                return True
            except TimeoutException:
                pass
            logger.warning("  Back() failed — navigating directly")
            self.driver.get(legal_area_url)
            time.sleep(3.0)
            return self._is_legal_area_page()
        except Exception as e:
            logger.error("_back_to_legal_area failed: %s", e)
            return False

    # =========================================================================
    # SMALL SECTION
    # =========================================================================

    def _collect_small_section(self, div_id: str) -> Tuple[List[str], None]:
        seen: set = set()
        actual_id = self._resolve_div_id(div_id)
        if actual_id is None:
            return [], None
        try:
            hrefs = self.driver.execute_script("""
                var heading = document.getElementById(arguments[0]);
                if (!heading) return [];
                var hrefs = [];
                var sib = heading.nextElementSibling;
                while (sib) {
                    if (sib.classList.contains('viewTitle')) break;
                    var links = sib.querySelectorAll(
                        'a.searchResultLink, a[href*="#document/"]'
                    );
                    links.forEach(function(a) {
                        var h = (a.getAttribute('href') || '').split('?')[0].trim();
                        if (h) hrefs.push(h);
                    });
                    sib = sib.nextElementSibling;
                }
                return hrefs;
            """, actual_id)

            urls = []
            for h in (hrefs or []):
                if h not in seen:
                    seen.add(h)
                    if h.startswith("#"):
                        h = "https://lovdata.no/pro/" + h.lstrip("/")
                    elif h.startswith("/"):
                        h = "https://lovdata.no" + h
                    urls.append(h)

            logger.info("  Small section div#%s — collected %s URLs", actual_id, len(urls))
            return urls, None
        except Exception as e:
            logger.debug("_collect_small_section error [div#%s]: %s", div_id, e)
            return [], None

    # =========================================================================
    # MAIN ENTRY: collect all section URLs
    # =========================================================================

    def collect_urls_from_current_view(self, max_pages: int = 500) -> List[dict]:
        if not self._wait_for_legal_area_header():
            logger.error("Page not ready — cannot collect sections")
            return []

        legal_area_url = self.driver.current_url
        section_links  = self._get_section_links()

        if not section_links:
            logger.warning("No section links found")
            return []

        logger.info("Processing %s sections", len(section_links))
        results = []

        for s_idx, (label, canonical, div_id) in enumerate(section_links, 1):
            logger.info("[%s/%s] Section: %s", s_idx, len(section_links), canonical)
            section_urls: List[str] = []
            expected:     Optional[int] = None

            try:
                if not self._is_legal_area_page():
                    logger.info("  Not on legal area page — navigating back")
                    if not self._back_to_legal_area(legal_area_url):
                        logger.error("  Could not return to legal area — skipping '%s'", canonical)
                        results.append({"document_type": canonical, "expected_count": None, "urls": []})
                        continue
                    time.sleep(1.0)

                if not self._click_section_tab(label):
                    logger.warning("  Could not click tab '%s' — skipping", label)
                    results.append({"document_type": canonical, "expected_count": None, "urls": []})
                    continue
                time.sleep(1.5)

                actual_id = self._resolve_div_id(div_id)
                if actual_id is None:
                    logger.warning("  Heading div#%s not found — skipping", div_id)
                    results.append({"document_type": canonical, "expected_count": None, "urls": []})
                    continue

                vis_alle_count = self._click_vis_alle(div_id)

                if vis_alle_count is not None or self._is_advanced_search_page():
                    expected = vis_alle_count

                    if not self._is_advanced_search_page():
                        logger.warning("  Advanced Search not loaded for '%s'", canonical)
                        self._back_to_legal_area(legal_area_url)
                        results.append({"document_type": canonical, "expected_count": expected, "urls": []})
                        continue

                    logger.info("  Advanced Search mode")
                    section_urls = self._collect_advanced_search_urls(
                        expected=expected, max_pages=max_pages
                    )

                    if not self._back_to_legal_area(legal_area_url):
                        logger.warning("  Back failed — re-navigating to: %s", legal_area_url)
                        self.driver.get(legal_area_url)
                        time.sleep(3.0)

                else:
                    logger.info("  Small section mode")
                    section_urls, expected = self._collect_small_section(div_id)

                if expected and len(section_urls) < expected:
                    logger.warning(
                        "  Count mismatch '%s': expected=%s  collected=%s",
                        canonical, expected, len(section_urls)
                    )
                else:
                    logger.info(
                        "  Section result: %-50s expected=%-6s collected=%s",
                        canonical, expected if expected is not None else "?", len(section_urls)
                    )

                results.append({
                    "document_type":  canonical,
                    "expected_count": expected,
                    "urls":           section_urls,
                })

            except Exception as e:
                logger.error("Section '%s' crashed: %s", canonical, e, exc_info=True)
                results.append({"document_type": canonical, "expected_count": None, "urls": section_urls})
                try:
                    if not self._is_legal_area_page():
                        self._back_to_legal_area(legal_area_url)
                except Exception:
                    pass

        total_collected = sum(len(r["urls"]) for r in results)
        logger.info("\nSection collection summary:")
        for r in results:
            logger.info(
                "  %-55s expected=%-6s found=%s",
                r["document_type"],
                r["expected_count"] if r["expected_count"] is not None else "?",
                len(r["urls"]),
            )
        logger.info("Total URLs collected: %s\n", total_collected)

        return results

    # =========================================================================
    # IFRAME CONTENT WAIT
    # =========================================================================

    def _wait_for_iframe_content(self, timeout: int = 20) -> None:
        """
        Wait until the iframe body has loaded real document content.

        FIX: The old condition only checked for '§' symbols or len>3000.
        This fails for forarbeid/proposisjoner/artikler which use numbered
        headings ('1. Sammendrag') with no § at all, causing premature
        timeout and 0-char reads.

        New strategy (any condition satisfies):
          1. Body text has ≥3 § symbols        (lover, forskrifter)
          2. Body text length > 3000            (any long document)
          3. #documentBody element exists and has >500 chars  (forarbeid, artikler)
          4. #lovdataDocument element exists and has >500 chars
        """
        try:
            def body_has_content(d):
                try:
                    body_text = d.find_element(By.TAG_NAME, "body").text
                    # Condition 1: paragraph law markers
                    if body_text.count('§') >= 3:
                        return True
                    # Condition 2: document is long enough
                    if len(body_text) > 3000:
                        return True
                    # Condition 3 & 4: known content divs are present and populated
                    for sel in ("#documentBody", "#lovdataDocument", "#maincolOneColumn"):
                        try:
                            el = d.find_element(By.CSS_SELECTOR, sel)
                            if len(el.text) > 500:
                                return True
                        except Exception:
                            continue
                    return False
                except Exception:
                    return False

            WebDriverWait(self.driver, timeout).until(body_has_content)
        except TimeoutException:
            # Content may still be partially loaded — proceed anyway
            pass
        time.sleep(1.0)

    # =========================================================================
    # CONTENT SCRAPING
    # =========================================================================

    def scrape_content_from_url(self, doc_url: str) -> dict:
        """
        Visit a Lovdata Pro document URL and extract content + metadata from iframe.

        Confirmed DOM structure (from DevTools screenshot):
          iframe[name="documentFrame", src="about:blank"]
            └── body.lovdata-document
                  └── div#maincolOneColumn
                        └── div#lovdataDocument
                              ├── div#documentMeta  (metadata table)
                              └── div#documentBody  ← PRIMARY CONTENT TARGET

        KEY FIXES in this version:
          1. _wait_for_iframe_content() now handles all document types,
             not just §-based laws. Forarbeid/artikler use numbered headings.
          2. Content selectors now prioritise #documentBody first.
          3. _strip_metadata_header() handles §, numbered headings, and
             court decision patterns — not just § lines.
          4. SPA fallback no longer applies nav_indicators check to specific
             CSS selectors (#documentBody etc.) — those selectors already
             target only document content, not the page shell.
        """
        result = {
            "title":          "",
            "date":           "",
            "year":           None,
            "content":        "",
            "content_source": "",
            "page_meta":      {},
        }

        try:
            self.driver.switch_to.default_content()
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
            best_meta: dict = {}

            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            logger.debug("  Found %s iframes on page", len(iframes))

            for idx, iframe in enumerate(iframes):
                try:
                    # Skip hidden 0x0 utility iframes
                    w = iframe.get_attribute("width")
                    h = iframe.get_attribute("height")
                    if w == "0" or h == "0":
                        continue
                    style = (iframe.get_attribute("style") or "").lower()
                    if "width: 0" in style or "height: 0" in style:
                        continue

                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(iframe)

                    # ── Wait for content to load ──────────────────────────
                    # Uses multi-condition wait that handles all document types.
                    self._wait_for_iframe_content(timeout=20)

                    # ── Metadata table ────────────────────────────────────
                    page_meta: dict = {}
                    try:
                        page_meta = self.driver.execute_script("""
                            var result = {};

                            // h1 = fulltittel (the big title above the table)
                            var h1 = document.querySelector('h1');
                            if (h1 && h1.innerText.trim()) {
                                result['fulltittel'] = h1.innerText.trim();
                            }

                            var tables = document.querySelectorAll('table');
                            for (var t = 0; t < tables.length; t++) {
                                var rows = tables[t].querySelectorAll('tr');
                                var tableHits = 0;
                                var tableResult = {};

                                for (var r = 0; r < rows.length; r++) {
                                    var cells = rows[r].querySelectorAll('td, th');
                                    if (cells.length < 2) continue;

                                    var label = (cells[0].innerText || '').trim().replace(/:$/, '');
                                    var value = (cells[1].innerText || '').trim();

                                    if (!label || !value) continue;
                                    if (label.length > 40) continue;
                                    if (label.indexOf('§') >= 0) continue;
                                    if (/^\\d/.test(label)) continue;
                                    if (/^[a-f]$/.test(label.toLowerCase())) continue;
                                    var skipLabels = ['historiske versjoner', 'endringer',
                                        'forskrifter', 'forarbeider', 'rundskriv',
                                        'avgjørelser', 'lovspeil', 'litteratur',
                                        'andre henvisninger'];
                                    var labelLower = label.toLowerCase();
                                    if (skipLabels.some(function(s){ return labelLower === s; })) continue;

                                    var key = label.toLowerCase()
                                        .replace(/\\s+/g, '_')
                                        .replace(/[^a-z0-9_\\u00e6\\u00f8\\u00e5]/g, '');
                                    if (!key || key[0] === '_') continue;
                                    if (key.length >= 60) continue;
                                    tableResult[key] = value;
                                    tableHits++;
                                }

                                if (tableHits >= 3) {
                                    for (var k in tableResult) result[k] = tableResult[k];
                                    break;
                                }
                            }

                            return result;
                        """) or {}
                        if page_meta:
                            logger.debug("  iframe[%s] table meta keys: %s", idx, list(page_meta.keys()))
                    except Exception as e:
                        logger.debug("  iframe[%s] table meta failed: %s", idx, e)

                    # ── Title ─────────────────────────────────────────────
                    title = page_meta.get("fulltittel", "") or page_meta.get("korttittel", "")
                    if not title:
                        for sel in ["h1", ".tittel", "[class*='tittel']", ".navn"]:
                            try:
                                t = self.driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                                if t and len(t) > 3:
                                    title = t
                                    break
                            except Exception:
                                continue

                    # ── Date ──────────────────────────────────────────────
                    date = page_meta.get("dato", "") or page_meta.get("kunngjort", "")
                    if not date:
                        for sel in [".dato", "[class*='dato']", ".kunngjort", "time"]:
                            try:
                                el = self.driver.find_element(By.CSS_SELECTOR, sel)
                                d  = (el.text or el.get_attribute("datetime") or "").strip()
                                if d:
                                    date = d
                                    break
                            except Exception:
                                continue

                    # ── Content extraction ────────────────────────────────
                    content = ""
                    source  = ""

                    # Priority 0: raw XML/pre content (some old documents)
                    for tag in ("pre", "code"):
                        try:
                            t = self.driver.find_element(By.TAG_NAME, tag).text.strip()
                            if len(t) >= _MIN_CONTENT_CHARS:
                                content = t
                                source  = "xml_pre"
                                break
                        except Exception:
                            continue

                    # Priority 1: specific content divs — ordered by specificity.
                    # #documentBody is the confirmed correct element from DevTools.
                    # No nav_indicators check needed: these selectors already
                    # target only the document content div, not the page shell.
                    if not content:
                        for sel in _CONTENT_SELECTORS:
                            try:
                                t = self.driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                                if len(t) >= _MIN_CONTENT_CHARS:
                                    content = t
                                    source  = "iframe_" + sel.strip("#.")
                                    break
                            except Exception:
                                continue

                    # Priority 2: full iframe body — only if longer than above,
                    # and only if it doesn't look like the outer page shell.
                    # (Inside an iframe, the body IS the document — but check anyway.)
                    try:
                        t = self.driver.find_element(By.TAG_NAME, "body").text.strip()
                        if len(t) >= _MIN_CONTENT_CHARS:
                            nav_indicators = [
                                "logg inn", "log in", "rettsområder",
                                "lovdata pro", "søk i lovdata",
                            ]
                            is_nav = any(nav in t.lower() for nav in nav_indicators)
                            if not is_nav and len(t) > len(content):
                                content = t
                                source  = "iframe_body"
                    except Exception:
                        pass

                    logger.info(
                        "  iframe[%s] content: %s chars (best so far: %s)",
                        idx, len(content), len(best_content)
                    )

                    if len(content) > len(best_content):
                        # Strip metadata header before the real document text.
                        # Works for §-based laws, numbered forarbeid headings,
                        # court decisions, and preamble lines.
                        content = _strip_metadata_header(content)

                        best_content = content
                        best_title   = title or best_title
                        best_date    = date or best_date
                        best_source  = source
                        best_meta    = page_meta if page_meta else best_meta

                except Exception as e:
                    logger.debug("iframe[%s] error: %s", idx, e)
                finally:
                    self.driver.switch_to.default_content()

            # ══════════════════════════════════════════════════════════════════
            # SPA FALLBACK
            # For URLs like: https://lovdata.no/pro/#document/PROP/forarbeid/otprp-55-197576
            # The iframe src=about:blank means content loads via JS into the iframe DOM.
            # If the iframe read above yielded nothing, navigate directly to the
            # document URL and read content from known div selectors.
            #
            # IMPORTANT: nav_indicators check is SKIPPED for specific CSS selectors
            # (#documentBody, #lovdataDocument etc.) because those divs contain only
            # document text. The nav_indicators check only applies to full body reads
            # where page chrome ("Lovdata Pro", navigation) may be included.
            # ══════════════════════════════════════════════════════════════════
            if not best_content and "#document/" in doc_url:
                m_spa = re.search(r"#document/(.+)", doc_url)
                if m_spa:
                    doc_path = m_spa.group(1).split("?")[0].split("#")[0]
                    candidate_urls = [
                        "https://lovdata.no/pro/document/" + doc_path,
                        "https://lovdata.no/pro/document/" + doc_path + "?showmarkings=true",
                    ]
                    nav_indicators = [
                        "logg inn", "log in", "rettsområder",
                        "lovdata pro", "søk i lovdata",
                    ]

                    # Selectors that target ONLY document content — no nav check needed.
                    CONTENT_ONLY_SELECTORS = [
                        "#documentBody",
                        "div#documentBody",
                        "#lovdataDocument",
                        "#maincolOneColumn",
                        "div.lovdata-document",
                        "div.document-content",
                        "article",
                        "main",
                    ]

                    for direct_url in candidate_urls:
                        if best_content:
                            break
                        logger.info("  SPA fallback — trying: %s", direct_url)
                        try:
                            self.driver.switch_to.default_content()
                            self.driver.get(direct_url)
                            try:
                                self.wait.until(
                                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                                )
                            except Exception:
                                pass
                            time.sleep(4)

                            # Strategy A: specific content elements — NO nav check.
                            for sel in CONTENT_ONLY_SELECTORS:
                                try:
                                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                                    t  = el.text.strip()
                                    logger.info("  direct[css:%s] %s chars", sel, len(t))
                                    if len(t) >= _MIN_CONTENT_CHARS:
                                        t = _strip_metadata_header(t)
                                        best_content = t
                                        best_source  = "direct_css_" + sel.strip("#.")
                                        logger.info(
                                            "  SPA fallback SUCCEEDED via CSS: %s chars", len(t)
                                        )
                                        break
                                except Exception:
                                    continue

                            # Strategy B: JS strip nav/header/footer then read body.
                            if not best_content:
                                try:
                                    t = self.driver.execute_script("""
                                        var cloneBody = document.body.cloneNode(true);
                                        var skipTags = cloneBody.querySelectorAll(
                                            'nav, header, footer, aside, ' +
                                            '[class*="nav"], [class*="menu"], [class*="sidebar"], ' +
                                            '[class*="header"], [class*="footer"], ' +
                                            '[id*="nav"], [id*="menu"], [id*="sidebar"], ' +
                                            '[id*="header"], [id*="footer"]'
                                        );
                                        skipTags.forEach(function(el) { el.remove(); });
                                        return cloneBody.innerText;
                                    """) or ""
                                    t = t.strip()
                                    logger.info("  direct[js_strip_nav] %s chars", len(t))
                                    if len(t) >= _MIN_CONTENT_CHARS:
                                        best_content = _strip_metadata_header(t)
                                        best_source  = "direct_js_strip_nav"
                                except Exception:
                                    pass

                            # Strategy C: full body — with nav check (last resort).
                            if not best_content:
                                try:
                                    t = self.driver.find_element(
                                        By.TAG_NAME, "body"
                                    ).text.strip()
                                    is_nav = any(n in t.lower() for n in nav_indicators)
                                    logger.info(
                                        "  direct[body_text] %s chars  is_nav=%s", len(t), is_nav
                                    )
                                    if not is_nav and len(t) >= _MIN_CONTENT_CHARS:
                                        best_content = _strip_metadata_header(t)
                                        best_source  = "direct_body"
                                except Exception:
                                    pass

                            # Strategy D: page_source parse (strip nav tags too).
                            if not best_content:
                                try:
                                    raw   = self.driver.page_source or ""
                                    logger.info(
                                        "  direct[page_source] len=%s  head: %s",
                                        len(raw), raw[:200].replace("\n", " ")
                                    )
                                    clean = re.sub(
                                        r"<(script|style|nav|header|footer|aside)[^>]*>"
                                        r".*?</(script|style|nav|header|footer|aside)>",
                                        " ", raw, flags=re.DOTALL | re.IGNORECASE
                                    )
                                    clean = re.sub(r"<[^>]+>", " ", clean)
                                    clean = re.sub(r"[ \t]+", " ", clean)
                                    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
                                    clean = (clean
                                        .replace("&amp;", "&").replace("&lt;", "<")
                                        .replace("&gt;", ">").replace("&nbsp;", " ")
                                    )
                                    logger.info(
                                        "  direct[page_source parsed] %s chars", len(clean)
                                    )
                                    if len(clean) >= _MIN_CONTENT_CHARS:
                                        best_content = _strip_metadata_header(clean)
                                        best_source  = "direct_page_source"
                                except Exception:
                                    pass

                            if best_content:
                                logger.info(
                                    "  SPA fallback SUCCEEDED: %s chars  source=%s  url=%s",
                                    len(best_content), best_source, direct_url
                                )
                            else:
                                logger.warning("  SPA fallback empty for: %s", direct_url)

                        except Exception as _e:
                            logger.error("  SPA direct URL error [%s]: %s", direct_url, _e)
                        finally:
                            self.driver.switch_to.default_content()

                    if not best_content:
                        logger.warning(
                            "  SPA fallback exhausted — all strategies empty for: %s", doc_url
                        )

            # ── Extract year ──────────────────────────────────────────────────
            year = None
            if best_date:
                m = re.search(r"(19\d{2}|20\d{2})", best_date)
                if m:
                    year = int(m.group(1))
            if year is None and best_title:
                m = re.search(r"(19\d{2}|20\d{2})", best_title)
                if m:
                    year = int(m.group(1))
            if year is None:
                year = _extract_year_from_doc_url(doc_url)
            if year is None and best_content:
                m = re.search(r"(19\d{2}|20\d{2})", best_content[:2000])
                if m:
                    year = int(m.group(1))

            result["title"]          = best_title
            result["date"]           = best_date
            result["year"]           = year
            result["content"]        = best_content
            result["content_source"] = best_source
            result["page_meta"]      = best_meta

            if best_content:
                logger.info(
                    "  Scraped: title='%s'  date='%s'  year=%s  chars=%s  source=%s  meta_fields=%s",
                    (best_title or "(none)")[:60],
                    best_date or "(none)",
                    year,
                    len(best_content),
                    best_source,
                    list(best_meta.keys()),
                )
            else:
                logger.warning(
                    "  No content extracted from any iframe: %s", doc_url
                )

        except Exception as e:
            logger.error("scrape_content_from_url failed [%s]: %s", doc_url, e)

        return result