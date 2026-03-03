"""
LOVDATA PRO SCRAPER

Confirmed XPaths from user (id="x-auto-3752" is dynamic but STRUCTURE is fixed):
  Show All : //*[@id="x-auto-3752"]/tbody/tr[2]/td[2]/em/button
  Next      : //*[@id="x-auto-3752"]/tbody/tr[2]/td[3]/em/button

The id "x-auto-NNNN" changes every page load — Ext JS regenerates all IDs.
So we strip the ID and use the stable table structure directly:

  Show All : //table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[2]/em/button
  Next      : //table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[3]/em/button

Each section on the page has its OWN toolbar table — so we find the toolbar
that belongs to a specific section by scoping the search to that section's
container element (x-panel).

HOW SECTION SCOPING WORKS:
  1. Each section (LOVER, FORSKRIFTER etc.) is rendered as an Ext JS x-panel.
  2. Inside each x-panel there is a paging toolbar — a <table class="x-toolbar-ct">.
  3. That toolbar has:
       tr[1] = spacer row
       tr[2] = actual toolbar buttons:
                 td[1] = Prev button
                 td[2] = "Vis alle (N)" button   ← Show All
                 td[3] = Next button
                 td[4] = separator
                 td[5] = Last button
                 td[6] = page info text
  4. We find the section panel first (by its heading text), then find the
     toolbar TABLE inside that panel, then click td[2]/em/button for Show All
     and td[3]/em/button for Next.
  5. Links are also collected from inside that panel only — so each section
     gives its OWN count, not the global page total.
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
)

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section label map  Norwegian → canonical English
# ---------------------------------------------------------------------------
SECTION_MAP = {
    "SISTE DOKUMENTER":   "LATEST DOCUMENTS",
    "LOVER":              "LAWS",
    "FORSKRIFTER":        "REGULATIONS",
    "AVGJØRELSER FRA HØYESTERETT":     "DECISIONS FROM THE SUPREME COURT",
    "AVGJØRELSER FRA LAGMANNSRETTENE": "DECISIONS FROM THE COURTS OF APPEAL",
    "AVGJØRELSER FRA TINGRETTENE":     "DECISIONS FROM THE DISTRICT COURTS",
    "ARTIKLER":           "ARTICLES",
    "DOKUMENTER FRA KLAGENEMNDA FOR OFFENTLIGE ANSKAFFELSER":
        "DOCUMENTS FROM THE PUBLIC PROCUREMENT COMPLAINTS BOARD",
    "DOKUMENTER FRA BYGGEBRANSJENS FAGLIG JURIDISKE RÅD":
        "DOCUMENTS FROM THE CONSTRUCTION INDUSTRY'S PROFESSIONAL LEGAL COUNCIL",
    "DOKUMENTER FRA JUSTISDEPARTEMENTET": "DOCUMENTS FROM THE MINISTRY OF JUSTICE",
    "ANDRE DOKUMENTER":   "OTHER DOCUMENTS",
    # English passthrough (page sometimes renders in English)
    "LATEST DOCUMENTS":   "LATEST DOCUMENTS",
    "LAWS":               "LAWS",
    "REGULATIONS":        "REGULATIONS",
    "ARTICLES":           "ARTICLES",
    "OTHER DOCUMENTS":    "OTHER DOCUMENTS",
}


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


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class LovdataScraper:

    # Stable CSS for tree — never use IDs
    _NODE_TEXT_CSS = "span.x-tree3-node-text"
    _EC_ICON_CSS   = "img.x-tree3-ec-icon"

    # ── Toolbar XPath (structural, no ID) ────────────────────────────
    # The confirmed XPath from user was:
    #   //*[@id="x-auto-3752"]/tbody/tr[2]/td[2]/em/button   ← Show All
    #   //*[@id="x-auto-3752"]/tbody/tr[2]/td[3]/em/button   ← Next
    #
    # "x-auto-3752" is a <table class="x-toolbar-ct">.
    # We replace the id-based //*[@id="..."] with class-based selector:
    #   .//table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[2]/em/button
    #
    # This is scoped INSIDE a section panel so it only hits that section's toolbar.

    _SHOW_ALL_XPATH = ".//table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[2]/em/button"
    _NEXT_XPATH     = ".//table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[3]/em/button"

    # ── Section panel heading XPaths ─────────────────────────────────
    # Each section heading is inside a panel header span
    _PANEL_HEADING_CSS = "span.x-panel-header-text"

    def __init__(self, driver):
        self.driver = driver
        self.wait   = WebDriverWait(self.driver, config.TIMEOUT)

    # ===================================================================
    # LOGIN — DO NOT CHANGE
    # ===================================================================
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

    # ===================================================================
    # NAVIGATION
    # ===================================================================

    def go_to_legal_areas(self):
        self.driver.switch_to.default_content()
        self.driver.get("https://lovdata.no/pro/#rettsomrade")
        self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)

        # Click "Rettsområder" tab if visible
        try:
            for a in self.driver.find_elements(By.TAG_NAME, "a"):
                if a.is_displayed() and "rettsområder" in (a.text or "").lower():
                    self.driver.execute_script("arguments[0].click();", a)
                    time.sleep(2)
                    break
        except Exception:
            pass

        # Wait for tree nodes — stable CSS, no ID
        try:
            self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, self._NODE_TEXT_CSS)
            ))
            logger.info("✅ Tree nodes present")
        except TimeoutException:
            logger.warning("⚠️ Tree nodes not found")
        time.sleep(1)

    # ===================================================================
    # TREE — discover root nodes
    # ===================================================================

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

    # ===================================================================
    # SECTION PANELS — find each section's x-panel container
    # ===================================================================

    def _find_section_panels(self) -> List[Tuple[str, object]]:
        """
        Scan every possible heading element on the page.
        Log ALL text found so we can see exactly what Lovdata renders.
        Match against SECTION_MAP and return (canonical, panel_el) pairs.

        Tries selectors in priority order:
          1. span.x-panel-header-text   (standard Ext JS panel header)
          2. div.x-panel-header         (the header div itself)
          3. td.x-panel-header-noborder-left  (table-based header layout)
          4. span[class*='header']      (any header span)
          5. div[class*='header']       (any header div)
          6. th, h1, h2, h3, h4        (generic headings)

        For each match: walk up to the nearest x-panel ancestor to get
        the full panel container (body + toolbar) for scoped link collection.
        """
        found = []
        seen  = set()

        # All selectors to try — in priority order
        selectors = [
            "span.x-panel-header-text",
            "div.x-panel-header",
            "td.x-panel-header-noborder-left",
            "span[class*='header-text']",
            "div[class*='panel-header']",
            "span[class*='header']",
            "th", "h1", "h2", "h3", "h4",
        ]

        # Collect ALL visible text from ALL selectors — log everything
        all_texts_seen = []

        for sel in selectors:
            try:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    try:
                        if not el.is_displayed():
                            continue
                        raw = re.sub(r"\s+", " ", (el.text or "").strip())
                        if not raw or raw in all_texts_seen:
                            continue
                        all_texts_seen.append(raw)
                    except Exception:
                        continue
            except Exception:
                continue

        # Log everything found so we can debug
        logger.info("  🔍 All heading texts on page (%s total):", len(all_texts_seen))
        for t in all_texts_seen:
            upper = t.upper()
            match = SECTION_MAP.get(upper, "—")
            logger.info("      [%s] → map=%s", t[:80], match)

        # Now do the actual matching pass
        for sel in selectors:
            try:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    try:
                        if not el.is_displayed():
                            continue
                        raw       = re.sub(r"\s+", " ", (el.text or "").strip())
                        upper     = raw.upper()
                        canonical = SECTION_MAP.get(upper)
                        if not canonical or canonical in seen:
                            continue

                        # Walk up to nearest x-panel ancestor
                        panel = None
                        for ancestor_cls in [
                            "x-panel",
                            "x-grid-panel",
                            "x-panel-noborder",
                        ]:
                            try:
                                panel = el.find_element(
                                    By.XPATH,
                                    f"./ancestor::div[contains(@class,'{ancestor_cls}')][1]"
                                )
                                break
                            except Exception:
                                continue

                        if panel is None:
                            # No panel ancestor — use a wide parent div
                            try:
                                panel = el.find_element(By.XPATH, "./ancestor::div[3]")
                            except Exception:
                                panel = el

                        seen.add(canonical)
                        found.append((canonical, panel))
                        logger.info(
                            "    ✓ MATCHED: %-50s  selector=%s  panel_cls=%s",
                            canonical, sel,
                            (panel.get_attribute("class") or "")[:60]
                        )

                    except StaleElementReferenceException:
                        continue
            except Exception:
                continue

            # Stop scanning more selectors once we have a good set
            if len(found) >= 3:
                break

        if not found:
            logger.error(
                "❌ No section panels matched. "
                "The section headings above show what IS on the page. "
                "Add the correct text to SECTION_MAP."
            )

        return found

    # ===================================================================
    # SHOW ALL — scoped to one section panel, structural XPath
    # ===================================================================

    def _click_show_all_in_panel(self, panel_el) -> Tuple[bool, Optional[int]]:
        """
        Find the Show All button using the structural XPath (no ID):
          .//table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[2]/em/button

        This is exactly the confirmed XPath with the dynamic ID stripped:
          Original: //*[@id="x-auto-3752"]/tbody/tr[2]/td[2]/em/button
          Stable  : //table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[2]/em/button

        Also reads the count from the button text: "Vis alle (168)"
        """
        total = None

        try:
            btn = panel_el.find_element(By.XPATH, self._SHOW_ALL_XPATH)
            if btn.is_displayed() and btn.is_enabled():
                # Read count from button text
                btn_text = (btn.text or "").strip()
                logger.info("    Show All button text: '%s'", btn_text)
                m = re.search(r"\((\d+)\)", btn_text)
                if m:
                    total = int(m.group(1))

                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", btn
                )
                time.sleep(0.3)
                self.driver.execute_script("arguments[0].click();", btn)
                logger.info("    ✅ Show All clicked — count=%s", total)
                time.sleep(2.5)
                return True, total

        except NoSuchElementException:
            logger.info("    ℹ️  No toolbar Show All button found in this panel")
        except Exception as e:
            logger.warning("    ⚠️ Show All click error: %s", e)

        return False, total

    # ===================================================================
    # NEXT PAGE — scoped to one section panel, structural XPath
    # ===================================================================

    def _click_next_in_panel(self, panel_el) -> bool:
        """
        Find the Next button using the structural XPath (no ID):
          .//table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[3]/em/button

        Original confirmed: //*[@id="x-auto-3752"]/tbody/tr[2]/td[3]/em/button
        Stable            : //table[contains(@class,'x-toolbar-ct')]/tbody/tr[2]/td[3]/em/button

        Before clicking, check that the button is NOT disabled:
          - no @disabled attribute
          - parent <td> does not have class x-item-disabled
        """
        try:
            btn = panel_el.find_element(By.XPATH, self._NEXT_XPATH)

            # Check button itself
            if btn.get_attribute("disabled"):
                logger.info("    ⏹  Next button is disabled (last page)")
                return False

            # Check parent td for x-item-disabled
            try:
                parent_td  = btn.find_element(By.XPATH, "./ancestor::td[1]")
                parent_cls = parent_td.get_attribute("class") or ""
                if "x-item-disabled" in parent_cls or "disabled" in parent_cls:
                    logger.info("    ⏹  Next button td is disabled (last page)")
                    return False
            except Exception:
                pass

            if not btn.is_displayed():
                return False

            self.driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", btn
            )
            self.driver.execute_script("arguments[0].click();", btn)
            logger.info("    ➡️  Next page clicked")
            time.sleep(2.0)
            return True

        except NoSuchElementException:
            logger.info("    ⏹  No Next button found — end of section")
            return False
        except Exception as e:
            logger.warning("    ⚠️ Next click error: %s", e)
            return False

    # ===================================================================
    # COLLECT LINKS — scoped to one section panel only
    # ===================================================================

    def _collect_links_in_panel(self, panel_el, seen: set) -> List[str]:
        """
        Collect #document/ links from inside this section's panel only.
        NOT from the whole page — that's what caused the 88/40 duplicates.
        """
        new_urls = []
        try:
            links = panel_el.find_elements(By.CSS_SELECTOR, "a[href*='#document/']")
            for a in links:
                try:
                    href = (a.get_attribute("href") or "").split("?")[0].strip()
                    if href and href not in seen:
                        seen.add(href)
                        new_urls.append(href)
                except StaleElementReferenceException:
                    continue
        except Exception:
            pass
        return new_urls

    # ===================================================================
    # MAIN: COLLECT URLs PER SECTION
    # ===================================================================

    def collect_urls_by_section(self, area_url: str, max_pages: int = 500) -> List[dict]:
        """
        For each section panel on the legal-area page:
          1. Find the panel by span.x-panel-header-text matching section name
          2. Click Show All inside that panel (structural xpath, td[2])
          3. Collect links inside that panel only
          4. Click Next inside that panel (structural xpath, td[3]) to paginate
          5. Stop when Next is disabled or no new links appear

        This gives the CORRECT count per section, not a global page total.
        """
        self.driver.get(area_url)
        try:
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            pass
        time.sleep(3)

        panels = self._find_section_panels()

        if not panels:
            logger.error(
                "❌ No section panels found. "
                "Check that span.x-panel-header-text exists on this page."
            )
            return []

        results = []

        for canonical, panel_el in panels:
            logger.info("  ↳ Section: %s", canonical)
            seen: set        = set()
            section_urls: List[str] = []

            try:
                # Scroll section into view
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", panel_el
                )
                time.sleep(0.5)

                # Click Show All — reads count from button text
                _, expected = self._click_show_all_in_panel(panel_el)

                # Collect + paginate
                page_num = 0
                while page_num < max_pages:
                    before = len(seen)
                    new    = self._collect_links_in_panel(panel_el, seen)
                    section_urls.extend(new)
                    after  = len(seen)

                    logger.info(
                        "      page %s: +%s new (total in section: %s / expected: %s)",
                        page_num + 1, after - before, after, expected
                    )

                    # Stop if we have all expected docs
                    if expected and after >= expected:
                        logger.info("      ✅ Collected all expected docs (%s)", expected)
                        break

                    # Try Next — scoped to this panel
                    if not self._click_next_in_panel(panel_el):
                        break

                    # If next click produced zero new links, stop
                    time.sleep(0.5)
                    if len(seen) == before:
                        logger.info("      No new links after Next — stopping")
                        break

                    page_num += 1

                logger.info(
                    "    ✅ %-55s expected=%-6s collected=%s",
                    canonical, expected, len(section_urls)
                )
                results.append({
                    "document_type":  canonical,
                    "expected_count": expected,
                    "urls":           section_urls,
                })

            except Exception as e:
                logger.error("    ❌ Section '%s': %s", canonical, e, exc_info=True)
                results.append({
                    "document_type":  canonical,
                    "expected_count": None,
                    "urls":           section_urls,
                })

        return results

    # ===================================================================
    # CONTENT SCRAPING — visit URL, extract from iframe
    # ===================================================================

    def scrape_content_from_url(self, doc_url: str) -> dict:
        """
        Lovdata Pro is a hash-based SPA.
        The actual document content renders inside an <iframe>.

        1. driver.get(url) — loads SPA shell
        2. Wait 3s for iframe to render
        3. Switch into each iframe, extract title + date + content
        4. Keep the iframe with the longest content
        5. Fallback: main window body text
        """
        result = {
            "title":          "",
            "date":           "",
            "content":        "",
            "content_source": "",
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

            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            logger.debug("  iframes: %s", len(iframes))

            for idx, iframe in enumerate(iframes):
                try:
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(iframe)
                    time.sleep(1.0)

                    # Title
                    title = ""
                    for sel in ["h1", ".tittel", "[class*='tittel']",
                                ".navn", "span.bold", "[class*='title']"]:
                        try:
                            t = self.driver.find_element(By.CSS_SELECTOR, sel).text.strip()
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
                            d  = (el.text or el.get_attribute("datetime") or "").strip()
                            if d:
                                date = d
                                break
                        except Exception:
                            continue

                    # Content — priority order
                    content = ""
                    source  = ""

                    # P1: raw <pre>/<code> = XML/text laws
                    for tag in ("pre", "code"):
                        try:
                            t = self.driver.find_element(By.TAG_NAME, tag).text.strip()
                            if len(t) > 100:
                                content = t
                                source  = "xml_pre"
                                break
                        except Exception:
                            continue

                    # P2: Lovdata document content divs
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
                                t = self.driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                                if len(t) > 200:
                                    content = t
                                    source  = "iframe_content"
                                    break
                            except Exception:
                                continue

                    # P3: full iframe body
                    if not content:
                        try:
                            t = self.driver.find_element(By.TAG_NAME, "body").text.strip()
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

            # If no iframe gave content, fallback to main window
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
                (best_title or "—")[:60], best_date or "—",
                len(best_content), best_source
            )

        except Exception as e:
            logger.error("❌ scrape_content_from_url [%s]: %s", doc_url, e)

        return result