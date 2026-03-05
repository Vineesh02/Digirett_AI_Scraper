import os
import re
import hashlib
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

# Fields written first in a fixed order if present.
# Everything else from page_meta is written after automatically.
_PRIORITY_FIELDS = [
    "fulltittel",
    "korttittel",
    "dato",
    "year",          # synthetic — derived from date/title/url/content
    "departement",
    "sist_endret",
    "ikraftdato",
    "ikrafttredelse",
    "endrer",
    "kunngjort",
    "avdeling",
    "myndighet",
    "rettsomrade",
    "rettsomr_de",
    "status",
]


class XMLHandler:

    @staticmethod
    def save(
        document: dict,
        local_folder: str,
    ) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
        """
        Build and save XML for a scraped document.

        page_meta is written dynamically — every key found in the metadata
        table on the page becomes an XML element. No fields are dropped.
        """
        try:
            Path(local_folder).mkdir(parents=True, exist_ok=True)

            file_name    = (document.get("file_name")      or "").strip()
            pro_url      = (document.get("url")            or "").strip()
            doc_type     = (document.get("document_type")  or "").strip()
            title        = (document.get("title")          or "").strip()
            date         = (document.get("date")           or "").strip()
            year         = document.get("year")
            content      = (document.get("content")        or "").strip()
            content_src  = (document.get("content_source") or "").strip()
            page_meta    = dict(document.get("page_meta")  or {})

            if not file_name:
                logger.error("XMLHandler.save: file_name is empty")
                return None, None, None, None

            # Fill in title/date from scraper fallbacks if table didn't have them
            if title and "fulltittel" not in page_meta:
                page_meta["fulltittel"] = title
            if title and "korttittel" not in page_meta:
                page_meta["korttittel"] = title
            if date and "dato" not in page_meta:
                page_meta["dato"] = date

            # year is always written as a derived field
            page_meta["year"] = str(year) if year is not None else "—"

            doc_id = str(uuid.uuid4())[:8]

            # ------------------------------------------------------------------
            # Build XML tree
            # ------------------------------------------------------------------
            root_el = ET.Element("document")

            meta = ET.SubElement(root_el, "metadata")
            ET.SubElement(meta, "id").text            = doc_id
            ET.SubElement(meta, "url").text           = pro_url
            ET.SubElement(meta, "scraped_at").text    = datetime.now().isoformat()
            ET.SubElement(meta, "document_type").text = doc_type

            # Write priority fields first (if present in page_meta)
            written = set()
            for field in _PRIORITY_FIELDS:
                if field in page_meta:
                    ET.SubElement(meta, field).text = str(page_meta[field]) or "—"
                    written.add(field)

            # Write ALL remaining page_meta fields — whatever the table had
            for key, val in page_meta.items():
                if key in written:
                    continue
                safe_key = _safe_tag(key)
                if safe_key:
                    ET.SubElement(meta, safe_key).text = str(val) if val else "—"

            # ------------------------------------------------------------------
            # Content wrapped in CDATA so special chars never break the XML
            # ------------------------------------------------------------------
            ET.SubElement(root_el, "content")

            ET.indent(root_el, space="  ")
            xml_str = ET.tostring(root_el, encoding="unicode", xml_declaration=False)

            cdata_block = f"<![CDATA[\n{content}\n]]>"
            xml_str = xml_str.replace("<content />", f"<content>{cdata_block}</content>")
            xml_str = xml_str.replace("<content/>",  f"<content>{cdata_block}</content>")

            file_path = os.path.join(local_folder, file_name)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("<?xml version='1.0' encoding='utf-8'?>\n")
                f.write(xml_str)

            file_size       = os.path.getsize(file_path)
            file_hash       = XMLHandler._md5(file_path)
            content_preview = content[:500]

            logger.info(
                "XML saved: %s  size=%s bytes  meta_fields=%s",
                file_name, file_size, list(page_meta.keys()),
            )
            return file_path, file_size, file_hash, content_preview

        except Exception as e:
            logger.error("XMLHandler.save failed: %s", e, exc_info=True)
            return None, None, None, None

    @staticmethod
    def _md5(file_path: str) -> str:
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()


def _safe_tag(name: str) -> str:
    """Convert an arbitrary string to a valid XML tag name."""
    name = re.sub(r"\s+", "_", name.strip())
    name = re.sub(r"[^\w\-.]", "", name)
    if name and name[0].isdigit():
        name = "field_" + name
    return name[:80]