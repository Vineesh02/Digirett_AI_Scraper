"""
XML FILE HANDLER
Saves scraped document content as a structured .xml file locally.
All metadata fields are always written — never empty tags for hierarchy.
"""

import os
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import xml.etree.ElementTree as ET

import config

logger = logging.getLogger(__name__)


class XMLHandler:

    @staticmethod
    def save(
        document: dict,
        local_folder: str,
    ) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
        """
        Build a structured XML file from a scraped document dict and save locally.

        Required keys in document:
          file_name, url, title, date, content, content_source,
          legal_area_root, legal_area_branch, legal_area_leaf, document_type

        Returns:
          (file_path, file_size_bytes, md5_hash, content_preview_500chars)
          All None on failure.
        """
        try:
            Path(local_folder).mkdir(parents=True, exist_ok=True)

            # ── Pull all fields — use "unknown" fallback never blank ──
            file_name    = (document.get("file_name")    or "").strip()
            source_url   = (document.get("url")          or "").strip()
            title        = (document.get("title")        or "").strip()
            date         = (document.get("date")         or "").strip()
            root         = (document.get("legal_area_root")   or "unknown").strip()
            branch       = (document.get("legal_area_branch") or "").strip()
            leaf         = (document.get("legal_area_leaf")   or "").strip()
            doc_type     = (document.get("document_type")     or "").strip()
            content_src  = (document.get("content_source")    or "").strip()
            content      = (document.get("content")           or "").strip()

            if not file_name:
                logger.error("XMLHandler.save: file_name is empty")
                return None, None, None, None

            # ── Build XML tree ────────────────────────────────────────
            root_el = ET.Element("document")

            # ── metadata block ────────────────────────────────────────
            meta = ET.SubElement(root_el, "metadata")

            ET.SubElement(meta, "file_name").text   = file_name
            ET.SubElement(meta, "source_url").text  = source_url
            ET.SubElement(meta, "title").text       = title if title   else "—"
            ET.SubElement(meta, "date").text        = date  if date    else "—"

            # Hierarchy — always written with clear labels
            hierarchy = ET.SubElement(meta, "hierarchy")
            ET.SubElement(hierarchy, "root").text   = root
            ET.SubElement(hierarchy, "branch").text = branch if branch else "(none)"
            ET.SubElement(hierarchy, "leaf").text   = leaf   if leaf   else "(none)"

            # Legacy flat fields kept for DB compatibility
            ET.SubElement(meta, "legal_area_root").text   = root
            ET.SubElement(meta, "legal_area_branch").text = branch if branch else ""
            ET.SubElement(meta, "legal_area_leaf").text   = leaf   if leaf   else ""

            ET.SubElement(meta, "document_type").text   = doc_type
            ET.SubElement(meta, "content_source").text  = content_src
            ET.SubElement(meta, "scraped_at").text       = datetime.now().isoformat()

            # ── content block ─────────────────────────────────────────
            content_el = ET.SubElement(root_el, "content")
            content_el.text = content

            # ── Write file ────────────────────────────────────────────
            file_path = os.path.join(local_folder, file_name)
            tree = ET.ElementTree(root_el)
            ET.indent(tree, space="  ")
            tree.write(file_path, encoding="utf-8", xml_declaration=True)

            file_size       = os.path.getsize(file_path)
            file_hash       = XMLHandler._md5(file_path)
            content_preview = content[:500]

            logger.info(
                "💾 XML saved: %s  root='%s'  branch='%s'  leaf='%s'  size=%sb",
                file_path, root, branch or "(none)", leaf or "(none)", file_size
            )
            return file_path, file_size, file_hash, content_preview

        except Exception as e:
            logger.error("❌ XMLHandler.save failed: %s", e, exc_info=True)
            return None, None, None, None

    @staticmethod
    def _md5(file_path: str) -> str:
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()