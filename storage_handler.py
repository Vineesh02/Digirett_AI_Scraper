"""
STORAGE HANDLER
===============
Manages Supabase Storage uploads and metadata table inserts.

Per-document flow:
  1. Upload XML to Supabase Storage bucket
  2. Insert metadata row into Supabase table
  3. Delete local XML file

Duplicate detection via file_name and file_hash before processing.
"""

import os
import logging
from supabase import create_client
import config

logger = logging.getLogger(__name__)


class StorageHandler:

    def __init__(self):
        self.client = create_client(
            config.SUPABASE_URL,
            config.SUPABASE_SERVICE_ROLE_KEY,
        )
        self.bucket = config.SUPABASE_BUCKET
        self.table  = config.SUPABASE_TABLE
        logger.info("Storage handler initialised  bucket=%s  table=%s", self.bucket, self.table)

    # -------------------------------------------------------------------------
    # Upload
    # -------------------------------------------------------------------------

    def upload_xml(self, local_path: str, bucket_path: str) -> str:
        """
        Upload XML file to Supabase Storage.
        Returns public URL on success, empty string on failure.
        Does NOT delete the local file — caller decides when to delete.
        """
        if not os.path.exists(local_path):
            logger.error("Upload failed — file not found: %s", local_path)
            return ""

        try:
            with open(local_path, "rb") as f:
                data = f.read()

            self.client.storage.from_(self.bucket).upload(
                bucket_path,
                data,
                {"content-type": "application/xml", "upsert": "true"},
            )

            public_url = self.client.storage.from_(self.bucket).get_public_url(bucket_path)
            if isinstance(public_url, dict):
                public_url = public_url.get("publicURL", public_url.get("publicUrl", ""))

            logger.info("Uploaded: %s", bucket_path)
            return public_url or ""

        except Exception as e:
            logger.error("Upload failed [%s]: %s", bucket_path, e)
            return ""

    # -------------------------------------------------------------------------
    # Metadata insert
    # -------------------------------------------------------------------------

    def insert_metadata(self, metadata: dict) -> bool:
        """
        Insert document metadata row into Supabase table.

        Expected keys:
          file_name, file_hash, file_size,
          legal_area_root, legal_area_branch, legal_area_leaf,
          document_type, source_url, content_source,
          bucket_path, public_uri
        """
        try:
            row = {
                "file_name":         metadata["file_name"],
                "file_hash":         metadata["file_hash"],
                "file_size":         metadata.get("file_size"),
                "legal_area_root":   metadata.get("legal_area_root",   ""),
                "legal_area_branch": metadata.get("legal_area_branch", ""),
                "legal_area_leaf":   metadata.get("legal_area_leaf"),       # None ok
                "document_type":     metadata.get("document_type",     ""),
                "source_url":        metadata.get("source_url",        ""),
                "storage_path":      f"{self.bucket}/{metadata.get('bucket_path', '')}",
                "public_uri":        metadata.get("public_uri",        ""),
            }

            resp = self.client.table(self.table).insert(row).execute()
            if resp.data:
                logger.info("Metadata inserted: %s", metadata["file_name"])
                return True

            logger.error("Metadata insert returned no data: %s", resp)
            return False

        except Exception as e:
            logger.error("Metadata insert failed [%s]: %s", metadata.get("file_name"), e)
            return False

    # -------------------------------------------------------------------------
    # Delete local file
    # -------------------------------------------------------------------------

    def delete_local(self, local_path: str) -> None:
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.debug("Local file deleted: %s", local_path)
        except Exception as e:
            logger.warning("Could not delete local file [%s]: %s", local_path, e)

    # -------------------------------------------------------------------------
    # Duplicate checks
    # -------------------------------------------------------------------------

    def record_exists(self, file_name: str) -> bool:
        try:
            resp = (
                self.client.table(self.table)
                .select("file_name")
                .eq("file_name", file_name)
                .limit(1)
                .execute()
            )
            return bool(resp.data)
        except Exception as e:
            logger.error("record_exists check failed: %s", e)
            return False

    def hash_exists(self, file_hash: str) -> bool:
        try:
            resp = (
                self.client.table(self.table)
                .select("file_hash")
                .eq("file_hash", file_hash)
                .limit(1)
                .execute()
            )
            return bool(resp.data)
        except Exception as e:
            logger.error("hash_exists check failed: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Cleanup empty local folders
    # -------------------------------------------------------------------------

    def cleanup_empty_folders(self, base_dir: str) -> None:
        try:
            for root, dirs, files in os.walk(base_dir, topdown=False):
                for d in dirs:
                    folder = os.path.join(root, d)
                    if not os.listdir(folder):
                        os.rmdir(folder)
        except Exception as e:
            logger.error("cleanup_empty_folders failed: %s", e)