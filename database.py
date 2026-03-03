"""
DATABASE MODULE
Updated to use new schema:
  legal_area_root, legal_area_branch, legal_area_leaf,
  document_type, storage_path, public_uri
"""

from supabase import create_client, Client
import logging
from typing import List, Dict, Optional
import config

logger = logging.getLogger(__name__)


class Database:

    def __init__(self):
        try:
            if not config.SUPABASE_URL or not config.SUPABASE_KEY:
                raise ValueError("Supabase credentials not configured")

            self.client: Client = create_client(
                config.SUPABASE_URL,
                config.SUPABASE_KEY
            )
            self.client.table('legal_area_metadata').select('id').limit(1).execute()
            logger.info("✓ Database connected")

        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            raise

    def exists_by_file_name(self, file_name: str) -> bool:
        try:
            response = self.client.table('legal_area_metadata') \
                .select('id') \
                .eq('file_name', file_name) \
                .execute()
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"Error checking filename: {e}")
            return False

    def hash_exists(self, file_hash: str) -> Optional[str]:
        try:
            response = self.client.table('legal_area_metadata') \
                .select('file_name') \
                .eq('file_hash', file_hash) \
                .execute()
            if response.data:
                return response.data[0]['file_name']
            return None
        except Exception as e:
            logger.error(f"Error checking hash: {e}")
            return None

    def save_metadata(self, metadata: dict) -> bool:
        """
        Save document metadata to Supabase with new schema fields.
        Expected keys in metadata:
          file_name, file_hash, file_size,
          legal_area_root, legal_area_branch, legal_area_leaf,
          document_type, source_url, storage_path, public_uri
        """
        try:
            if self.exists_by_file_name(metadata['file_name']):
                logger.info(f"⊘ Already in DB: {metadata['file_name']}")
                return False

            insert_data = {
                "file_name":         metadata["file_name"],
                "file_hash":         metadata["file_hash"],
                "file_size":         metadata.get("file_size"),
                "legal_area_root":   metadata.get("legal_area_root", ""),
                "legal_area_branch": metadata.get("legal_area_branch", ""),
                "legal_area_leaf":   metadata.get("legal_area_leaf", ""),
                "document_type":     metadata.get("document_type", ""),
                "source_url":        metadata.get("source_url", ""),
                "storage_path":      metadata.get("storage_path", ""),
                "public_uri":        metadata.get("public_uri", ""),
            }

            self.client.table("legal_area_metadata").insert(insert_data).execute()
            logger.info(f"✓ Saved to DB: {metadata['file_name']}")
            return True

        except Exception as e:
            logger.error(f"✗ DB insert failed for {metadata.get('file_name')}: {e}")
            return False

    def get_all_metadata(self) -> List[Dict]:
        try:
            response = self.client.table('legal_area_metadata') \
                .select('*') \
                .execute()
            logger.info(f"Retrieved {len(response.data)} records")
            return response.data
        except Exception as e:
            logger.error(f"Error retrieving metadata: {e}")
            return []

    def get_statistics(self) -> Dict:
        try:
            response = self.client.table('legal_area_metadata') \
                .select('legal_area_root, legal_area_branch, file_size') \
                .execute()
            data = response.data

            stats = {
                'total_files':   len(data),
                'total_size_mb': sum(d.get('file_size', 0) or 0 for d in data) / (1024 * 1024),
                'by_root':       {},
            }

            for item in data:
                root = item.get('legal_area_root', 'Unknown')
                size = item.get('file_size', 0) or 0

                if root not in stats['by_root']:
                    stats['by_root'][root] = {'count': 0, 'size_mb': 0}
                stats['by_root'][root]['count']   += 1
                stats['by_root'][root]['size_mb'] += size / (1024 * 1024)

            return stats

        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}

    def delete_by_filename(self, file_name: str) -> bool:
        try:
            self.client.table('legal_area_metadata') \
                .delete() \
                .eq('file_name', file_name) \
                .execute()
            logger.info(f"✓ Deleted from DB: {file_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting record: {e}")
            return False