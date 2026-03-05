"""
LOVDATA PRO SCRAPER — CONFIGURATION
"""

import os
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

# ============================================================
# CREDENTIALS
# ============================================================
SUPABASE_URL              = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY              = os.getenv("SUPABASE_KEY", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "legal_area").strip()
SUPABASE_TABLE  = os.getenv("SUPABASE_TABLE",  "legal_area_metadata").strip()

LOVDATA_USERNAME = os.getenv("LOVDATA_USERNAME", "").strip()
LOVDATA_PASSWORD = os.getenv("LOVDATA_PASSWORD", "").strip()

# ============================================================
# LOCAL STORAGE  (temp folder for XML files before upload)
# ============================================================
BASE_DIR = "./scraped_xml"
LOGS_DIR = "./logs"

Path(BASE_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# ============================================================
# SCRAPING SETTINGS
# ============================================================
HEADLESS               = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
TIMEOUT                = int(os.getenv("TIMEOUT", "30"))
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "2.0"))

START_YEAR = int(os.getenv("START_YEAR", "2000"))
END_YEAR   = int(os.getenv("END_YEAR",   "2026"))

MAX_DOCS_PER_LEGAL_AREA = int(os.getenv("MAX_DOCS_PER_LEGAL_AREA", "999999"))
BATCH_UPLOAD_SIZE       = int(os.getenv("BATCH_UPLOAD_SIZE", "10"))

TARGET_ROOT_CATEGORIES = [
    "Anskaffelser, avtaler, bygg og entrepriser",
    "Arbeidsrett",
]
# ============================================================
print("ENV loaded     :", ENV_PATH)
print("SUPABASE_TABLE :", SUPABASE_TABLE)
print("SUPABASE_BUCKET:", SUPABASE_BUCKET)
print("BASE_DIR       :", BASE_DIR)