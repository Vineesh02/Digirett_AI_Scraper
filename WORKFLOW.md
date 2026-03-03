# LOVDATA PRO SCRAPER - WORKFLOW DIAGRAM

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    LOVDATA PRO SCRAPER WORKFLOW                         │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│   START SCRAPE  │
│   python main.py│
└────────┬────────┘
         │
         v
┌─────────────────────────────┐
│  LOGIN TO LOVDATA PRO       │
│  - Username/Password        │
│  - Navigate to #myPage      │
└────────┬────────────────────┘
         │
         v
┌─────────────────────────────┐         ┌──────────────────────┐
│  MODE SELECTION             │         │ Config Settings:     │
│  1. Sources Only            │────────→│ START_YEAR = 2000    │
│  2. Legal Areas Only        │         │ END_YEAR = 2026      │
│  3. Everything (Both)       │         │ MAX_DOCS = 5000      │
└────────┬────────────────────┘         │ BATCH_SIZE = 20      │
         │                               └──────────────────────┘
         v
┌─────────────────────────────────────────────────────────────────────────┐
│                    PARALLEL SCRAPING PATHS                              │
├────────────────────────────────────┬────────────────────────────────────┤
│  SOURCES OF LAW                    │  LEGAL AREAS                       │
│  ├─ Laws & Regulations             │  ├─ Labor Law                      │
│  │  ├─ Active Laws (NL)            │  ├─ Corporate Law                  │
│  │  ├─ Repealed Laws (NLO)         │  ├─ Contract Law                   │
│  │  ├─ Regulations (SF)            │  ├─ Tax Law                        │
│  │  └─ ... (8 subcategories)       │  ├─ Criminal Law                   │
│  ├─ Court Decisions                │  ├─ ... (35 areas total)           │
│  │  ├─ Supreme Court (HR)          │  └─ Immigration Law                │
│  │  ├─ Appeal Court (LA)           │                                    │
│  │  └─ ... (6 subcategories)       │                                    │
│  ├─ Preparatory Works              │                                    │
│  ├─ Treaties                       │                                    │
│  └─ Guidance                       │                                    │
└────────────────────────────────────┴────────────────────────────────────┘
         │                                       │
         └───────────────┬───────────────────────┘
                         v
         ┌───────────────────────────────┐
         │  FOR EACH CATEGORY:           │
         │  1. Navigate to category page │
         │  2. Extract document URLs     │
         │  3. Process each URL...       │
         └───────────────┬───────────────┘
                         v
┌─────────────────────────────────────────────────────────────────────────┐
│                      DOCUMENT PROCESSING LOOP                           │
│  (For each document URL found)                                          │
└─────────────────────────────────────────────────────────────────────────┘
         │
         v
┌─────────────────────────────┐
│  1. SCRAPE DOCUMENT         │
│  - Open URL in Selenium     │
│  - Extract title            │
│  - Extract content          │
│  - Extract metadata         │
└────────┬────────────────────┘
         │
         v
┌─────────────────────────────┐
│  2. YEAR FILTER             │
│  - Extract year from doc    │
│  - Check: 2000 ≤ year ≤ 2026│
│  - If outside range: SKIP   │
└────────┬────────────────────┘
         │
         v
┌─────────────────────────────┐
│  3. DUPLICATE CHECK         │
│  - Check filename in DB     │
│  - Check content hash in DB │
│  - If duplicate: SKIP       │
└────────┬────────────────────┘
         │
         v
┌─────────────────────────────────────────┐
│  4. CREATE XML FILE (LOCALLY)           │
│  - Location: ./scraped_xml/Category/    │
│  - Structure: Full XML with metadata    │
│  - Generate: File hash (MD5)            │
│  - Extract: Content preview (500 chars) │
└────────┬────────────────────────────────┘
         │
         v
┌─────────────────────────────────────────┐
│  5. ADD TO BATCH QUEUE                  │
│  - Add file to pending uploads list     │
│  - Current batch size: X / 20           │
└────────┬────────────────────────────────┘
         │
         v
    ┌────────────┐
    │ Batch Full?│
    │ (20 files) │
    └──┬─────┬───┘
       │ NO  │ YES
       │     │
       │     v
       │  ┌──────────────────────────────────────────┐
       │  │  6. BATCH UPLOAD TO SUPABASE             │
       │  │  - Upload all 20 XMLs to Storage bucket  │
       │  │  - Path: lovdata-docs/Category/file.xml  │
       │  │  - Get public URLs                       │
       │  └────────┬─────────────────────────────────┘
       │           │
       │           v
       │  ┌──────────────────────────────────────────┐
       │  │  7. DELETE LOCAL FILES                   │
       │  │  - Remove all 20 XML files from disk     │
       │  │  - Free up disk space                    │
       │  └────────┬─────────────────────────────────┘
       │           │
       └───────────┤
                   v
┌─────────────────────────────────────────┐
│  8. SAVE METADATA TO DATABASE           │
│  - Table: lovdata_metadata              │
│  - Fields:                              │
│    • file_name                          │
│    • category / subcategory             │
│    • storage_path                       │
│    • public_url                         │
│    • content_preview (500 chars)        │
│    • file_size, file_hash               │
│    • source_url                         │
└────────┬────────────────────────────────┘
         │
         v
┌─────────────────────────────┐
│  9. INCREMENT COUNTER       │
│  - Total docs: X / 5000     │
│  - Log progress             │
└────────┬────────────────────┘
         │
         v
    ┌────────────┐
    │Max reached?│
    │ (5000 docs)│
    └──┬─────┬───┘
       │ NO  │ YES
       │     │
       │     └─────────────┐
       │                   │
    [Continue]             │
       │                   v
       └──────────> ┌──────────────────────────────┐
                    │  10. FLUSH REMAINING BATCH   │
                    │  - Upload any files < 20     │
                    │  - Delete local files        │
                    └────────┬─────────────────────┘
                             v
                    ┌──────────────────────────────┐
                    │  11. CREATE FINAL EXCEL      │
                    │  - Query all metadata from DB│
                    │  - Create Excel with:        │
                    │    • Filename                │
                    │    • Category/Subcategory    │
                    │    • Content Preview         │
                    │    • Public URL              │
                    │    • File Size               │
                    │  - Add Summary sheet         │
                    │  - Save to: outputs/         │
                    └────────┬─────────────────────┘
                             v
                    ┌──────────────────────────────┐
                    │  12. PRINT SUMMARY           │
                    │  - Success: XXXX             │
                    │  - Failed: XX                │
                    │  - Skipped: XXX              │
                    │  - Total uploaded: XXXX      │
                    └────────┬─────────────────────┘
                             v
                    ┌──────────────────────────────┐
                    │      SCRAPING COMPLETE!      │
                    │                              │
                    │  ✓ XMLs in Supabase Storage  │
                    │  ✓ Metadata in Database      │
                    │  ✓ Excel file created        │
                    │  ✓ No local files remain     │
                    └──────────────────────────────┘
```

---

## DATA FLOW SUMMARY

```
LOVDATA PRO WEBSITE
        │
        │ (Selenium Scraping)
        ↓
LOCAL XML FILES (Temporary)
./scraped_xml/Category/file.xml
        │
        │ (Batch Upload - Every 20 files)
        ↓
SUPABASE STORAGE BUCKET
lovdata-docs/Category/Subcategory/file.xml
        │
        │ (Public URLs Generated)
        ↓
SUPABASE DATABASE
lovdata_metadata table
(Metadata + Content Preview + URLs)
        │
        │ (Final Export)
        ↓
EXCEL FILE
outputs/lovdata_export_YYYYMMDD.xlsx
(All metadata + 500 char content preview)
```

---

## FILE ORGANIZATION

```
lovdata-docs/                           (Supabase Storage Bucket)
├── 01_Laws_and_Regulations/
│   ├── Active_Laws_NL/
│   │   ├── nl-20190301-0002.xml
│   │   ├── nl-20200515-1234.xml
│   │   └── ... (hundreds of files)
│   ├── Central_Regulations_SF/
│   │   └── sf-20201215-5678.xml
│   └── Supreme_Court_HR/
│       └── hr-2025-0123-a.xml
├── Area_01_Labor_Law/
│   └── ... (latest documents)
├── Area_02_Corporate_Law/
│   └── ...
... (35 legal areas total)
└── Area_39_Immigration_Law/
    └── ...
```

---

## BATCH UPLOAD BEHAVIOR

```
Document Counter:  1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20
Local Files:      [█][█][█][█][█][█][█][█][█][█] [█] [█] [█] [█] [█] [█] [█] [█] [█] [█]
                                                                                        │
                                                                                        └→ BATCH FULL!
                                                                                           
                                                                                           ↓
                                                                              Upload all 20 to Supabase
                                                                                           ↓
                                                                              Delete all 20 from local
                                                                                           ↓
                                                                              [Empty] → Ready for next batch

This repeats every 20 documents until scraping completes.
```

---

## FINAL OUTPUT FILES

```
lovdata_scraper_updated/
├── outputs/
│   └── lovdata_export_20260212_143045.xlsx    ← Final Excel with ALL data
├── logs/
│   └── lovdata_scraper_20260212_100530.log    ← Complete execution log
└── scraped_xml/                                ← EMPTY (all files deleted after upload)
```

---

## SUPABASE DASHBOARD VIEW

After completion, your Supabase project will have:

**Storage (lovdata-docs bucket):**
- ~4,000-5,000 XML files
- Organized in folders by category
- Total size: ~1-1.5 GB

**Database (lovdata_metadata table):**
- ~4,000-5,000 rows
- Each row contains:
  - File metadata
  - Public URL to XML
  - 500-char content preview
  - Category classification

**Excel Export:**
- Single file with all metadata
- Content preview column for quick review
- Public URL links (clickable)
- Summary sheet with statistics

---

## RESOURCE USAGE

| Resource | During Scraping | After Upload | Final |
|----------|----------------|--------------|-------|
| Local Disk | ~500 MB (20 files max) | 0 MB | 0 MB |
| Supabase Storage | Growing | ~1-1.5 GB | ~1-1.5 GB |
| Database | Growing | ~50-100 MB | ~50-100 MB |
| Excel File | N/A | N/A | ~20-50 MB |

---

This workflow ensures:
✅ Minimal local disk usage (only 20 files at a time)
✅ All data safely stored in cloud (Supabase)
✅ Easy access via Excel export
✅ Ready for AI/chatbot integration
