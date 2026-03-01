A Document AI pipeline that reads PDF medical claims / EOB reports and outputs
structured Excel exhibits, JSON data, and narrative summaries — using Claude.

---

## Architecture

```
PDF Input
    │
    ▼
[Extractor]   ── pdfplumber: layout-aware text + table extraction
    │
    ▼
[Classifier]  ── Identify page types (PMPM table, EOB, narrative)
    │
    ▼
[LLM Parser]  ── Structured JSON extraction with schema enforcement
    │
    ▼
[Validator]   ── cross-foot checks, completeness, format validation
    │
    ▼
[Normaliser]  ── long-format pandas DataFrame
    │
    ├──► Excel Exhibit (Exhibit 1: PMPM Summary + Exhibit 2: LOB Detail)
    ├──► JSON data file (for API / downstream ingestion)
    └──► Markdown narrative report
```

---

## Quick Start

### Demo (no PDF or API key needed)
```bash
git clone https://github.com/YOUR_USERNAME/claims-pdf-to-exhibit
cd claims-pdf-to-exhibit
pip install -r requirements.txt

# Run with the included Excel demo data
python demo_from_excel.py --input data/Company_ABC_Claims_Data.xlsx
```

## What PDFs does it handle?

| PDF Type | Support |
|----------|---------|
| Text-based insurer reports (most common) | ✅ Full |
| EOB (Explanation of Benefits) documents | ✅ Full |
| Multi-column actuarial exhibits | ✅ Full |
| Scanned/image PDFs | ⚠️ Partial (add pytesseract for OCR) |
| Password-protected PDFs | ❌ Not supported |

---

## Output

### Excel Exhibit
- **Cover sheet** — source metadata, date range, LOB + provider list
- **Exhibit 1 – PMPM Summary** — monthly trend table + line chart per provider
- **Exhibit 2 – LOB Detail** — month-by-month PMPM + membership by LOB

### JSON
Structured record array matching the extraction schema, suitable for:
- API ingestion
- Database loading
- Further downstream processing

### Markdown Report
LLM-generated narrative with Executive Summary, Key Findings,
Notable Observations — ready for client delivery.

---

## Project Structure
```
claims-pdf-to-exhibit/
├── src/
│   ├── pipeline.py        # Main orchestrator
│   ├── pdf_extractor.py   # pdfplumber extraction + page classification
│   ├── llm_parser.py      # Claude-powered structured extraction
│   ├── normalizer.py      # JSON → long-format DataFrame
│   └── exhibit_builder.py # DataFrame → Excel exhibit (openpyxl)
├── demo_from_excel.py     # Demo: test exhibit builder without a PDF
├── data/                  # Place PDF inputs here
├── output/                # Generated files land here
├── tests/
└── requirements.txt
```

---

## Why this matters

Traditional PDF → Excel workflows require:
- Custom regex per document template
- Manual QA of every extraction
- Hours of reformatting

---
