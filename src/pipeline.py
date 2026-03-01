"""
claims-pdf-to-exhibit / src/pipeline.py
========================================
Document AI pipeline that reads PDF claims / EOB reports and outputs:
  - Structured pandas DataFrames
  - Excel exhibit workbooks (Exhibit 1 + Exhibit 2)
  - JSON structured data (for downstream systems)
  - Narrative summary

Architecture (mirrors LedgrAI's lease PDF → amortization schedule):

  PDF Input
      │
      ▼
  [Extractor]  ←── pdfplumber (layout-aware text extraction)
      │
      ▼
  [Classifier] ←── LLM (identify table type: PMPM, membership, EOB, etc.)
      │
      ▼
  [Parser]     ←── LLM structured extraction → JSON schema
      │
      ▼
  [Validator]  ←── Data quality checks, cross-footing, completeness
      │
      ▼
  [Normaliser] ←── Convert to long-format DataFrame
      │
      ├──► Excel Exhibit (openpyxl)
      ├──► JSON output
      └──► Narrative report

Usage:
    python src/pipeline.py --input data/claims_report.pdf
    python src/pipeline.py --input data/ --batch          # batch all PDFs in folder
"""

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber
from anthropic import Anthropic

from .pdf_extractor import extract_pdf_content, classify_pages
from .llm_parser import parse_tables_with_llm, validate_extracted_data
from .exhibit_builder import build_exhibit_from_df
from .normalizer import normalize_to_long_format

# ── Anthropic client ──────────────────────────────────────────────────────────
client = Anthropic()

OUTPUT_DIR = Path("output")


def run_pipeline(pdf_path: str, output_dir: str = "output") -> dict[str, Any]:
    """
    Full PDF → Exhibit pipeline.
    Returns a dict with keys: df, excel_path, json_path, narrative
    """
    pdf_path = Path(pdf_path)
    out_dir  = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = pdf_path.stem

    print(f"\n📄 Processing: {pdf_path.name}")

    # ── Step 1: Extract text + tables from PDF ─────────────────────────────
    print("  [1/5] Extracting PDF content...")
    raw_content = extract_pdf_content(str(pdf_path))
    page_types  = classify_pages(raw_content)
    print(f"        {len(raw_content['pages'])} pages | "
          f"types: {', '.join(set(page_types.values()))}")

    # ── Step 2: LLM structured extraction ─────────────────────────────────
    print("  [2/5] Parsing tables with LLM...")
    extracted = parse_tables_with_llm(raw_content, client)
    print(f"        Extracted {len(extracted.get('records', []))} records")

    # ── Step 3: Validate ───────────────────────────────────────────────────
    print("  [3/5] Validating extracted data...")
    validation = validate_extracted_data(extracted)
    if validation["warnings"]:
        for w in validation["warnings"]:
            print(f"        ⚠️  {w}")
    if validation["errors"]:
        for e in validation["errors"]:
            print(f"        ❌  {e}")

    # ── Step 4: Normalise → DataFrame ──────────────────────────────────────
    print("  [4/5] Normalising to long format...")
    df = normalize_to_long_format(extracted)
    print(f"        DataFrame: {df.shape[0]} rows × {df.shape[1]} columns")

    # ── Step 5: Export ─────────────────────────────────────────────────────
    print("  [5/5] Exporting exhibits...")
    excel_path = out_dir / f"{stem}_exhibit.xlsx"
    json_path  = out_dir / f"{stem}_data.json"

    build_exhibit_from_df(df, str(excel_path), source_pdf=pdf_path.name)
    json_path.write_text(json.dumps(extracted, indent=2, default=str), encoding="utf-8")

    narrative = _generate_narrative(df, extracted, client)
    narrative_path = out_dir / f"{stem}_report.md"
    narrative_path.write_text(narrative, encoding="utf-8")

    print(f"\n  ✅ Excel exhibit → {excel_path}")
    print(f"  ✅ JSON data     → {json_path}")
    print(f"  ✅ Report        → {narrative_path}\n")

    return {
        "df":         df,
        "excel_path": str(excel_path),
        "json_path":  str(json_path),
        "narrative":  narrative,
        "validation": validation,
    }


def run_batch(folder: str, output_dir: str = "output") -> list[dict]:
    """Process all PDFs in a folder."""
    pdfs = list(Path(folder).glob("*.pdf"))
    if not pdfs:
        print(f"No PDF files found in {folder}")
        return []

    print(f"\n📂 Batch processing {len(pdfs)} PDFs from {folder}")
    results = []
    for pdf in pdfs:
        try:
            result = run_pipeline(str(pdf), output_dir)
            results.append({"file": pdf.name, "status": "success", **result})
        except Exception as e:
            print(f"  ❌ Failed: {pdf.name} — {e}")
            results.append({"file": pdf.name, "status": "error", "error": str(e)})

    success = sum(1 for r in results if r["status"] == "success")
    print(f"\n📊 Batch complete: {success}/{len(pdfs)} succeeded")
    return results


def _generate_narrative(df: pd.DataFrame, extracted: dict, client: Anthropic) -> str:
    """Use Claude to write a narrative summary of the extracted claims data."""
    if df.empty:
        return "No data extracted from the PDF."

    data_summary = df.describe().to_string() if not df.empty else "No data"
    records_preview = df.head(6).to_json(orient="records", date_format="iso")

    prompt = f"""You are an actuarial analyst. Based on the following extracted claims data,
write a concise professional narrative (3-5 paragraphs) suitable for a client report.

Data summary:
{data_summary}

Sample records (first 6):
{records_preview}

Extraction metadata:
- Source type: {extracted.get('document_type', 'unknown')}
- Periods detected: {extracted.get('periods', 'unknown')}
- LOBs detected: {extracted.get('lines_of_business', 'unknown')}
- Providers detected: {extracted.get('providers', 'unknown')}

Write a professional narrative with sections: Executive Summary, Key Findings, Notable Observations.
Use specific numbers where available. Be concise and client-ready."""

    response = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PDF Claims Report → Excel Exhibit")
    parser.add_argument("--input",   required=True, help="PDF file or folder (with --batch)")
    parser.add_argument("--output",  default="output", help="Output directory")
    parser.add_argument("--batch",   action="store_true", help="Process all PDFs in folder")
    args = parser.parse_args()

    if args.batch:
        run_batch(args.input, args.output)
    else:
        run_pipeline(args.input, args.output)
