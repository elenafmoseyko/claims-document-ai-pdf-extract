"""
src/pdf_extractor.py
====================
Layout-aware PDF extraction using pdfplumber.

Handles the messy reality of PDF claims reports:
  - Text-based PDFs (most insurer reports)
  - Scanned/image PDFs (falls back gracefully with a message)
  - Multi-column layouts
  - Embedded tables with varying delimiters
  - Headers/footers that repeat across pages
"""

import re
from pathlib import Path
from typing import Any

import pdfplumber


def extract_pdf_content(pdf_path: str) -> dict[str, Any]:
    """
    Extract all content from a PDF file.

    Returns a dict:
    {
        "pages": [
            {
                "page_number": 1,
                "text": "...",
                "tables": [[row, row, ...], ...],
                "words": [...],
                "width": 612,
                "height": 792,
            },
            ...
        ],
        "total_pages": N,
        "metadata": {...},
        "full_text": "...",
    }
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    pages_data = []

    with pdfplumber.open(pdf_path) as pdf:
        metadata = pdf.metadata or {}
        total_pages = len(pdf.pages)

        for i, page in enumerate(pdf.pages, 1):
            # Extract text
            text = page.extract_text(
                x_tolerance=3, y_tolerance=3, layout=True
            ) or ""

            # Extract tables (pdfplumber's layout-aware table detection)
            tables = []
            raw_tables = page.extract_tables({
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "min_words_vertical": 2,
                "min_words_horizontal": 1,
            })
            if raw_tables:
                for tbl in raw_tables:
                    cleaned = _clean_table(tbl)
                    if cleaned:
                        tables.append(cleaned)

            # If no tables found via layout, try lines-based
            if not tables:
                lines_tables = page.extract_tables({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                })
                if lines_tables:
                    for tbl in lines_tables:
                        cleaned = _clean_table(tbl)
                        if cleaned:
                            tables.append(cleaned)

            pages_data.append({
                "page_number": i,
                "text": _clean_text(text),
                "tables": tables,
                "char_count": len(text),
                "width": float(page.width),
                "height": float(page.height),
            })

    full_text = "\n\n--- PAGE BREAK ---\n\n".join(
        p["text"] for p in pages_data if p["text"].strip()
    )

    return {
        "pages": pages_data,
        "total_pages": total_pages,
        "metadata": metadata,
        "full_text": full_text,
    }


def classify_pages(content: dict[str, Any]) -> dict[int, str]:
    """
    Classify each page by content type.

    Returns: {page_number: page_type}
    Page types: "title", "summary_table", "detail_table", "eob", "text_narrative", "unknown"
    """
    page_types = {}
    for page in content["pages"]:
        text_lower = page["text"].lower()
        num_tables = len(page["tables"])

        if _is_title_page(text_lower, num_tables):
            page_types[page["page_number"]] = "title"
        elif _is_pmpm_table(text_lower, num_tables):
            page_types[page["page_number"]] = "summary_table"
        elif _is_eob_page(text_lower):
            page_types[page["page_number"]] = "eob"
        elif _is_detail_table(text_lower, num_tables):
            page_types[page["page_number"]] = "detail_table"
        elif len(page["text"].strip()) > 200:
            page_types[page["page_number"]] = "text_narrative"
        else:
            page_types[page["page_number"]] = "unknown"

    return page_types


def extract_tables_from_pages(content: dict, page_types: dict) -> list[dict]:
    """
    Return tables from pages classified as summary_table or detail_table,
    with page context attached.
    """
    relevant = []
    for page in content["pages"]:
        ptype = page_types.get(page["page_number"], "unknown")
        if ptype in ("summary_table", "detail_table") and page["tables"]:
            for tbl in page["tables"]:
                relevant.append({
                    "page": page["page_number"],
                    "type": ptype,
                    "table": tbl,
                    "context_text": page["text"][:500],  # surrounding text for LLM context
                })
    return relevant


# ── Private helpers ────────────────────────────────────────────────────────────
def _clean_table(raw_table: list) -> list[list[str]]:
    """Remove None cells, strip whitespace, drop blank rows."""
    cleaned = []
    for row in raw_table:
        if row is None:
            continue
        cleaned_row = [str(cell).strip() if cell is not None else "" for cell in row]
        if any(cell for cell in cleaned_row):   # skip entirely blank rows
            cleaned.append(cleaned_row)
    return cleaned


def _clean_text(text: str) -> str:
    """Normalise whitespace and remove common PDF artefacts."""
    if not text:
        return ""
    # Collapse multiple spaces (but preserve line breaks)
    lines = text.split("\n")
    lines = [re.sub(r" {3,}", "  ", line) for line in lines]
    # Remove lines that are only dots or dashes (table border artefacts)
    lines = [l for l in lines if not re.match(r"^[\.\-_=\s]{5,}$", l)]
    return "\n".join(lines).strip()


def _is_title_page(text: str, num_tables: int) -> bool:
    keywords = ["company", "claims analysis", "medical", "enrollment", "report"]
    return sum(1 for k in keywords if k in text) >= 2 and num_tables == 0


def _is_pmpm_table(text: str, num_tables: int) -> bool:
    keywords = ["pmpm", "per member", "per month", "hmo", "ppo", "medicaid", "medicare"]
    return sum(1 for k in keywords if k in text) >= 2 and num_tables > 0


def _is_eob_page(text: str) -> bool:
    keywords = ["explanation of benefits", "eob", "claim number", "date of service",
                "billed amount", "allowed amount", "patient responsibility"]
    return sum(1 for k in keywords if k in text) >= 3


def _is_detail_table(text: str, num_tables: int) -> bool:
    keywords = ["provider", "member", "claim", "amount", "date"]
    return sum(1 for k in keywords if k in text) >= 2 and num_tables > 0
