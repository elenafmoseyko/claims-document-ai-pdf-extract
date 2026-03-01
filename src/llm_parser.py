"""
src/llm_parser.py
=================
Uses Claude to extract structured data from raw PDF text/tables.

Two-step approach (mirrors LedgrAI lease extraction):
  Step 1 — Classify:  identify what data is present
  Step 2 — Extract:   pull structured records with a JSON schema prompt

The JSON schema is strict, enabling downstream validation and
direct ingestion into pandas / Excel without manual cleanup.
"""

import json
import re
from typing import Any

from anthropic import Anthropic


# ── JSON output schema ────────────────────────────────────────────────────────
EXTRACTION_SCHEMA = {
    "document_type": "string (pmpm_summary | eob | detail_claims | enrollment | mixed)",
    "document_period": "string e.g. '2024-01' or '2024-Q1' or 'Full Year 2024'",
    "periods": "list of strings e.g. ['2024-01', '2024-02', ...]",
    "lines_of_business": "list of strings e.g. ['HMO/POS', 'PPO', 'Medicaid', 'Medicare']",
    "providers": "list of strings e.g. ['Provider A', 'Provider B', 'Provider C']",
    "records": [
        {
            "month":       "YYYY-MM-DD (first day of month)",
            "lob":         "line of business string",
            "provider":    "provider name string",
            "members":     "integer or null",
            "pmpm":        "float (per-member per-month spend) or null",
            "paid_claims": "float (total paid claims) or null",
            "data_type":   "pmpm | paid_claims | enrollment | eob_line",
        }
    ],
    "extraction_notes": "string — any caveats, partial extraction warnings, assumptions made",
}

CLASSIFY_PROMPT = """You are a healthcare data extraction specialist.

Analyze this text extracted from a PDF claims/enrollment report and answer:
1. What type of document is this? (pmpm_summary, eob, detail_claims, enrollment, mixed)
2. What time period(s) does it cover?
3. What lines of business are present? (HMO/POS, PPO, Medicaid, Medicare, etc.)
4. What provider labels are present?
5. Does it contain PMPM tables, raw claim lines, or both?

Text (first 2000 chars):
{text_preview}

Respond in JSON:
{{
  "document_type": "...",
  "periods": [...],
  "lines_of_business": [...],
  "providers": [...],
  "has_pmpm_tables": true/false,
  "has_raw_claims": true/false,
  "confidence": 0-100
}}"""

EXTRACT_PROMPT = """You are a healthcare actuarial data extraction AI.

Extract ALL structured claims and enrollment data from this PDF content into
the exact JSON schema below. Be precise with numbers — do not round or estimate.

SCHEMA:
{schema}

PDF CONTENT:
{content}

INSTRUCTIONS:
- Extract every data point you can find
- For PMPM tables: each row becomes one record per (month, lob, provider)
- For EOB lines: each claim line becomes one record with data_type="eob_line"
- If a value is missing/unclear, use null (not 0)
- Dates must be YYYY-MM-DD format (use first of month for monthly data)
- Standardise LOB names: HMO/POS, PPO, Medicaid, Medicare
- Include extraction_notes if you made any assumptions

Return ONLY valid JSON matching the schema above. No markdown, no explanation."""


def parse_tables_with_llm(
    content: dict[str, Any],
    client: Anthropic,
    model: str = "claude-3-5-sonnet-20241022",
) -> dict[str, Any]:
    """
    Main extraction function.
    Classifies the document, then extracts structured records.
    """
    full_text = content.get("full_text", "")
    if not full_text.strip():
        return {"records": [], "document_type": "empty", "extraction_notes": "No text found in PDF"}

    # ── Step 1: Classify ───────────────────────────────────────────────────
    classify_resp = client.messages.create(
        model=model,
        max_tokens=500,
        messages=[{
            "role": "user",
            "content": CLASSIFY_PROMPT.format(text_preview=full_text[:2000]),
        }],
    )
    classification = _parse_json_response(classify_resp.content[0].text)

    # ── Step 2: Extract structured data ────────────────────────────────────
    # Send full content but cap at ~12k chars to avoid token limits
    content_for_llm = _prepare_content_for_llm(content)

    extract_resp = client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": EXTRACT_PROMPT.format(
                schema=json.dumps(EXTRACTION_SCHEMA, indent=2),
                content=content_for_llm,
            ),
        }],
    )
    extracted = _parse_json_response(extract_resp.content[0].text)

    # Merge classification metadata
    extracted.update({
        "document_type":    classification.get("document_type", "unknown"),
        "periods":          classification.get("periods", []),
        "lines_of_business": classification.get("lines_of_business", []),
        "providers":        classification.get("providers", []),
        "confidence":       classification.get("confidence", 0),
    })

    return extracted


def validate_extracted_data(extracted: dict) -> dict[str, list]:
    """
    Validate extracted records.
    Returns {"warnings": [...], "errors": [...]}
    """
    warnings = []
    errors   = []
    records  = extracted.get("records", [])

    if not records:
        errors.append("No records extracted from document")
        return {"warnings": warnings, "errors": errors}

    # Check required fields
    required = ["month", "lob", "provider"]
    for i, rec in enumerate(records):
        for field in required:
            if not rec.get(field):
                warnings.append(f"Record {i}: missing '{field}' field")

    # Check for null PMPM when expected
    pmpm_null = sum(1 for r in records if r.get("pmpm") is None)
    if pmpm_null > len(records) * 0.5:
        warnings.append(f"{pmpm_null}/{len(records)} records have null PMPM — "
                        "check if document uses a different spend metric")

    # Check date format
    bad_dates = [r.get("month") for r in records
                 if r.get("month") and not re.match(r"^\d{4}-\d{2}-\d{2}$", str(r.get("month", "")))]
    if bad_dates:
        warnings.append(f"Non-standard date formats found: {bad_dates[:3]}")

    # Cross-foot check: paid_claims ≈ members × pmpm
    cross_foot_errors = 0
    for rec in records:
        if all(rec.get(k) for k in ["members", "pmpm", "paid_claims"]):
            expected = rec["members"] * rec["pmpm"]
            actual   = rec["paid_claims"]
            if abs(expected - actual) / max(actual, 1) > 0.05:   # >5% discrepancy
                cross_foot_errors += 1

    if cross_foot_errors > 0:
        warnings.append(f"{cross_foot_errors} records have paid_claims ≠ members × pmpm "
                        "(>5% discrepancy) — may indicate rounding or data quality issues")

    return {"warnings": warnings, "errors": errors}


# ── Helpers ────────────────────────────────────────────────────────────────────
def _parse_json_response(text: str) -> dict:
    """Safely parse JSON from LLM response, stripping markdown fences."""
    text = text.strip()
    # Remove markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Attempt to extract first JSON object
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return {"records": [], "extraction_notes": "Failed to parse LLM JSON response"}


def _prepare_content_for_llm(content: dict, max_chars: int = 12000) -> str:
    """Prepare a compact representation of the PDF content for the LLM."""
    parts = []
    for page in content.get("pages", []):
        page_text = page.get("text", "").strip()
        if page_text:
            parts.append(f"=== PAGE {page['page_number']} ===\n{page_text}")

        for tbl in page.get("tables", []):
            parts.append(f"--- TABLE (page {page['page_number']}) ---")
            for row in tbl:
                parts.append(" | ".join(str(c) for c in row))

    full = "\n".join(parts)
    if len(full) > max_chars:
        full = full[:max_chars] + f"\n[... truncated at {max_chars} chars ...]"
    return full
