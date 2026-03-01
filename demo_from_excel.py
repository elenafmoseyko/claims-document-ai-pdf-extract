"""
demo_from_excel.py
==================
Demo script: converts the Company ABC Excel file through the normalizer
and exhibit builder WITHOUT needing a real PDF or API key.

This lets you test the full output pipeline immediately.

Usage:
    python demo_from_excel.py --input data/Company_ABC_2018_Claims_and_Enrollment_Data.xlsx
"""

import argparse
import sys
from pathlib import Path

# Make src importable
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from src.normalizer import normalize_to_long_format
from src.exhibit_builder import build_exhibit_from_df

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

OFFSETS = {"HMO/POS": 2, "PPO": 7, "Medicaid": 12, "Medicare": 17}
PROVIDERS = ["Provider A", "Provider B", "Provider C"]


def excel_to_long(path: str) -> pd.DataFrame:
    """Read the wide-format Company ABC Excel and return long-format DataFrame."""
    raw = pd.read_excel(path, sheet_name="Exhibit 2 - Detail", header=None)
    records = []
    for i, (_, row) in enumerate(raw.iloc[8:20].iterrows()):
        month = pd.to_datetime(row.iloc[0])
        for lob, col in OFFSETS.items():
            members = float(row.iloc[col]) if row.iloc[col] else None
            for j, provider in enumerate(PROVIDERS):
                pmpm = float(row.iloc[col + 1 + j]) if row.iloc[col + 1 + j] else None
                records.append({
                    "month":       month,
                    "lob":         lob,
                    "provider":    provider,
                    "members":     members,
                    "pmpm":        pmpm,
                    "paid_claims": members * pmpm if members and pmpm else None,
                    "data_type":   "pmpm",
                })
    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True)
    parser.add_argument("--output", default="output/demo_exhibit.xlsx")
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    print(f"\n📊 Loading: {args.input}")
    df = excel_to_long(args.input)
    print(f"   ✓ {len(df)} records | {df['lob'].nunique()} LOBs | {df['provider'].nunique()} providers")

    print(f"\n📁 Building exhibit: {args.output}")
    build_exhibit_from_df(df, args.output, source_pdf="Company_ABC_Excel_Demo")
    print(f"   ✓ Saved → {args.output}")

    print("\nPreview (first 5 rows):")
    print(df.head().to_string(index=False))
    print()


if __name__ == "__main__":
    main()
