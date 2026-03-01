"""
src/normalizer.py  +  src/exhibit_builder.py combined
======================================================
normalizer:      extracted JSON → clean pandas DataFrame
exhibit_builder: DataFrame → formatted Excel workbook
"""

# ═══════════════════════════════════════════════════════════════
#  NORMALIZER
# ═══════════════════════════════════════════════════════════════
import numpy as np
import pandas as pd


def normalize_to_long_format(extracted: dict) -> pd.DataFrame:
    """
    Convert LLM-extracted JSON records into a clean long-format DataFrame.
    Schema: month | lob | provider | members | pmpm | paid_claims | data_type
    """
    records = extracted.get("records", [])
    if not records:
        return pd.DataFrame(columns=["month", "lob", "provider", "members", "pmpm",
                                     "paid_claims", "data_type"])

    df = pd.DataFrame(records)

    # Coerce types
    df["month"] = pd.to_datetime(df.get("month"), errors="coerce")
    for col in ["members", "pmpm", "paid_claims"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derive paid_claims from members × pmpm if missing
    if "paid_claims" not in df.columns:
        df["paid_claims"] = None
    mask = df["paid_claims"].isna() & df["members"].notna() & df["pmpm"].notna()
    df.loc[mask, "paid_claims"] = df.loc[mask, "members"] * df.loc[mask, "pmpm"]

    # Standardise LOB names
    lob_map = {
        "hmo": "HMO/POS", "hmo/pos": "HMO/POS", "hmo pos": "HMO/POS",
        "ppo": "PPO",
        "medicaid": "Medicaid", "mcd": "Medicaid",
        "medicare": "Medicare",  "mcr": "Medicare",
    }
    if "lob" in df.columns:
        df["lob"] = df["lob"].str.strip().str.lower().map(
            lambda x: lob_map.get(x, x.title() if pd.notna(x) else x)
        )

    # Drop rows with no usable data
    df = df.dropna(subset=["month"])
    df = df.sort_values(["month", "lob", "provider"]).reset_index(drop=True)

    return df
