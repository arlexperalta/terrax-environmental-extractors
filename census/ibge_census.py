"""
TerraCore Data Engine — IBGE Censo Agropecuário.

Extracts agricultural census data per municipality from SIDRA API.
Key variable: P_familiar (proportion of family farming).

Source: IBGE Censo Agropecuário 2017 via SIDRA API
    - Table 6778: Establishments by type (familiar / non-familiar)
    - Table 6853: Area by type

Usage:
    from terracore.scripts.modules.ibge_census import extract_p_familiar
    df = extract_p_familiar()
"""

import pandas as pd
import requests
import time
from .base import validate_output, save_processed

SIDRA_BASE = "https://apisidra.ibge.gov.br/values"


def _fetch_sidra(table, variables, classifications="", territorial="n6/all", header="y"):
    """
    Generic SIDRA API fetch.

    Parameters
    ----------
    table : str
        Table number (e.g., '6778').
    variables : str
        Variable codes (e.g., '183' for number of establishments).
    classifications : str
        Classification filter (e.g., '/c829/46302,46303').
    territorial : str
        Territorial level. 'n6/all' = all municipalities.
    header : str
        'y' to include header row.
    """
    url = f"{SIDRA_BASE}/t/{table}/{territorial}/v/{variables}{classifications}/h/{header}"
    print(f"  Fetching {url[:120]}...")

    try:
        r = requests.get(url, timeout=300)
        r.raise_for_status()
        data = r.json()
        # Row 0 = header, rows 1+ = data
        if len(data) < 2:
            print(f"  WARNING: Only {len(data)} rows returned")
            return pd.DataFrame()
        df = pd.DataFrame(data[1:])
        print(f"  Got {len(df)} rows")
        return df
    except Exception as e:
        print(f"  ERROR: {str(e)[:120]}")
        return pd.DataFrame()


def extract_p_familiar():
    """
    Extract proportion of family farming (P_familiar) per municipality.

    Uses Censo Agropecuário 2017, Table 6778:
    - Variable 183: Número de estabelecimentos agropecuários
    - Classification c829: Tipo do estabelecimento
        - 46302: Agricultura familiar (Lei 11.326)
        - 46303: Não agricultura familiar

    Returns
    -------
    DataFrame with columns: codigo_ibg, n_familiar, n_total, P_familiar
    """
    print("Extracting P_familiar from IBGE Censo Agropecuário 2017...")

    # Fetch establishments by type
    # c829/46302 = familiar, c829/46303 = non-familiar
    df_familiar = _fetch_sidra(
        table="6778",
        variables="183",
        classifications="/c829/46302",
    )
    time.sleep(1)

    df_nao_familiar = _fetch_sidra(
        table="6778",
        variables="183",
        classifications="/c829/46303",
    )

    if df_familiar.empty or df_nao_familiar.empty:
        raise RuntimeError("Failed to fetch IBGE census data")

    # Parse: SIDRA returns columns like 'D1C' (territorial code), 'V' (value)
    # Column mapping depends on the response structure
    # Typically: D1C = municipio code (7 digits), V = value
    fam = _parse_sidra_response(df_familiar, "n_familiar")
    nfam = _parse_sidra_response(df_nao_familiar, "n_nao_familiar")

    # Merge
    merged = fam.merge(nfam, on="codigo_ibg", how="outer")
    merged["n_familiar"] = merged["n_familiar"].fillna(0)
    merged["n_nao_familiar"] = merged["n_nao_familiar"].fillna(0)
    merged["n_total"] = merged["n_familiar"] + merged["n_nao_familiar"]
    merged["P_familiar"] = merged["n_familiar"] / merged["n_total"].replace(0, float("nan"))

    print(f"\nP_familiar stats:")
    print(f"  Municipalities: {len(merged)}")
    print(f"  Mean P_familiar: {merged['P_familiar'].mean():.3f}")
    print(f"  Median P_familiar: {merged['P_familiar'].median():.3f}")

    return merged


def _parse_sidra_response(df, value_name):
    """
    Parse SIDRA API response into clean DataFrame.

    SIDRA responses have varying column names depending on the query.
    The municipality code is typically in a column containing territorial codes,
    and the value in column 'V'.
    """
    # Find the municipality code column
    # SIDRA uses numbered columns: D1C, D2C, etc. for codes
    # Or sometimes named columns. Let's check what we got.
    cols = list(df.columns)

    # Strategy: find column with 7-digit codes (IBGE municipal codes)
    code_col = None
    for col in cols:
        sample = df[col].astype(str).iloc[:5]
        if all(s.isdigit() and len(s) == 7 for s in sample):
            code_col = col
            break

    if code_col is None:
        # Fallback: look for column name patterns
        for col in cols:
            if "D" in str(col) and "C" in str(col):
                sample = df[col].astype(str).str.strip()
                if sample.str.match(r"^\d{7}$").mean() > 0.5:
                    code_col = col
                    break

    if code_col is None:
        print(f"WARNING: Could not find municipality code column in: {cols}")
        print(f"Sample data:\n{df.head(2).to_string()}")
        raise ValueError("Cannot parse SIDRA response")

    # Value column is 'V'
    if "V" not in df.columns:
        raise ValueError(f"No 'V' column in SIDRA response. Columns: {cols}")

    result = pd.DataFrame({
        "codigo_ibg": df[code_col].astype(str).str.strip(),
        value_name: pd.to_numeric(df["V"].str.replace(",", "."), errors="coerce"),
    })

    # Remove non-municipal codes (e.g., state totals)
    result = result[result["codigo_ibg"].str.match(r"^\d{7}$")]

    return result


def extract_and_save(filename=None):
    """Extract P_familiar and save to processed directory."""
    df = extract_p_familiar()

    validate_output(
        df,
        expected_cols=["codigo_ibg", "P_familiar"],
        name="IBGE Census",
    )

    fname = filename or "ibge_censo_agro_municipalities.csv"
    save_processed(df, fname, name="IBGE Census")

    return df


if __name__ == "__main__":
    df = extract_and_save()
    print(f"\nDone: {len(df)} municipalities with P_familiar")
