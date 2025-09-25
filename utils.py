import os
import pandas as pd

def load_dataframe():
    csv_url = os.getenv("GOOGLE_SHEET_CSV_URL", "").strip()
    xlsx_path = os.getenv("LOCAL_XLSX_PATH", "").strip()

    if csv_url:
        try:
            df = pd.read_csv(csv_url)
            return sanitize_dataframe(df)
        except Exception as e:
            print(f"[WARN] Falha ao ler CSV do Google Sheets: {e}")

    if xlsx_path and os.path.exists(xlsx_path):
        try:
            df = pd.read_excel(xlsx_path)
            return sanitize_dataframe(df)
        except Exception as e:
            print(f"[WARN] Falha ao ler XLSX local: {e}")

    print("[WARN] Nenhuma fonte de dados disponível. Retornando DF vazio.")
    return pd.DataFrame()

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    for c in ["data", "dt", "date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    for c in ["valor", "vendas", "quantidade", "qtd", "ticket_medio", "leads", "convertidos"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["estado", "regiao", "profissao", "canal"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df

def has_cols(df, cols):
    return all(c in df.columns for c in cols)

def group_count(df, by_cols):
    if not df.empty and has_cols(df, by_cols):
        return (df
                .assign(_ones=1)
                .groupby(by_cols, dropna=False)["_ones"]
                .sum()
                .reset_index()
                .rename(columns={"_ones":"total"}))
    return pd.DataFrame(columns=[*by_cols, "total"])

def group_sum(df, by_cols, value_col):
    if not df.empty and has_cols(df, [*by_cols, value_col]):
        return (df
                .groupby(by_cols, dropna=False)[value_col]
                .sum()
                .reset_index()
                .rename(columns={value_col:"total"}))
    return pd.DataFrame(columns=[*by_cols, "total"])

def group_avg(df, by_cols, value_col):
    if not df.empty and has_cols(df, [*by_cols, value_col]):
        return (df
                .groupby(by_cols, dropna=False)[value_col]
                .mean()
                .reset_index()
                .rename(columns={value_col:"media"}))
    return pd.DataFrame(columns=[*by_cols, "media"])

def format_ptbr_int(n):
    try:
        return f"{int(n):,}".replace(",", ".")
    except:
        return "-"

def format_ptbr_money(n):
    try:
        return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "-"
