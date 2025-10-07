import os
import re
from datetime import datetime
import pandas as pd

# ---------------------------
# Config / aliases de colunas
# ---------------------------

# se a 1ª aba for TABELA ÚNICA, estes nomes são os esperados.
# se na planilha vierem diferentes, mapeie aqui:
COLUMN_ALIAS = {
    # origem (qualquer acento/maiúscula/espaco) -> destino (nome esperado)
    "uf": "estado",
    "est": "estado",
    "regiao": "regiao",
    "região": "regiao",
    "canal_de_aquisicao": "canal",
    "profissão": "profissao",
    "profissao ": "profissao",
    "qtde_vendas": "vendas",
    "ticket": "valor",
    "dt": "data",
    "mes": "mes",
}

# âncoras para quando a 1ª aba tiver BLOCOS agregados (KPIs/tabelas)
ANCHORS = {
    "KPI_TOTAL_LINHAS": "kpi_total_linhas",
    "KPI_TOTAL_VENDAS": "kpi_total_vendas",
    "KPI_TOTAL_VALOR":  "kpi_total_valor",

    "TABELA_POR_ESTADO": "tbl_por_estado",
    "TABELA_POR_REGIAO": "tbl_por_regiao",
    "TABELA_TICKET_PROFISSAO": "tbl_ticket_prof",
    "TABELA_POR_CANAL": "tbl_por_canal",
    "TABELA_TAXA_CANAL": "tbl_taxa_canal",
    "TABELA_PROFISSAO_X_CANAL": "tbl_prof_canal",

    "SERIE_MENSAL": "serie_mensal",
}

# ---------------------------
# Utilidades
# ---------------------------

def _slug_pt(s: str) -> str:
    s = str(s or "").strip().lower()
    rep = {
        "ç":"c","ã":"a","á":"a","à":"a","â":"a","ä":"a",
        "é":"e","ê":"e","è":"e","ë":"e",
        "í":"i","ì":"i","ï":"i",
        "ó":"o","ô":"o","õ":"o","ò":"o","ö":"o",
        "ú":"u","ù":"u","ü":"u",
    }
    for k,v in rep.items():
        s = s.replace(k,v)
    return s.replace(" ", "_")

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    rename = {}
    for c in df.columns:
        key = _slug_pt(c)
        dest = COLUMN_ALIAS.get(key, key)
        rename[c] = dest
    df = df.rename(columns=rename)
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df

# ---------------------------
# Resolvedor de URLs (GSheets)
# ---------------------------

_GSHEET_ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")

def _ensure_export_xlsx_url(src: str) -> str:
    """
    Se receber um link /edit?... público do Google Sheets, converte para
    /export?format=xlsx. Caso já seja export, mantém.
    """
    if not isinstance(src, str):
        return src
    if "docs.google.com/spreadsheets" in src and "/export" not in src:
        m = _GSHEET_ID_RE.search(src)
        if m:
            sheet_id = m.group(1)
            return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    return src

# ---------------------------
# Leitura da 1ª aba com fallback
# ---------------------------

def _read_inputs_first_sheet(src: str | bytes | bytearray) -> pd.DataFrame:
    """
    Tenta ler XLSX (1ª aba). Se der erro (404, engine, rede, permissão),
    cai para CSV usando GOOGLE_SHEET_CSV_URL.
    """
    # 1) tenta XLSX (corrigindo URL se vier /edit)
    src_xlsx = _ensure_export_xlsx_url(src if isinstance(src, str) else "")

    last_err = None

    # Tentativa XLSX
    try:
        if src_xlsx:
            xls = pd.ExcelFile(src_xlsx)
            sheet = xls.sheet_names[0]
            df = pd.read_excel(xls, sheet_name=sheet)
            return _normalize_columns(df)
    except Exception as e:
        last_err = e  # guarda para logging e continuar no fallback

    # Fallback CSV (aba Inputs) – precisa estar público
    csv_url = os.environ.get("GOOGLE_SHEET_CSV_URL", "").strip()
    if csv_url:
        try:
            df_csv = pd.read_csv(csv_url)
            return _normalize_columns(df_csv)
        except Exception as e2:
            last_err = e2

    # Se chegou aqui, não conseguiu nem XLSX nem CSV
    msg = [
        "Falha ao carregar a planilha.",
        f"DATA_XLSX_PATH utilizado: {src_xlsx or '<vazio>'}",
        f"GOOGLE_SHEET_CSV_URL: {csv_url or '<vazio>'}",
        f"Erro mais recente: {repr(last_err)}",
        "Verifique se o arquivo está com permissão 'Qualquer pessoa com o link' (Leitor),",
        "e se as variáveis de ambiente apontam para URLs de /export (xlsx/csv).",
    ]
    raise RuntimeError("\n".join(msg))

def _find_blocks(raw_df: pd.DataFrame):
    """
    Procura âncoras na PRIMEIRA coluna.
    Layout de bloco:
      A: <ÂNCORA>
      A+1: cabeçalhos (linha)
      próximas linhas: dados, até linha completamente vazia.
    """
    if raw_df is None or raw_df.empty:
        return {}

    col0_name = raw_df.columns[0]
    col0 = raw_df[col0_name].astype(str).str.strip()

    blocks = {}
    i, n = 0, len(raw_df)

    while i < n:
        token = col0.iloc[i]
        if token in ANCHORS:
            key = ANCHORS[token]
            head_row = i + 1
            data_row = i + 2
            if head_row < n:
                header = raw_df.iloc[head_row].tolist()
                rows = []
                j = data_row
                while j < n and not raw_df.iloc[j].isna().all():
                    rows.append(raw_df.iloc[j].tolist())
                    j += 1
                if rows:
                    blk = pd.DataFrame(rows, columns=header)
                    blk = _normalize_columns(blk)
                    blocks[key] = blk
                i = j
                continue
        i += 1
    return blocks

# ---------------------------
# Loader principal (1ª aba)
# ---------------------------

def load_inputs_dashboard(src: str | bytes | bytearray | None = None):
    """
    Tenta primeiro tabela 'longa' (colunas mínimas: estado/canal/profissao + valor/vendas).
    Se não bater, procura BLOCOS por âncoras.
    Retorna:
      {'mode':'long','df':DataFrame}  ou
      {'mode':'blocks', <blocos...>}
    """
    if src is None:
        src_env = os.environ.get("DATA_XLSX_PATH")
        if not src_env:
            raise RuntimeError("Defina DATA_XLSX_PATH com o caminho/URL da planilha.")
        src = src_env

    # tenta tabela longa
    df_long = _read_inputs_first_sheet(src)
    cols = set(map(str, df_long.columns))
    has_min = {"estado","canal","profissao"}.issubset(cols)
    has_metric = ("valor" in cols) or ("vendas" in cols)
    if has_min and has_metric:
        return {"mode":"long", "df": df_long}

    # senão, blocos (releia bruto da 1ª aba, sem header, para achar âncoras)
    try:
        xls_url = _ensure_export_xlsx_url(src if isinstance(src, str) else "")
        xls = pd.ExcelFile(xls_url)
        raw = pd.read_excel(xls, sheet_name=0, header=None)
    except Exception:
        # se nem XLSX bruto der, tenta CSV bruto (menos comum pra blocos, mas evita crash)
        csv_url = os.environ.get("GOOGLE_SHEET_CSV_URL", "").strip()
        if not csv_url:
            raise
        raw = pd.read_csv(csv_url, header=None)

    blocks = _find_blocks(raw)

    # pós-processos úteis
    if "tbl_taxa_canal" in blocks:
        b = blocks["tbl_taxa_canal"].copy()
        for c in ("leads","convertidos"):
            if c in b.columns:
                b[c] = pd.to_numeric(b[c], errors="coerce")
        if "taxa_conv" not in b.columns and {"convertidos","leads"}.issubset(b.columns):
            b["taxa_conv"] = (b["convertidos"] / b["leads"]).fillna(0)
        blocks["tbl_taxa_canal"] = b

    if "serie_mensal" in blocks and "mes" in blocks["serie_mensal"].columns:
        s = blocks["serie_mensal"].copy()
        s["mes"] = pd.to_datetime(s["mes"], errors="coerce")
        blocks["serie_mensal"] = s

    return {"mode":"blocks", **blocks}

# ---------------------------
# helpers de agregação (usados nas rotas quando 'long')
# ---------------------------

def group_count(df: pd.DataFrame, by_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[*by_cols, "total"])
    g = df.groupby(by_cols).size().reset_index(name="total")
    return g

def group_sum(df: pd.DataFrame, by_cols: list[str], value_col: str) -> pd.DataFrame:
    if not value_col or value_col not in df.columns:
        return pd.DataFrame(columns=[*by_cols, "total"])
    g = (df.groupby(by_cols)[value_col]
           .sum()
           .reset_index()
           .rename(columns={value_col:"total"}))
    return g

def group_avg(df: pd.DataFrame, by_cols: list[str], value_col: str) -> pd.DataFrame:
    if not value_col or value_col not in df.columns:
        return pd.DataFrame(columns=[*by_cols, "media"])
    g = (df.groupby(by_cols)[value_col]
           .mean()
           .reset_index()
           .rename(columns={value_col:"media"}))
    return g

# ---------------------------
# formatações PT-BR
# ---------------------------

def format_ptbr_int(x):
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return "-"

def format_ptbr_money(x):
    try:
        return f"R$ {float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ -"
