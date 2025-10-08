import os
import re
import pandas as pd

# ===========================
# Normalização de colunas
# ===========================

COLUMN_ALIAS = {
    # dimensões
    "uf": "estado",
    "est": "estado",
    "estado": "estado",
    "regiao": "regiao",
    "região": "regiao",
    "canal": "canal",
    "canal_de_aquisicao": "canal",
    "profissao": "profissao",
    "profissão": "profissao",
    "profissao ": "profissao",

    # métricas
    "vendas": "vendas",
    "qtde_vendas": "vendas",
    "qtd_vendas": "vendas",
    "qtd": "vendas",
    "quantidade": "vendas",
    "valor": "valor",
    "faturamento": "valor",
    "receita": "valor",
    "ticket": "valor",  # se vier só 'ticket', tratamos como valor
    "valor_total": "valor",
    "total": "valor",

    # séries
    "dt": "data",
    "data": "data",
    "mes": "mes",
    "mês": "mes",
}

# anchors (só usados se não acharmos uma planilha “longa”)
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

# ===========================
# Helpers
# ===========================

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
    s = re.sub(r"\s+", "_", s)
    return s

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    rename = {}
    for c in df.columns:
        key = _slug_pt(c)
        dest = COLUMN_ALIAS.get(key, key)
        rename[c] = dest
    df = df.rename(columns=rename)
    # tipos comuns
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"], errors="coerce")
    return df

def _is_long_candidate(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    cols = set(map(str, df.columns))
    dims = {"estado", "regiao", "canal", "profissao"}
    has_any_dim = len(cols.intersection(dims)) >= 1
    has_metric = ("valor" in cols) or ("vendas" in cols)
    return has_any_dim and has_metric

def _looks_like_google_sheets(url: str) -> bool:
    return "docs.google.com/spreadsheets" in (url or "")

def _to_gsheets_export(url: str, fmt: str = "xlsx") -> str:
    """
    Converte qualquer link de edição/visualização do Google Sheets em export.
    - /edit?gid=...  -> /export?format=xlsx
    - se já vier format=csv, mantém CSV
    """
    if "export?format=" in url:
        return url  # já é export
    base = url.split("/edit")[0]
    return f"{base}/export?format={fmt}"

def _read_any(src: str | bytes | bytearray) -> dict:
    """
    Lê o arquivo/URL em memória:
    - Se for CSV (format=csv), retorna {"type":"csv","df":DataFrame}
    - Caso contrário, tenta Excel e retorna {"type":"excel","xls":ExcelFile}
    """
    if isinstance(src, (bytes, bytearray)):
        # tentar Excel direto do binário
        xls = pd.ExcelFile(src)
        return {"type": "excel", "xls": xls}

    url = str(src)

    # Google Sheets: sempre tentar export XLSX por padrão
    if _looks_like_google_sheets(url) and "export?format=" not in url:
        url = _to_gsheets_export(url, fmt="xlsx")

    # CSV?
    if "export?format=csv" in url or url.lower().endswith(".csv"):
        df = pd.read_csv(url)
        return {"type": "csv", "df": df}

    # fallback Excel
    xls = pd.ExcelFile(url)
    return {"type": "excel", "xls": xls}

# ===========================
# Leitura “longa” (todas as abas)
# ===========================

def _read_best_long_df(src: str | bytes | bytearray) -> pd.DataFrame | None:
    """
    Procura, entre TODAS as abas, uma que pareça “longa” (dimensões + métrica).
    Retorna o primeiro match normalizado. Se nada servir, retorna None.
    """
    handle = _read_any(src)

    if handle["type"] == "csv":
        df = _normalize_columns(handle["df"])
        return df if _is_long_candidate(df) else None

    xls: pd.ExcelFile = handle["xls"]

    # Preferências de nome (se existirem)
    preferred_names = [
        "Base", "Resultado", "Base por Estado", "Região", "Estado", "Dados", "Data"
    ]

    # 1) tentar preferidas (ordem)
    for name in preferred_names:
        if name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=name)
            df = _normalize_columns(df)
            if _is_long_candidate(df):
                return df

    # 2) varrer todas as abas
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        df = _normalize_columns(df)
        if _is_long_candidate(df):
            return df

    return None

# ===========================
# Parser de blocos (fallback)
# ===========================

def _find_blocks(raw_df: pd.DataFrame):
    """
    Procura âncoras na PRIMEIRA coluna.
    Layout esperado:
      A: <ÂNCORA>
      A+1: cabeçalhos (linha)
      próximas linhas: dados, até linha completamente vazia.
    """
    if raw_df is None or raw_df.empty:
        return {}

    # usar primeira coluna como “tokens”
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

# ===========================
# Loader principal
# ===========================

def load_inputs_dashboard(src: str | bytes | bytearray | None = None):
    """
    Estratégia:
      1) Procurar uma aba “longa” em QUALQUER aba (dimensões + métrica).
         - Se encontrar, retorna {'mode':'long','df': DataFrame}
      2) Caso não encontre, tentar “blocos” na 1ª aba (anchors).
         - Retorna {'mode':'blocks', <blocos...>}
    """
    if src is None:
        # aceita tanto DATA_XLSX_PATH (xlsx) quanto GOOGLE_SHEET_CSV_URL (csv)
        src = os.environ.get("DATA_XLSX_PATH") or os.environ.get("GOOGLE_SHEET_CSV_URL")
        if not src:
            raise RuntimeError("Defina DATA_XLSX_PATH ou GOOGLE_SHEET_CSV_URL com o caminho/URL da planilha.")

    # 1) LONG MODE (qualquer aba)
    df_long = _read_best_long_df(src)
    if df_long is not None and _is_long_candidate(df_long):
        return {"mode": "long", "df": df_long}

    # 2) BLOCKS MODE (fallback) — tenta ler a 1ª aba sem header para varrer anchors
    handle = _read_any(src)
    if handle["type"] == "csv":
        raw = handle["df"].copy()
        raw.columns = raw.columns.astype(str)  # segurança
        raw = raw.reset_index(drop=True)
    else:
        xls: pd.ExcelFile = handle["xls"]
        first = xls.sheet_names[0]
        raw = pd.read_excel(xls, sheet_name=first, header=None)

    blocks = _find_blocks(raw)

    # pós-processos úteis
    if "tbl_taxa_canal" in blocks:
        b = blocks["tbl_taxa_canal"].copy()
        for c in ("leads", "convertidos"):
            if c in b.columns:
                b[c] = pd.to_numeric(b[c], errors="coerce")
        if "taxa_conv" not in b.columns and {"convertidos", "leads"}.issubset(b.columns):
            b["taxa_conv"] = (b["convertidos"] / b["leads"]).fillna(0)
        blocks["tbl_taxa_canal"] = b

    if "serie_mensal" in blocks and "mes" in blocks["serie_mensal"].columns:
        s = blocks["serie_mensal"].copy()
        s["mes"] = pd.to_datetime(s["mes"], errors="coerce")
        blocks["serie_mensal"] = s

    return {"mode": "blocks", **blocks}

# ===========================
# Agregadores (usados nas rotas)
# ===========================

def group_count(df: pd.DataFrame, by_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[*by_cols, "total"])
    g = df.groupby(by_cols).size().reset_index(name="total")
    return g

def group_sum(df: pd.DataFrame, by_cols: list[str], value_col: str) -> pd.DataFrame:
    if not value_col or value_col not in df.columns:
        return pd.DataFrame(columns=[*by_cols, "total"])
    g = (
        df.groupby(by_cols)[value_col]
          .sum()
          .reset_index()
          .rename(columns={value_col: "total"})
    )
    return g

def group_avg(df: pd.DataFrame, by_cols: list[str], value_col: str) -> pd.DataFrame:
    if not value_col or value_col not in df.columns:
        return pd.DataFrame(columns=[*by_cols, "media"])
    g = (
        df.groupby(by_cols)[value_col]
          .mean()
          .reset_index()
          .rename(columns={value_col: "media"})
    )
    return g

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
