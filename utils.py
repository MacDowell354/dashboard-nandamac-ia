import os
from io import BytesIO
from datetime import datetime
import pandas as pd
import requests

# ---------------------------
# Config / aliases de colunas
# ---------------------------

COLUMN_ALIAS = {
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

# títulos (em A) que marcam INÍCIO de bloco na sua 1ª aba
SECTION_TOKENS = {
    "PROFISSOES": "tbl_prof_canal",
    "ESTADO X PROFISSÃO": "tbl_estado_prof",
    "REGIÃO POR PROFISSÃO": "tbl_regiao_prof",
    "vendas_realizadas": "vendas",
    "progecao_de_resultados": "projecao_resultados",
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
    # normaliza campos típicos
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df

def _is_blank_row(sr: pd.Series) -> bool:
    return sr.isna().all() or (sr.astype(str).str.strip() == "").all()

def _download_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content

# ---------------------------
# Leituras
# ---------------------------

def _read_csv_url_if_any() -> pd.DataFrame | None:
    """
    Se GOOGLE_SHEET_CSV_URL estiver definido, lê como tabela 'longa'.
    """
    csv_url = os.environ.get("GOOGLE_SHEET_CSV_URL", "").strip()
    if not csv_url:
        return None
    df = pd.read_csv(csv_url)
    return _normalize_columns(df)

def _read_first_sheet_bytes(src: str | bytes | bytearray) -> pd.DataFrame:
    """
    Lê a 1ª aba com HEADER=True (para o caso da tabela longa comum).
    """
    xls = pd.ExcelFile(src)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(xls, sheet_name=sheet)
    return _normalize_columns(df)

def _read_first_sheet_raw(src: str | bytes | bytearray) -> pd.DataFrame:
    """
    Lê a 1ª aba com header=None para detectar blocos por linhas.
    """
    xls = pd.ExcelFile(src)
    raw = pd.read_excel(xls, sheet_name=0, header=None)
    return raw

# ---------------------------
# Parser específico da sua 1ª aba (inputs_dashboard_cht22)
# ---------------------------

def _extract_kv_block_from_inputs(raw: pd.DataFrame) -> pd.DataFrame | None:
    """
    Procura um cabeçalho com 'CAMPO' e 'VALOR ATUAL' (qualquer posição na linha).
    Lê até encontrar linha em branco OU um título de seção (em A).
    Retorna DataFrame com colunas: campo, descricao, valor, celula_ref, tipo, celula
    """
    if raw is None or raw.empty:
        return None

    nrows = len(raw)
    header_row = None
    for i in range(min(50, nrows)):  # procura no topo
        row_vals = raw.iloc[i].astype(str).str.strip().tolist()
        if "CAMPO" in row_vals and "VALOR ATUAL" in row_vals:
            header_row = i
            break
    if header_row is None:
        return None

    # monta header
    header = raw.iloc[header_row].tolist()
    # linhas de dados começam na próxima linha
    data_start = header_row + 1
    data_rows = []
    for j in range(data_start, nrows):
        a_val = str(raw.iloc[j, 0]).strip() if raw.shape[1] > 0 else ""
        # para se acharmos título de seção ou linha em branco (primeira coluna vazia E linha toda vazia)
        is_section = a_val in SECTION_TOKENS
        if is_section or _is_blank_row(raw.iloc[j]):
            break
        data_rows.append(raw.iloc[j].tolist())

    if not data_rows:
        return None

    df = pd.DataFrame(data_rows, columns=header)
    # normaliza nomes esperados
    colmap = {}
    for c in df.columns:
        k = _slug_pt(c)
        if k == "descricao" or k == "descrição":
            colmap[c] = "descricao"
        elif k in ("valor_atual","valor","valor_atual_"):
            colmap[c] = "valor"
        elif k in ("celula_ref","celula_ref_","célula_ref","celula__ref"):
            colmap[c] = "celula_ref"
        elif k in ("tipo",):
            colmap[c] = "tipo"
        elif k in ("celula","célula"):
            colmap[c] = "celula"
        elif k in ("campo",):
            colmap[c] = "campo"
    df = df.rename(columns=colmap)

    # mantém só as colunas úteis
    keep = [c for c in ["campo","descricao","valor","celula_ref","tipo","celula"] if c in df.columns]
    df = df[keep].copy()
    # limpeza básica
    df["campo"] = df["campo"].astype(str).str.strip()
    df = df[df["campo"] != ""]
    return df.reset_index(drop=True)

def _extract_sections_after_kv(raw: pd.DataFrame) -> dict:
    """
    Após o bloco KV, detecta seções marcadas por um TÍTULO na coluna A
    (exatamente igual às chaves de SECTION_TOKENS). Para cada seção:
      - a linha imediatamente abaixo é o cabeçalho
      - segue lendo até linha vazia ou próximo título
    Retorna dict { block_key: DataFrame }
    """
    blocks = {}
    nrows = len(raw)
    i = 0
    # 1) pular o topo até o início da primeira seção "oficial" (ou o fim do KV)
    # já que o KV extractor para ao encontrar linha vazia ou título, aqui só precisamos
    # varrer o arquivo todo em busca de títulos
    while i < nrows:
        a_val = str(raw.iloc[i, 0]).strip()
        if a_val in SECTION_TOKENS:
            block_key = SECTION_TOKENS[a_val]
            header_row = i + 1
            # skip linhas vazias entre título e header
            while header_row < nrows and _is_blank_row(raw.iloc[header_row]):
                header_row += 1
            if header_row >= nrows:
                break
            header = raw.iloc[header_row].tolist()
            data_start = header_row + 1

            rows = []
            j = data_start
            while j < nrows:
                next_title = str(raw.iloc[j, 0]).strip()
                if next_title in SECTION_TOKENS or _is_blank_row(raw.iloc[j]):
                    break
                rows.append(raw.iloc[j].tolist())
                j += 1

            if rows:
                df = pd.DataFrame(rows, columns=header)
                df = _normalize_columns(df)
                blocks[block_key] = df.reset_index(drop=True)
            i = j
        else:
            i += 1
    return blocks

# ---------------------------
# Loader principal
# ---------------------------

def load_inputs_dashboard(src: str | bytes | bytearray | None = None):
    """
    1) Se GOOGLE_SHEET_CSV_URL estiver setado, usa como TABELA LONGA.
    2) Caso contrário, lê a 1ª aba e tenta:
       - extrair bloco KV (grid CAMPO/VALOR ATUAL)
       - extrair seções nomeadas (PROFISSOES, ESTADO X PROFISSÃO, etc.)
       Retorna em modo=blocks.
    """
    # 1) Tenta CSV longo
    df_long = _read_csv_url_if_any()
    if df_long is not None and not df_long.empty:
        cols = set(map(str, df_long.columns))
        has_min = {"estado","canal","profissao"}.issubset(cols)
        has_metric = ("valor" in cols) or ("vendas" in cols)
        if has_min and has_metric:
            return {"mode": "long", "df": df_long}

    # 2) XLSX 1ª aba (sua inputs_dashboard_cht22)
    if src is None:
        # permite apontar para link XLSX do Google Sheets (export?format=xlsx)
        xlsx_url = os.environ.get("DATA_XLSX_PATH", "").strip()
        if not xlsx_url:
            raise RuntimeError("Defina GOOGLE_SHEET_CSV_URL (tabela longa) ou DATA_XLSX_PATH (xlsx).")
        src = _download_bytes(xlsx_url)

    # tenta primeiro ler com header=True (só para detectar se por acaso já é longa)
    try:
        df_try = _read_first_sheet_bytes(src)
        cols = set(map(str, df_try.columns))
        if {"estado","canal","profissao"}.issubset(cols) and (("valor" in cols) or ("vendas" in cols)):
            return {"mode": "long", "df": df_try}
    except Exception:
        pass  # segue para o parser de blocos

    # parser de blocos na 1ª aba
    raw = _read_first_sheet_raw(src)
    blocks = {}

    # 2a) KV
    kv = _extract_kv_block_from_inputs(raw)
    if kv is not None and not kv.empty:
        blocks["kv"] = kv

    # 2b) Seções abaixo do KV
    more = _extract_sections_after_kv(raw)
    blocks.update(more)

    # Pós-processos úteis (exemplos)
    if "tbl_prof_canal" in blocks:
        # remover colunas '%', se existirem (pivôs costumam trazer pares Qtd/%)
        b = blocks["tbl_prof_canal"].copy()
        b = b[[c for c in b.columns if not str(c).strip().endswith("%")]]
        blocks["tbl_prof_canal"] = b.reset_index(drop=True)

    return {"mode": "blocks", **blocks}

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
        return f"{int(float(str(x).replace('.','').replace(',','.'))):,}".replace(",", ".")
    except Exception:
        return "-"

def format_ptbr_money(x):
    try:
        v = str(x)
        v = v.replace("R$","").strip().replace(".","").replace(",",".")
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ -"
