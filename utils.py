# utils.py
# -*- coding: utf-8 -*-
import os
import io
import re
import json
from datetime import datetime
import pandas as pd
import requests

# -------------------------------------------------
# Formatação PT-BR
# -------------------------------------------------
def format_ptbr_int(x):
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return "-"

def format_ptbr_money(x):
    try:
        return f"R$ {float(str(x).replace('R$', '').replace('.', '').replace(',', '.')):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ -"

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _slug_pt(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    transl = str.maketrans("çãáàâäéêèëíìïóôõòöúùü", "caaaaaeeeeiiiooooouuu")
    s = s.translate(transl)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s

def _to_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return x
    s = str(x).strip()
    s = s.replace("R$", "").replace("%", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _fetch_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content

def _first_sheet_df_from_xlsx_bytes(b: bytes, header="infer") -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(b))
    return pd.read_excel(xls, sheet_name=0, header=header)

def _find_table(df: pd.DataFrame, required_cols, max_gap=2):
    """
    Procura, no DataFrame 'df' (sem header), um "miolo" de tabela cuja linha de header
    contenha todas as 'required_cols' (case-insensitive). Retorna (df_tabela, start_row, end_row)
    """
    req = [c.lower() for c in required_cols]
    n = len(df)
    for i in range(n):
        row = [str(x).strip().lower() for x in list(df.iloc[i, :])]
        if all(any(rc in str(cell) for cell in row) for rc in req):
            # Encontrou header em i; coletar linhas seguintes até 2 linhas totalmente vazias
            headers = list(df.iloc[i, :])
            rows = []
            gaps = 0
            for j in range(i + 1, n):
                ser = df.iloc[j, :]
                if ser.isna().all():
                    gaps += 1
                    if gaps > max_gap:
                        break
                    else:
                        continue
                rows.append(list(ser))
            tab = pd.DataFrame(rows, columns=headers).dropna(how="all")
            # normaliza nomes
            tab.columns = [_slug_pt(c) for c in tab.columns]
            return tab, i, i + len(rows)
    return None, None, None

def _kv_from_inputs_sheet(df_infer_header: pd.DataFrame):
    """
    A tabela Campo / Valor Atual costuma aparecer na aba inputs_dashboard_cht22.
    Tentamos detectar colunas 'CAMPO' e 'VALOR ATUAL' (variações aceitas).
    """
    df = df_infer_header.copy()
    # Normaliza nomes
    df.columns = [_slug_pt(c) for c in df.columns]
    col_map = {}
    for c in df.columns:
        if c in ("campo", "campos", "kpi", "variavel", "variavel_kpi"):
            col_map["campo"] = c
        if c in ("valor_atual", "valor", "valoratual"):
            col_map["valor"] = c
    if not {"campo", "valor"}.issubset(col_map):
        # Não está com header "bonitinho"; vamos tentar achar por padrão:
        return {}

    out = {}
    for _, row in df[[col_map["campo"], col_map["valor"]]].dropna(how="all").iterrows():
        k = _slug_pt(row[col_map["campo"]])
        v = row[col_map["valor"]]
        out[k] = v
    return out

# -------------------------------------------------
# Carregadores de dados
# -------------------------------------------------
def load_from_csv_url(url: str):
    """
    CSV tidy (recomendado). Espera colunas como:
    data, estado, regiao, canal, profissao, leads, vendas, valor
    Apenas as usadas no dashboard serão agregadas; colunas extras são ignoradas.
    """
    b = _fetch_bytes(url)
    df = pd.read_csv(io.BytesIO(b))
    # normaliza
    df.columns = [_slug_pt(c) for c in df.columns]
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    # valores numéricos comuns
    for c in ("leads", "vendas", "valor"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return {"mode": "csv", "long": df}

def load_from_xlsx_url_or_path(src: str):
    """
    Lê a 1ª aba da planilha (inputs_dashboard_cht22).
    Extrai:
      - blocos_kpi (campo->valor)
      - vendas_realizadas (tabela)
      - estado_x_profissao (matriz)
      - regiao_x_profissao (matriz)
      - profissoes_x_canais (matriz estilo pivô)
    """
    # Suporta tanto URL quanto caminho local
    if re.match(r"^https?://", src.strip(), re.I):
        content = _fetch_bytes(src)
    else:
        with open(src, "rb") as f:
            content = f.read()

    # Tentativa 1: DF com header “bonitinho”
    df0 = _first_sheet_df_from_xlsx_bytes(content, header=0)
    # Tentativa 2: DF cru para caça de cabeçalhos
    df_raw = _first_sheet_df_from_xlsx_bytes(content, header=None)

    # 1) KPIs (campo/valor_atual)
    kpis = _kv_from_inputs_sheet(df0)

    # 2) vendas_realizadas
    vendas_cols = ["Data", "Nome", "Profissão", "Vendedora", "estado_contato", "valor-venda", "valor_liquido"]
    vendas_tab, _, _ = _find_table(df_raw, vendas_cols)
    if vendas_tab is not None:
        # ajustes
        if "data" in vendas_tab.columns:
            vendas_tab["data"] = pd.to_datetime(vendas_tab["data"], errors="coerce")
        for c in ("valorvenda", "valor_liquido", "valor_venda", "valorliquido"):
            if c in vendas_tab.columns:
                vendas_tab[c] = vendas_tab[c].apply(_to_number)
        # normaliza nomes esperados
        ren = {}
        if "valorvenda" in vendas_tab.columns:
            ren["valorvenda"] = "valor_venda"
        if "valorliquido" in vendas_tab.columns:
            ren["valorliquido"] = "valor_liquido"
        vendas_tab = vendas_tab.rename(columns=ren)

    # 3) estado x profissão
    est_cols = ["Estado", "Dentista", "Fisioterapeuta", "Fonoaudiólogo", "Médico", "Nutricionista", "Outra", "Psicoterapeuta", "Psicólogo", "Veterinário"]
    est_prof_tab, _, _ = _find_table(df_raw, est_cols)

    # 4) região x profissão
    reg_cols = ["Região", "Dentista", "Fisioterapeuta", "Fonoaudiólogo", "Médico", "Nutricionista", "Outra", "Psicoterapeuta", "Psicólogo", "Veterinário"]
    reg_prof_tab, _, _ = _find_table(df_raw, reg_cols)

    # 5) profissões x canais (tabela de “Rótulos de Linha/Coluna”)
    # Buscar header que contenha "Rótulos de Linha" e alguns canais como 'facebook', 'instagram', etc.
    rotais = ["rótulos de linha", "rotulos de linha", "rótulos de coluna", "rotulos de coluna"]
    canais_suspeitos = ["facebook", "instagram", "youtube", "email", "googlesearch", "manychat", "redirect"]
    found = None
    n = len(df_raw)
    for i in range(n):
        row = [str(x).strip().lower() for x in list(df_raw.iloc[i, :])]
        if any(tag in " ".join(row) for tag in rotais) and sum(1 for c in canais_suspeitos if any(c in cell for cell in row)) >= 2:
            # header provável
            headers = list(df_raw.iloc[i, :])
            rows = []
            for j in range(i + 1, n):
                ser = df_raw.iloc[j, :]
                if ser.isna().all():
                    break
                rows.append(list(ser))
            tab = pd.DataFrame(rows, columns=headers).dropna(how="all")
            tab.columns = [_slug_pt(c) for c in tab.columns]
            found = tab
            break

    prof_canais_tab = found

    return {
        "mode": "xlsx",
        "kpis": kpis,
        "vendas": vendas_tab,
        "estado_x_profissao": est_prof_tab,
        "regiao_x_profissao": reg_prof_tab,
        "profissoes_x_canais": prof_canais_tab,
    }

# -------------------------------------------------
# Orquestrador principal (usado pelo app)
# -------------------------------------------------
def load_inputs_dashboard():
    """
    Orquestra carregamento conforme envs:
      - GOOGLE_SHEET_CSV_URL (prioridade)
      - DATA_XLSX_PATH (URL .xlsx do Google Sheets OU caminho local)
    """
    csv_url = (os.getenv("GOOGLE_SHEET_CSV_URL") or "").strip()
    xlsx_src = (os.getenv("DATA_XLSX_PATH") or "").strip()

    if csv_url:
        try:
            blob = load_from_csv_url(csv_url)
            return blob
        except Exception as e:
            # se der ruim no CSV, tenta planilha
            pass

    if xlsx_src:
        return load_from_xlsx_url_or_path(xlsx_src)

    raise RuntimeError(
        "Defina GOOGLE_SHEET_CSV_URL (CSV tidy) ou DATA_XLSX_PATH (XLSX da aba 'inputs_dashboard_cht22')."
    )
