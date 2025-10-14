# app.py
# -----------------------------------------
# Flask + Loader com cache e logs (Render)
# Dep.: pandas, openpyxl, requests
# Vars de ambiente:
#   - GOOGLE_SHEET_CSV_URL (prioridade, CSV da aba com gid)
#   - DATA_XLSX_PATH       (fallback, caminho local ou URL xlsx)
#   - DATA_CACHE_TTL_SECONDS (opcional; padrão 300)
# -----------------------------------------
import os, io, time
from datetime import datetime, timedelta
from typing import Tuple, Optional

import pandas as pd
import requests
from flask import Flask, render_template, request

# ---------- Flask ----------
app = Flask(__name__)

# === filtros Jinja para formatação ===
@app.template_filter('dash')
def dash(value):
    """Mostra '—' para vazio/NaN; caso contrário, devolve string."""
    try:
        if value is None:
            return "—"
        s = str(value).strip()
        if s == "" or s.lower() == "nan":
            return "—"
        return s
    except Exception:
        return "—"

@app.template_filter('br_money')
def br_money(value):
    """Formata número em R$ pt-BR; se string já vier formatada, mantém; se vazio, '—'."""
    import math
    try:
        if value is None:
            return "—"
        if isinstance(value, float) and math.isnan(value):
            return "—"
        s = str(value).strip()
        if s.startswith("R$"):
            return s
        if isinstance(value, (int, float)):
            return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        raw = s.replace("R$", "").replace(".", "").replace(",", ".")
        num = float(raw)
        return f"R$ {num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return dash(value)

# ---------- Contexto (usado para ativar aba do menu, etc.) ----------
@app.context_processor
def inject_current_path():
    try:
        return {"current_path": request.path or ""}
    except Exception:
        return {"current_path": ""}

def _ui_globals():
    last_loaded = globals().get("LAST_LOADED")
    data_mode   = globals().get("DATA_MODE")
    return {"last_loaded": last_loaded, "data_mode": data_mode}

# ---------- Loader de dados com cache ----------
GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "").strip()
DATA_XLSX_PATH       = os.getenv("DATA_XLSX_PATH", "").strip()
CACHE_TTL_SECONDS    = int(os.getenv("DATA_CACHE_TTL_SECONDS", "300"))
CACHE_DIR            = os.path.join(os.getcwd(), "data")
os.makedirs(CACHE_DIR, exist_ok=True)

def _log(msg: str):
    print(f"[DATA] {datetime.utcnow().isoformat()}Z | {msg}", flush=True)

def _download_to_bytes(url: str, timeout: int = 45, max_attempts: int = 3) -> bytes:
    last_err = None
    for i in range(1, max_attempts + 1):
        try:
            _log(f"Baixando ({i}/{max_attempts}): {url}")
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            _log(f"Download OK: {len(r.content)} bytes")
            return r.content
        except Exception as e:
            last_err = e
            wait = 2 ** i
            _log(f"Falha: {e} | tentando de novo em {wait}s")
            time.sleep(wait)
    raise last_err

def _fetch_google_csv(url: str) -> pd.DataFrame:
    _log("Lendo Google Sheet (CSV)")
    text = _download_to_bytes(url).decode("utf-8", errors="replace")
    # carrega como grade crua (a aba tem múltiplas seções)
    df = pd.read_csv(io.StringIO(text), header=None)
    _log(f"CSV lido (grade crua): linhas={len(df)} colunas={df.shape[1]}")
    return df

def _fetch_xlsx_from_url(url: str, cache_name="sheet.xlsx") -> pd.DataFrame:
    content = _download_to_bytes(url)
    cache_path = os.path.join(CACHE_DIR, cache_name)
    with open(cache_path, "wb") as f:
        f.write(content)
    _log(f"XLSX salvo em cache: {cache_path}")
    df = pd.read_excel(io.BytesIO(content), header=None)  # grade crua
    _log(f"XLSX lido (grade crua): linhas={len(df)} colunas={df.shape[1]}")
    return df

def _read_local_xlsx(path: str) -> pd.DataFrame:
    _log(f"Lendo XLSX local: {path}")
    return pd.read_excel(path, header=None)

def _resolve_source() -> Tuple[Optional[pd.DataFrame], str]:
    if GOOGLE_SHEET_CSV_URL:
        try:
            return _fetch_google_csv(GOOGLE_SHEET_CSV_URL), "google-csv"
        except Exception as e:
            _log(f"ERRO CSV: {e}")
    if DATA_XLSX_PATH:
        try:
            if DATA_XLSX_PATH.lower().startswith("http"):
                return _fetch_xlsx_from_url(DATA_XLSX_PATH), "xlsx-url"
            if not os.path.exists(DATA_XLSX_PATH):
                raise FileNotFoundError(DATA_XLSX_PATH)
            return _read_local_xlsx(DATA_XLSX_PATH), "xlsx-local"
        except Exception as e:
            _log(f"ERRO XLSX: {e}")
    _log("Nenhuma fonte configurada (defina GOOGLE_SHEET_CSV_URL ou DATA_XLSX_PATH).")
    return None, "none"

_DF_CACHE = {"df": None, "loaded_at": None, "mode": None}

def load_dataframe() -> pd.DataFrame:
    df, mode = _resolve_source()
    if df is None:
        df = pd.DataFrame()
    globals()["LAST_LOADED"] = datetime.utcnow()
    globals()["DATA_MODE"]   = mode
    _log(f"Fonte: {mode} | shape={df.shape}")
    return df

def get_data() -> pd.DataFrame:
    now = datetime.utcnow()
    if (_DF_CACHE["df"] is None or _DF_CACHE["loaded_at"] is None or
        (now - _DF_CACHE["loaded_at"]) > timedelta(seconds=CACHE_TTL_SECONDS)):
        _log("Recarregando dados (cache expirado ou inexistente)...")
        _DF_CACHE["df"] = load_dataframe()
        _DF_CACHE["loaded_at"] = now
        _DF_CACHE["mode"] = globals().get("DATA_MODE")
        _log(f"Cache atualizado | TTL={CACHE_TTL_SECONDS}s | mode={_DF_CACHE['mode']}")
    else:
        age = int((now - _DF_CACHE["loaded_at"]).total_seconds())
        _log(f"Usando cache (idade={age}s / TTL={CACHE_TTL_SECONDS}s)")
    return _DF_CACHE["df"]

# ---------- Helpers para "seções" da aba ----------
def _first_eq(series: pd.Series, value: str) -> Optional[int]:
    mask = series.astype(str).str.strip().str.lower() == value.lower()
    idx = mask.idxmax() if mask.any() else None
    return int(idx) if idx is not None else None

def extract_vendas_realizadas(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Extrai a tabela iniciada por 'vendas_realizadas' (coluna A) até linha 'Total'."""
    if df_raw.empty: 
        return pd.DataFrame()
    start = _first_eq(df_raw[0], "vendas_realizadas")
    if start is None: 
        _log("Seção 'vendas_realizadas' não encontrada.")
        return pd.DataFrame()
    header_row = start + 1
    end = header_row + 1
    while end < len(df_raw):
        a = str(df_raw.iloc[end, 0]).strip()
        if a.lower().startswith("total") or (a == "nan" and str(df_raw.iloc[end,1]).strip() == "nan"):
            break
        end += 1
    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)
    # normalizações
    if "Data" in sub.columns:
        sub["Data"] = pd.to_datetime(sub["Data"], errors="coerce")
    for col in ["valor_venda", "valor_liquido"]:
        if col in sub.columns:
            sub[col] = (
                sub[col]
                .astype(str)
                .str.replace("R$", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
            sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(0.0)
    return sub

def extract_projecao(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Extrai a tabela iniciada por 'progecao_de_resultados' e evita NaN."""
    if df_raw.empty:
        return pd.DataFrame()
    start = _first_eq(df_raw[0], "progecao_de_resultados")
    if start is None:
        _log("Seção 'progecao_de_resultados' não encontrada.")
        return pd.DataFrame()
    header_row = start + 1
    end = header_row + 1
    blank_count = 0
    while end < len(df_raw):
        row_is_blank = df_raw.iloc[end].isna().all()
        blank_count = blank_count + 1 if row_is_blank else 0
        if blank_count >= 2:
            break
        end += 1
    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)
    sub.columns = [("col_" + str(i) if (c is None or str(c) == "nan" or str(c).strip() == "")
                    else str(c)) for i, c in enumerate(sub.columns)]
    sub = sub.fillna("")
    return sub

# ---------- Rotas ----------
@app.get("/")
def index():
    df_raw = get_data()
    linhas = len(df_raw) if not df_raw.empty else 0
    return render_template("index.html", linhas=linhas, **_ui_globals())

@app.get("/visao-geral")
def visao_geral():
    return render_template("visao_geral.html", **_ui_globals())

@app.get("/origem-conversao")
def origem_conversao():
    return render_template("origem_conversao.html", **_ui_globals())

@app.get("/profissao-por-canal")
def profissao_por_canal():
    return render_template("profissao_por_canal.html", **_ui_globals())

@app.get("/analise-regional")
def analise_regional():
    return render_template("analise_regional.html", **_ui_globals())

@app.get("/insights-ia")
def insights_ia():
    return render_template("insights_ia.html", **_ui_globals())

@app.get("/projecao-resultados")
def projecao_resultados():
    df_raw = get_data()
    proj = extract_projecao(df_raw)
    has_data = not proj.empty
    table = proj.to_dict(orient="records") if has_data else []
    return render_template("projecao_resultados.html",
                           has_data=has_data, table=table, **_ui_globals())

@app.get("/acompanhamento-vendas")
def acompanhamento_vendas():
    df_raw = get_data()
    vendas = extract_vendas_realizadas(df_raw)
    if vendas.empty:
        return render_template("acompanhamento_vendas.html",
                               has_data=False, kpis={}, series=[], table=[],
                               **_ui_globals())
    total_qtd = len(vendas)
    soma_liquido = float(vendas.get("valor_liquido", pd.Series(dtype=float)).sum())
    kpis = {"qtd": total_qtd, "liquido": soma_liquido}
    series = []
    if "Data" in vendas.columns and "valor_liquido" in vendas.columns:
        tmp = vendas[["Data", "valor_liquido"]].dropna()
        tmp = tmp.groupby(tmp["Data"].dt.date, as_index=False)["valor_liquido"].sum()
        series = [{"x": str(d), "y": float(v)} for d, v in zip(tmp["Data"], tmp["valor_liquido"])]
    table = vendas.head(50).to_dict(orient="records")
    return render_template("acompanhamento_vendas.html",
                           has_data=True, kpis=kpis, series=series, table=table,
                           **_ui_globals())

# rota de debug (opcional)
@app.get("/debug")
def debug_grid():
    df_raw = get_data()
    sample = df_raw.head(30).fillna("").astype(str).to_dict(orient="records")
    cols = list(range(df_raw.shape[1]))
    return render_template("debug.html", cols=cols, rows=sample, **_ui_globals())

# (não usar app.run com gunicorn)
# if __name__ == "__main__":
#     app.run(debug=True)
