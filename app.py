# app.py
# -----------------------------------------
# Flask + Loader com cache e logs (Render)
# Dep.: pandas, openpyxl, requests
# Vars de ambiente:
#   - GOOGLE_SHEET_CSV_URL (prioridade)
#   - DATA_XLSX_PATH       (fallback, caminho local ou URL)
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

# ---------- Contexto (usado para ativar aba do menu, etc.) ----------
@app.context_processor
def inject_current_path():
    try:
        return {"current_path": request.path or ""}
    except Exception:
        return {"current_path": ""}

def _ui_globals():
    """Variáveis usadas no rodapé; não quebra se não existirem."""
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
    df = pd.read_csv(io.StringIO(text))
    _log(f"CSV lido: linhas={len(df)} colunas={list(df.columns)}")
    return df

def _fetch_xlsx_from_url(url: str, cache_name="sheet.xlsx") -> pd.DataFrame:
    content = _download_to_bytes(url)
    cache_path = os.path.join(CACHE_DIR, cache_name)
    with open(cache_path, "wb") as f:
        f.write(content)
    _log(f"XLSX salvo em cache: {cache_path}")
    df = pd.read_excel(io.BytesIO(content))
    _log(f"XLSX lido: linhas={len(df)} colunas={list(df.columns)}")
    return df

def _read_local_xlsx(path: str) -> pd.DataFrame:
    _log(f"Lendo XLSX local: {path}")
    return pd.read_excel(path)

def _resolve_source() -> Tuple[Optional[pd.DataFrame], str]:
    # 1) CSV de aba específica (prioritário)
    if GOOGLE_SHEET_CSV_URL:
        try:
            return _fetch_google_csv(GOOGLE_SHEET_CSV_URL), "google-csv"
        except Exception as e:
            _log(f"ERRO CSV: {e}")
    # 2) XLSX por URL ou local
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
    if df.empty:
        _log(f"DataFrame vazio | mode={mode}")
    else:
        _log(f"OK | linhas={len(df)} | cols={list(df.columns)[:8]} | mode={mode}")
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

# ---------- Rotas ----------
@app.get("/")
def index():
    df = get_data()
    linhas = len(df) if not df.empty else 0
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
    return render_template("projecao_resultados.html", **_ui_globals())

@app.get("/acompanhamento-vendas")
def acompanhamento_vendas():
    df = get_data()
    if df.empty:
        return render_template("acompanhamento_vendas.html",
                               has_data=False, kpis={}, series=[], table=[],
                               **_ui_globals())

    cols = {c.lower(): c for c in df.columns}
    c_data  = cols.get("data")  or cols.get("dt") or cols.get("date")
    c_valor = cols.get("valor") or cols.get("venda") or cols.get("sales") or cols.get("value")

    total_linhas = len(df)
    soma_valor = float(df[c_valor].sum()) if (c_valor and c_valor in df.columns) else 0.0
    kpis = {"linhas": total_linhas, "soma_valor": soma_valor}

    series = []
    if c_data and c_valor and c_data in df.columns and c_valor in df.columns:
        tmp = df[[c_data, c_valor]].copy()
        tmp[c_data] = pd.to_datetime(tmp[c_data], errors="coerce")
        tmp = tmp.dropna(subset=[c_data]).groupby(tmp[c_data].dt.date, as_index=False)[c_valor].sum()
        series = [{"x": str(d), "y": float(v)} for d, v in zip(tmp[c_data], tmp[c_valor])]

    table = df.head(50).to_dict(orient="records")

    return render_template("acompanhamento_vendas.html",
                           has_data=(len(table) > 0 or len(series) > 0),
                           kpis=kpis, series=series, table=table, **_ui_globals())

# (não use app.run com gunicorn)
# if __name__ == "__main__":
#     app.run(debug=True)
