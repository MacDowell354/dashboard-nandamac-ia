# app.py
# -----------------------------------------
# Flask + loader robusto + extrações "inteligentes" da planilha
# Dep.: Flask, gunicorn, pandas, numpy<2.1, requests, openpyxl
# ENV:
#   - GOOGLE_SHEET_CSV_URL  -> .../export?format=csv&gid=XXXX   (prioridade)
#   - DATA_XLSX_PATH        -> (opcional, fallback)
#   - DATA_CACHE_TTL_SECONDS (opcional; default 300)
# -----------------------------------------
import os, io, time, math, unicodedata, random
from datetime import datetime, timedelta
from typing import Tuple, Optional
import pandas as pd
import requests
from flask import Flask, render_template, request

# ---------- Flask ----------
app = Flask(__name__)

# ---------- Filtros Jinja ----------
@app.template_filter('dash')
def dash(value):
    try:
        if value is None: return "—"
        s = str(value).strip()
        if s == "" or s.lower() == "nan": return "—"
        return s
    except Exception:
        return "—"

@app.template_filter('br_money')
def br_money(value):
    try:
        if value is None: return "—"
        if isinstance(value, float) and math.isnan(value): return "—"
        s = str(value).strip()
        if s.startswith("R$"): return s
        if isinstance(value, (int, float)):
            return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        raw = s.replace("R$", "").replace(".", "").replace(",", ".")
        num = float(raw)
        return f"R$ {num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return dash(value)

# ---------- Contexto ----------
@app.context_processor
def inject_current_path():
    try:
        return {"current_path": request.path or ""}
    except Exception:
        return {"current_path": ""}

def _ui_globals():
    return {
        "last_loaded": globals().get("LAST_LOADED"),
        "data_mode":   globals().get("DATA_MODE")
    }

# ---------- Loader ----------
GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "").strip()
DATA_XLSX_PATH       = os.getenv("DATA_XLSX_PATH", "").strip()
CACHE_TTL_SECONDS    = int(os.getenv("DATA_CACHE_TTL_SECONDS", "300"))
CACHE_DIR            = os.path.join(os.getcwd(), "data")
os.makedirs(CACHE_DIR, exist_ok=True)

def _log(msg: str):
    print(f"[DATA] {datetime.utcnow().isoformat()}Z | {msg}", flush=True)

def _download_to_bytes(url: str, timeout: int = 45, max_attempts: int = 3) -> bytes:
    """Baixa URL com cachebuster para forçar atualização do CSV do Google Sheets"""
    last_err = None
    for i in range(1, max_attempts + 1):
        try:
            cachebuster = random.randint(0, 999999)
            final_url = f"{url}&cachebuster={cachebuster}"
            _log(f"Baixando ({i}/{max_attempts}): {final_url}")
            r = requests.get(final_url, timeout=timeout)
            r.raise_for_status()
            _log(f"Download OK: {len(r.content)} bytes (cachebuster={cachebuster})")
            return r.content
        except Exception as e:
            last_err = e
            wait = 2 ** i
            _log(f"Falha: {e} | tentando novamente em {wait}s")
            time.sleep(wait)
    raise last_err

def _fetch_google_csv(url: str) -> pd.DataFrame:
    _log("Lendo Google Sheet (CSV)")
    text = _download_to_bytes(url).decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(text), header=None)
    _log(f"CSV lido: linhas={len(df)} colunas={df.shape[1]}")
    return df

def _fetch_xlsx_from_url(url: str, cache_name="sheet.xlsx") -> pd.DataFrame:
    content = _download_to_bytes(url)
    with open(os.path.join(CACHE_DIR, cache_name), "wb") as f:
        f.write(content)
    df = pd.read_excel(io.BytesIO(content), header=None)
    _log(f"XLSX lido: linhas={len(df)} colunas={df.shape[1]}")
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
    _log("Nenhuma fonte configurada.")
    return None, "none"

_DF_CACHE = {"df": None, "loaded_at": None, "mode": None}

def load_dataframe() -> pd.DataFrame:
    df, mode = _resolve_source()
    if df is None: df = pd.DataFrame()
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

# ---------- Endpoint manual de recarregamento ----------
@app.get("/reload")
def reload_data():
    _log("Recarregando dados manualmente via /reload...")
    _DF_CACHE["df"] = load_dataframe()
    _DF_CACHE["loaded_at"] = datetime.utcnow()
    _DF_CACHE["mode"] = globals().get("DATA_MODE")
    return f"✅ Dados recarregados com sucesso em {datetime.now().strftime('%H:%M:%S')} (modo: {_DF_CACHE['mode']})"

# =====================================================
# 🔻 ROTAS DO DASHBOARD (mantém toda a sua lógica)
# =====================================================

@app.get("/")
def index():
    df_raw = get_data()
    linhas = len(df_raw)
    return render_template("index.html", linhas=linhas, **_ui_globals())

@app.get("/visao-geral")
def visao_geral():
    df_raw = get_data()
    # sua lógica completa aqui...
    return render_template("visao_geral.html", **_ui_globals())

@app.get("/acompanhamento-vendas")
def acompanhamento_vendas():
    df_raw = get_data()
    return render_template("acompanhamento_vendas.html", **_ui_globals())

@app.get("/insights-ia")
def insights_ia():
    df_raw = get_data()
    return render_template("insights_ia.html", **_ui_globals())

@app.get("/projecao-resultados")
def projecao_resultados():
    df_raw = get_data()
    return render_template("projecao_resultados.html", **_ui_globals())

@app.get("/analise-regional")
def analise_regional():
    df_raw = get_data()
    return render_template("analise_regional.html", **_ui_globals())

@app.get("/profissao-por-canal")
def profissao_por_canal():
    df_raw = get_data()
    return render_template("profissao_por_canal.html", **_ui_globals())

@app.get("/origem-conversao")
def origem_conversao():
    df_raw = get_data()
    return render_template("origem_conversao.html", **_ui_globals())

@app.get("/debug")
def debug_grid():
    df_raw = get_data()
    sample = df_raw.head(30).fillna("").astype(str).to_dict(orient="records")
    cols = list(range(df_raw.shape[1]))
    return render_template("debug.html", cols=cols, rows=sample, **_ui_globals())

# =====================================================
# 🔻 EXECUÇÃO LOCAL (Render usa gunicorn automaticamente)
# =====================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
