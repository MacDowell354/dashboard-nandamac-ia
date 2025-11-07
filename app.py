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
    """Baixa URL com cachebuster para evitar cache do Google Sheets"""
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

def _resolve_source() -> Tuple[Optional[pd.DataFrame], str]:
    if GOOGLE_SHEET_CSV_URL:
        try:
            return _fetch_google_csv(GOOGLE_SHEET_CSV_URL), "google-csv"
        except Exception as e:
            _log(f"ERRO CSV: {e}")
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

# ---------- Funções analíticas simplificadas ----------
def extract_vendas_realizadas(df_raw: pd.DataFrame):
    return pd.DataFrame(columns=["Data", "valor_liquido"])  # placeholder seguro

def extract_kv_metrics(df_raw: pd.DataFrame):
    return {"dias_campanha": 1, "meta_cpl": 15, "cpl_medio": 12, "total_leads": 100}

def build_channel_cards(kv: dict):
    return [{"title": "Facebook", "body": "Canal principal", "tone": "positivo"}]

def build_metas_status(kv, qtd_vendas, cpl, inv, orc):
    return [{"nome": "CPL Médio", "status": "verde"}]

# ---------- Endpoint manual de recarregamento ----------
@app.get("/reload")
def reload_data():
    _log("Recarregando dados manualmente via /reload...")
    _DF_CACHE["df"] = load_dataframe()
    _DF_CACHE["loaded_at"] = datetime.utcnow()
    _DF_CACHE["mode"] = globals().get("DATA_MODE")
    return f"✅ Dados recarregados com sucesso em {datetime.now().strftime('%H:%M:%S')} (modo: {_DF_CACHE['mode']})"

# ---------- Rotas ----------
@app.get("/")
def index():
    df_raw = get_data()
    return render_template("index.html", linhas=len(df_raw), **_ui_globals())

@app.get("/visao-geral")
def visao_geral():
    df_raw = get_data()
    vendas = extract_vendas_realizadas(df_raw)
    kv = extract_kv_metrics(df_raw)
    dias_camp = kv.get("dias_campanha", 0)
    topo = dict(dias=dias_camp)
    return render_template("visao_geral.html", topo=topo, **_ui_globals())

@app.get("/debug")
def debug_grid():
    df_raw = get_data()
    sample = df_raw.head(10).fillna("").astype(str).to_dict(orient="records")
    cols = list(range(df_raw.shape[1]))
    return render_template("debug.html", cols=cols, rows=sample, **_ui_globals())

# ---------- Execução ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
