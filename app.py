# app.py — versão consolidada e funcional (Render + cachebuster + reload)
# -----------------------------------------
# Flask + loader robusto + extrações "inteligentes" da planilha
# Dep.: Flask, gunicorn, pandas, numpy<2.1, requests, openpyxl
# ENV:
#   - GOOGLE_SHEET_CSV_URL  -> .../export?format=csv&gid=XXXX   (prioridade)
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
CACHE_TTL_SECONDS = int(os.getenv("DATA_CACHE_TTL_SECONDS", "300"))
CACHE_DIR = os.path.join(os.getcwd(), "data")
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
            time.sleep(2 ** i)
    raise last_err

def _fetch_google_csv(url: str) -> pd.DataFrame:
    _log("Lendo Google Sheet (CSV)")
    text = _download_to_bytes(url).decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(text), header=None)
    _log(f"CSV lido: linhas={len(df)} colunas={df.shape[1]}")
    return df

_DF_CACHE = {"df": None, "loaded_at": None, "mode": None}

def load_dataframe() -> pd.DataFrame:
    if not GOOGLE_SHEET_CSV_URL:
        _log("Nenhuma URL de planilha configurada.")
        return pd.DataFrame()
    df = _fetch_google_csv(GOOGLE_SHEET_CSV_URL)
    globals()["LAST_LOADED"] = datetime.utcnow()
    globals()["DATA_MODE"]   = "google-csv"
    return df

def get_data() -> pd.DataFrame:
    now = datetime.utcnow()
    if (_DF_CACHE["df"] is None or _DF_CACHE["loaded_at"] is None or
        (now - _DF_CACHE["loaded_at"]) > timedelta(seconds=CACHE_TTL_SECONDS)):
        _log("Recarregando dados (cache expirado ou inexistente)...")
        _DF_CACHE["df"] = load_dataframe()
        _DF_CACHE["loaded_at"] = now
        _DF_CACHE["mode"] = "google-csv"
        _log(f"Cache atualizado | TTL={CACHE_TTL_SECONDS}s | mode={_DF_CACHE['mode']}")
    else:
        age = int((now - _DF_CACHE["loaded_at"]).total_seconds())
        _log(f"Usando cache (idade={age}s / TTL={CACHE_TTL_SECONDS}s)")
    return _DF_CACHE["df"]

# ---------- Funções analíticas ----------
def extract_kv_metrics(df: pd.DataFrame):
    kv = {}
    try:
        for i in range(len(df)):
            row = [str(x).strip() for x in df.iloc[i].tolist() if str(x).strip()]
            if len(row) >= 2:
                kv[row[0].lower().replace(" ", "_")] = row[1]
    except Exception as e:
        _log(f"extract_kv_metrics erro: {e}")
    return kv

def extract_vendas_realizadas(df: pd.DataFrame):
    try:
        idx = df[df[0].astype(str).str.contains("vendas_realizadas", case=False, na=False)].index
        if len(idx) == 0:
            return pd.DataFrame()
        start = idx[0] + 2
        sub = df.iloc[start:].dropna(how="all").reset_index(drop=True)
        sub.columns = sub.iloc[0]
        sub = sub[1:]
        if "Data" in sub.columns:
            sub["Data"] = pd.to_datetime(sub["Data"], errors="coerce", dayfirst=True)
        return sub
    except Exception as e:
        _log(f"extract_vendas_realizadas erro: {e}")
        return pd.DataFrame()

def build_channel_cards(kv: dict):
    canais = []
    for canal in ["Facebook", "Google Ads", "YouTube"]:
        cpl = kv.get(f"{canal.lower().replace(' ','_')}_cpl")
        roas = kv.get(f"{canal.lower().replace(' ','_')}_roas")
        if cpl or roas:
            canais.append({
                "title": canal,
                "body": f"CPL {cpl or '—'} | ROAS {roas or '—'}",
                "tone": "positivo" if roas and float(roas) >= 2 else "alerta"
            })
    return canais

def build_metas_status(kv, qtd_vendas, cpl, inv, orc):
    metas = []
    meta_cpl = kv.get("meta_cpl") or kv.get("meta_cpl_captacao")
    if meta_cpl and cpl:
        metas.append({
            "nome": "CPL Médio",
            "atual": cpl,
            "meta": meta_cpl,
            "status": "verde" if float(cpl) <= float(meta_cpl) else "vermelho"
        })
    return metas

# ---------- Endpoint manual ----------
@app.get("/reload")
def reload_data():
    _log("Recarregando dados manualmente via /reload...")
    _DF_CACHE["df"] = load_dataframe()
    _DF_CACHE["loaded_at"] = datetime.utcnow()
    _DF_CACHE["mode"] = "google-csv"
    return f"✅ Dados recarregados com sucesso em {datetime.now().strftime('%H:%M:%S')}"

# ---------- Rotas ----------
@app.get("/")
def index():
    df = get_data()
    vendas = extract_vendas_realizadas(df)
    kpi_vendas = 0 if vendas.empty else len(vendas)
    kpi_fatur = float(vendas["valor_liquido"].sum()) if ("valor_liquido" in vendas.columns) else 0.0
    return render_template("index.html",
                           linhas=len(df),
                           kpi_vendas=kpi_vendas,
                           kpi_fatur=kpi_fatur,
                           **_ui_globals())

@app.get("/visao-geral")
def visao_geral():
    df = get_data()
    vendas = extract_vendas_realizadas(df)
    kv = extract_kv_metrics(df)

    def _safe_float(val):
        try:
            return float(val)
        except Exception:
            return None

    dias = kv.get("dias_campanha") or 0
    meta_cpl = _safe_float(kv.get("meta_cpl"))
    cpl_atual = _safe_float(kv.get("cpl_medio"))
    inv_usado = _safe_float(kv.get("investimento_total"))
    orc_meta  = _safe_float(kv.get("orcamento_total"))

    delta_cpl = ((cpl_atual - meta_cpl) / meta_cpl * 100) if (meta_cpl and cpl_atual) else 0.0
    delta_orc = ((inv_usado - orc_meta) / orc_meta * 100) if (inv_usado and orc_meta) else 0.0

    topo = dict(
        dias=dias or 0,
        delta_cpl=delta_cpl or 0.0,
        meta_cpl=meta_cpl or 0.0,
        cpl_atual=cpl_atual or 0.0,
        delta_orc=delta_orc or 0.0,
        orc_meta=orc_meta or 0.0,
        inv_usado=inv_usado or 0.0,
        roas=kv.get("roas_geral") or 0.0,
    )

    canais = build_channel_cards(kv)
    metas = build_metas_status(kv, len(vendas), cpl_atual, inv_usado, orc_meta)

    return render_template(
        "visao_geral.html",
        topo=topo,
        canais_cards=canais,
        metas=metas,
        qtd_vendas=len(vendas),
        **_ui_globals()
    )

@app.get("/debug")
def debug():
    df = get_data()
    sample = df.head(20).fillna("").astype(str).to_dict(orient="records")
    cols = list(range(df.shape[1]))
    return render_template("debug.html", cols=cols, rows=sample, **_ui_globals())

# ---------- Execução local ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
