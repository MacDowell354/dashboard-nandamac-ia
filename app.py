# app.py
# -----------------------------------------
# Flask + loader robusto + extrações "inteligentes" das seções da planilha
# Dep.: Flask, gunicorn, pandas, numpy<2.1, requests, openpyxl
# ENV:
#   - GOOGLE_SHEET_CSV_URL  -> https://docs.google.com/.../export?format=csv&gid=XXXX   (prioridade)
#   - DATA_XLSX_PATH        -> (opcional, fallback) https://docs.google.com/.../export?format=xlsx
#   - DATA_CACHE_TTL_SECONDS (opcional; default 300)
# Start (Render): gunicorn app:app --bind 0.0.0.0:$PORT --workers 2
# -----------------------------------------
import os, io, time, math, unicodedata
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict

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
    return {"last_loaded": globals().get("LAST_LOADED"),
            "data_mode":   globals().get("DATA_MODE")}

# ---------- Loader ----------
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
            _log(f"Falha: {e} | tentando novamente em {wait}s")
            time.sleep(wait)
    raise last_err

def _fetch_google_csv(url: str) -> pd.DataFrame:
    _log("Lendo Google Sheet (CSV)")
    text = _download_to_bytes(url).decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(text), header=None)  # grade crua (várias seções)
    _log(f"CSV lido (grade crua): linhas={len(df)} colunas={df.shape[1]}")
    return df

def _fetch_xlsx_from_url(url: str, cache_name="sheet.xlsx") -> pd.DataFrame:
    content = _download_to_bytes(url)
    with open(os.path.join(CACHE_DIR, cache_name), "wb") as f:
        f.write(content)
    df = pd.read_excel(io.BytesIO(content), header=None)
    _log(f"XLSX lido (grade crua): linhas={len(df)} colunas={df.shape[1]}")
    return df

def _read_local_xlsx(path: str) -> pd
