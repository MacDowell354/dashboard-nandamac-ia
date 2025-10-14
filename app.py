# app.py
# -----------------------------------------
# Flask + loader robusto + extrações "inteligentes" das seções da planilha
# Dep.: Flask, gunicorn, pandas, numpy<2.1, requests, openpyxl
# ENV:
#   - GOOGLE_SHEET_CSV_URL  -> .../export?format=csv&gid=XXXX   (prioridade)
#   - DATA_XLSX_PATH        -> (opcional, fallback) .../export?format=xlsx
#   - DATA_CACHE_TTL_SECONDS (opcional; default 300)
# Start (Render): gunicorn app:app --bind 0.0.0.0:$PORT --workers 2
# -----------------------------------------
import os, io, time, math, unicodedata
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

# ---------- Utilidades p/ localizar seções ----------
def _strip_accents_lower(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('ascii')
    return s.strip().lower()

def _first_match_contains(series: pd.Series, needle: str) -> Optional[int]:
    n = _strip_accents_lower(needle)
    ser = series.astype(str).map(_strip_accents_lower)
    mask = ser.str.contains(n, regex=False)
    if mask.any(): return int(mask.idxmax())
    return None

def _first_eq(series: pd.Series, value: str) -> Optional[int]:
    v = _strip_accents_lower(value)
    ser = series.astype(str).map(_strip_accents_lower)
    mask = ser.eq(v)
    if mask.any(): return int(mask.idxmax())
    return None

def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Garante nomes de colunas únicos e legíveis."""
    seen = {}
    new_cols = []
    for c in df.columns:
        base = str(c).strip() if c is not None else ""
        if base == "" or base.lower() == "nan":
            base = "col"
        name = base
        k = 1
        while name in seen:
            k += 1
            name = f"{base}_{k}"
        seen[name] = 1
        new_cols.append(name)
    df.columns = new_cols
    return df

def _col_is_all_empty(df: pd.DataFrame, col_name: str) -> bool:
    """True se a coluna (ou conjunto de colunas com o mesmo nome) for toda vazia."""
    obj = df.loc[:, col_name]
    if isinstance(obj, pd.DataFrame):
        return obj.apply(lambda s: s.astype(str).str.strip().eq("")).all().all()
    return obj.astype(str).str.strip().eq("").all()

# ---------- Extrações específicas ----------
def extract_vendas_realizadas(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Seção 'vendas_realizadas' até a linha 'Total'."""
    if df_raw.empty: return pd.DataFrame()
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

    if "Data" in sub.columns:
        sub["Data"] = pd.to_datetime(sub["Data"], errors="coerce", dayfirst=True)
    for col in ["valor_venda", "valor_liquido"]:
        if col in sub.columns:
            sub[col] = (
                sub[col].astype(str)
                .str.replace("R$", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
            sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(0.0)
    return sub

def extract_projecao(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Seção 'progecao_de_resultados' limpa."""
    if df_raw.empty: return pd.DataFrame()
    start = _first_match_contains(df_raw[0], "progecao_de_resultados")
    if start is None:
        _log("Seção 'progecao_de_resultados' não encontrada.")
        return pd.DataFrame()
    header_row = start + 1
    end = header_row + 1
    blank = 0
    while end < len(df_raw):
        row_is_blank = df_raw.iloc[end].isna().all()
        blank = blank + 1 if row_is_blank else 0
        if blank >= 2: break
        end += 1
    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)

    def _norm(i, c):
        if c is None: return f"col_{i}"
        s = str(c).strip()
        return f"col_{i}" if s == "" or s.lower() == "nan" else s

    sub.columns = [_norm(i, c) for i, c in enumerate(sub.columns)]
    sub = sub.dropna(how="all").fillna("")
    empty_cols = [c for c in sub.columns if sub[c].astype(str).str.strip().eq("").all()]
    if empty_cols: sub = sub.drop(columns=empty_cols)

    wanted = ["Métrica","performance_real","projecao_lancamento","potencial_otimista"]
    lower_map = {str(c).casefold(): c for c in sub.columns}
    sel = [lower_map.get(k.casefold()) for k in wanted if lower_map.get(k.casefold())]
    if sel: sub = sub[sel]
    return sub

def extract_profissoes_por_canal(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Bloco 'PROFISSOES': primeira coluna = profissão; demais colunas = canais.
    Terminamos quando a primeira coluna contém 'Total Geral' ou linha vazia.
    """
    if df_raw.empty:
        return pd.DataFrame()

    start = _first_match_contains(df_raw[0], "profissoes")
    if start is None:
        _log("Bloco 'PROFISSOES' não encontrado.")
        return pd.DataFrame()

    header_row = start + 2
    data_start = header_row + 1

    end = data_start
    while end < len(df_raw):
        first = str(df_raw.iloc[end, 0]).strip()
        if first.lower().startswith("total geral") or df_raw.iloc[end].isna().all():
            break
        end += 1

    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)
    sub = _dedupe_columns(sub).fillna("")

    if sub.columns[0] != "Profissao":
        cols = list(sub.columns)
        cols[0] = "Profissao"
        sub.columns = cols

    drop_cols = [c for c in sub.columns if c != "Profissao" and _col_is_all_empty(sub, c)]
    if drop_cols:
        sub = sub.drop(columns=drop_cols)

    for c in sub.columns:
        if c == "Profissao":
            continue
        obj = sub.loc[:, c]
        if isinstance(obj, pd.DataFrame):
            num = obj.apply(lambda s: pd.to_numeric(
                s.astype(str).str.replace("%","",regex=False).str.replace(".","",regex=False).str.replace(",",".",regex=False),
                errors="coerce"
            )).fillna(0)
            sub[c] = num.sum(axis=1)
        else:
            serie = obj.astype(str).str.replace("%","",regex=False).str.replace(".","",regex=False).str.replace(",",".",regex=False)
            nums = pd.to_numeric(serie, errors="coerce")
            if nums.notna().mean() >= 0.5:
                sub[c] = nums.fillna(0)

    return sub

def extract_estado_profissao(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Tabela 'ESTADO X PROFISSÃO' (primeira coluna = Estado)."""
    if df_raw.empty:
        return pd.DataFrame()

    start = _first_match_contains(df_raw[0], "estado x profissao")
    if start is None:
        _log("Tabela 'Estado x Profissão' não encontrada.")
        return pd.DataFrame()

    header_row = start + 1
    data_start = header_row + 1
    end = data_start
    while end < len(df_raw):
        first = str(df_raw.iloc[end, 0]).strip().lower()
        if first == "" or first.startswith("regiao por profissao"):
            break
        end += 1

    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)
    sub = _dedupe_columns(sub).fillna("")

    if sub.columns[0] != "Estado":
        cols = list(sub.columns)
        cols[0] = "Estado"
        sub.columns = cols

    for c in sub.columns[1:]:
        obj = sub.loc[:, c]
        if isinstance(obj, pd.DataFrame):
            num = obj.apply(lambda s: pd.to_numeric(
                s.astype(str).str.replace(".","",regex=False).str.replace(",",".",regex=False),
                errors="coerce"
            )).fillna(0)
            sub[c] = num.sum(axis=1)
        else:
            serie = obj.astype(str).str.replace(".","",regex=False).str.replace(",",".",regex=False)
            nums = pd.to_numeric(serie, errors="coerce")
            if nums.notna().mean() >= 0.5:
                sub[c] = nums.fillna(0)

    sub = sub[sub["Estado"].astype(str).str.strip() != ""].reset_index(drop=True)
    return sub

def extract_regiao_profissao(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Tabela 'REGIÃO POR PROFISSÃO' (primeira coluna = Região)."""
    if df_raw.empty:
        return pd.DataFrame()

    start = _first_match_contains(df_raw[0], "regiao por profissao")
    if start is None:
        _log("Tabela 'Região por Profissão' não encontrada.")
        return pd.DataFrame()

    header_row = start + 1
    data_start = header_row + 1
    end = data_start
    blank = 0
    while end < len(df_raw):
        row_is_blank = df_raw.iloc[end].isna().all()
        blank = blank + 1 if row_is_blank else 0
        if blank >= 1: break
        end += 1

    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)
    sub = _dedupe_columns(sub).fillna("")

    if sub.columns[0] != "Regiao":
        cols = list(sub.columns)
        cols[0] = "Regiao"
        sub.columns = cols

    for c in sub.columns[1:]:
        obj = sub.loc[:, c]
        if isinstance(obj, pd.DataFrame):
            num = obj.apply(lambda s: pd.to_numeric(
                s.astype(str).str.replace(".","",regex=False).str.replace(",",".",regex=False),
                errors="coerce"
            )).fillna(0)
            sub[c] = num.sum(axis=1)
        else:
            serie = obj.astype(str).str.replace(".","",regex=False).str.replace(",",".",regex=False)
            nums = pd.to_numeric(serie, errors="coerce")
            if nums.notna().mean() >= 0.5:
                sub[c] = nums.fillna(0)

    sub = sub[sub["Regiao"].astype(str).str.strip() != ""].reset_index(drop=True)
    return sub

# ---------- Rotas ----------
@app.get("/")
def index():
    df_raw = get_data()
    vendas = extract_vendas_realizadas(df_raw)
    proj   = extract_projecao(df_raw)
    kpi_vendas = 0 if vendas.empty else len(vendas)
    kpi_fatur  = 0.0
    if not vendas.empty and "valor_liquido" in vendas.columns:
        kpi_fatur = float(vendas["valor_liquido"].sum())
    return render_template("index.html",
                           linhas=len(df_raw),
                           kpi_vendas=kpi_vendas,
                           kpi_fatur=kpi_fatur,
                           kpi_proj_linhas=0 if proj.empty else len(proj),
                           **_ui_globals())

@app.get("/visao-geral")
def visao_geral():
    df_raw = get_data()
    vendas = extract_vendas_realizadas(df_raw)
    profcan= extract_profissoes_por_canal(df_raw)

    total_liquido = float(vendas["valor_liquido"].sum()) if (not vendas.empty and "valor_liquido" in vendas.columns) else 0.0
    top_canal, top_val = None, 0
    if not profcan.empty:
        num_cols = [c for c in profcan.columns if c!="Profissao" and pd.api.types.is_numeric_dtype(profcan[c])]
        if num_cols:
            soma = profcan[num_cols].sum().sort_values(ascending=False)
            if len(soma):
                top_canal, top_val = soma.index[0], float(soma.iloc[0])

    return render_template("visao_geral.html",
                           has_vendas=not vendas.empty,
                           total_vendas=0 if vendas.empty else len(vendas),
                           total_liquido=total_liquido,
                           top_canal=top_canal, top_val=top_val,
                           **_ui_globals())

@app.get("/origem-conversao")
def origem_conversao():
    df_raw = get_data()
    profcan = extract_profissoes_por_canal(df_raw)
    canais = []
    if not profcan.empty:
        num_cols = [c for c in profcan.columns if c!="Profissao" and pd.api.types.is_numeric_dtype(profcan[c])]
        if num_cols:
            tot = profcan[num_cols].sum()
            canais = [{"canal": c, "qtde": float(tot[c])} for c in num_cols]
            canais.sort(key=lambda x: x["qtde"], reverse=True)
    return render_template("origem_conversao.html",
                           canais=canais, has_data=len(canais)>0,
                           **_ui_globals())

@app.get("/profissao-por-canal")
def profissao_por_canal():
    df_raw = get_data()
    profcan = extract_profissoes_por_canal(df_raw)
    has_data = not profcan.empty
    table = profcan.to_dict(orient="records") if has_data else []
    series = []
    if has_data:
        num_cols = [c for c in profcan.columns if c!="Profissao" and pd.api.types.is_numeric_dtype(profcan[c])]
        somas = profcan[num_cols].sum().sort_values(ascending=False) if num_cols else pd.Series(dtype=float)
        top_cols = list(somas.head(8).index)
        for canal in top_cols:
            pts = [{"x": p, "y": float(v)} for p, v in zip(profcan["Profissao"], profcan[canal])]
            series.append({"canal": canal, "data": pts})
    return render_template("profissao_por_canal.html",
                           has_data=has_data, table=table, series=series,
                           **_ui_globals())

@app.get("/analise-regional")
def analise_regional():
    df_raw = get_data()
    uf = extract_estado_profissao(df_raw)
    reg = extract_regiao_profissao(df_raw)
    serie_uf, serie_reg = [], []
    if not uf.empty:
        num_cols = [c for c in uf.columns if c!="Estado" and pd.api.types.is_numeric_dtype(uf[c])]
        if num_cols:
            uf["TOTAL"] = uf[num_cols].sum(axis=1)
            serie_uf = [{"x": e, "y": float(v)} for e, v in zip(uf["Estado"], uf["TOTAL"])]
            serie_uf = sorted(serie_uf, key=lambda x: x["y"], reverse=True)[:20]
    if not reg.empty:
        num_cols = [c for c in reg.columns if c!="Regiao" and pd.api.types.is_numeric_dtype(reg[c])]
        if num_cols:
            reg["TOTAL"] = reg[num_cols].sum(axis=1)
            serie_reg = [{"x": r, "y": float(v)} for r, v in zip(reg["Regiao"], reg["TOTAL"])]
    return render_template("analise_regional.html",
                           has_uf=not uf.empty, has_reg=not reg.empty,
                           uf_table=uf.to_dict(orient="records") if not uf.empty else [],
                           reg_table=reg.to_dict(orient="records") if not reg.empty else [],
                           serie_uf=serie_uf, serie_reg=serie_reg,
                           **_ui_globals())

@app.get("/insights-ia")
def insights_ia():
    df_raw = get_data()
    vendas = extract_vendas_realizadas(df_raw)
    profcan= extract_profissoes_por_canal(df_raw)
    uf     = extract_estado_profissao(df_raw)

    insights = []
    if not vendas.empty and "Data" in vendas.columns:
        by_day = vendas[["Data","valor_liquido"]].dropna().groupby(vendas["Data"].dt.date)["valor_liquido"].sum().reset_index()
        if len(by_day) >= 3:
            last3 = by_day["valor_liquido"].tail(3).tolist()
            trend = "alta" if last3[-1] >= last3[0] else "queda"
            insights.append(f"Vendas dos últimos 3 dias sugerem {trend}.")
    if not profcan.empty:
        num_cols = [c for c in profcan.columns if c!="Profissao" and pd.api.types.is_numeric_dtype(profcan[c])]
        if num_cols:
            s = profcan[num_cols].sum().sort_values(ascending=False)
            insights.append(f"Canal com maior volume: {s.index[0]} ({int(s.iloc[0])}).")
    if not uf.empty:
        num_cols = [c for c in uf.columns if c!="Estado" and pd.api.types.is_numeric_dtype(uf[c])]
        if num_cols:
            uf["TOTAL"] = uf[num_cols].sum(axis=1)
            top = uf.sort_values("TOTAL", ascending=False).iloc[0]
            insights.append(f"UF com maior volume: {top['Estado']} ({int(top['TOTAL'])}).")

    return render_template("insights_ia.html",
                           insights=insights, **_ui_globals())

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

@app.get("/debug")
def debug_grid():
    df_raw = get_data()
    sample = df_raw.head(30).fillna("").astype(str).to_dict(orient="records")
    cols = list(range(df_raw.shape[1]))
    return render_template("debug.html", cols=cols, rows=sample, **_ui_globals())
