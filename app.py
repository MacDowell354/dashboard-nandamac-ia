# app.py
# -----------------------------------------
# Flask + loader robusto + extrações "inteligentes" da planilha
# Dep.: Flask, gunicorn, pandas, numpy<2.1, requests, openpyxl
# ENV:
#   - GOOGLE_SHEET_CSV_URL  -> .../export?format=csv&gid=XXXX   (prioridade)
#   - DATA_XLSX_PATH        -> (opcional, fallback) .../export?format=xlsx
#   - DATA_CACHE_TTL_SECONDS (opcional; default 300)
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

# ---------- Normalizadores/Key-Value ----------
def _to_number_pt(s: str) -> float | None:
    """Converte 'R$ 140.000,00', '3.456,7%', '9.000' -> float. Vazio -> None."""
    if s is None:
        return None
    txt = str(s).strip()
    if txt == "" or txt.lower() == "nan":
        return None
    txt = txt.replace("R$", "").replace("%", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(txt)
    except Exception:
        return None

def extract_kv_metrics(df_raw: pd.DataFrame) -> dict:
    """
    Lê o bloco superior 'CAMPO | DESCRIÇÃO | VALOR ATUAL ...' até 'PROFISSOES'.
    Retorna {chave_normalizada: valor_num (ou string se não for número)}.
    """
    if df_raw.empty:
        return {}

    start = _first_eq(df_raw[0], "CAMPO")
    if start is None:
        start = _first_match_contains(df_raw[0], "campo")
    if start is None:
        _log("Bloco de métricas (CAMPO...) não encontrado.")
        return {}

    end = start + 1
    while end < len(df_raw):
        a = str(df_raw.iloc[end, 0]).strip().lower()
        if a.startswith("profissoes"):
            break
        if end - start > 200:
            break
        end += 1

    sub = df_raw.iloc[start:end].reset_index(drop=True)
    if sub.empty or len(sub) < 2:
        return {}

    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)

    cols_map = {str(c).strip().lower(): c for c in sub.columns}
    c_key = cols_map.get("campo") or list(sub.columns)[0]
    c_val = None
    for k in ["valor atual", "valor_atual", "valor"]:
        if k in cols_map:
            c_val = cols_map[k]
            break
    if c_val is None:
        c_val = sub.columns[2] if len(sub.columns) > 2 else (sub.columns[1] if len(sub.columns) > 1 else sub.columns[0])

    metrics = {}
    for _, row in sub.iterrows():
        key_raw = str(row.get(c_key, "")).strip()
        if key_raw == "" or key_raw.lower().startswith("profissoes"):
            continue
        val_raw = row.get(c_val, "")
        val_num = _to_number_pt(val_raw)
        key = _strip_accents_lower(key_raw).replace(" ", "_")
        metrics[key] = val_num if val_num is not None else str(val_raw).strip()
    return metrics

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
    Lê a seção 'PROFISSOES' mesmo com linhas de cabeçalho/explicação e pares de colunas Qtd/%.
    Retorna: Profissao | <canal1> | <canal2> | ...
    """
    if df_raw.empty:
        return pd.DataFrame()

    start = _first_match_contains(df_raw[0], "profissoes")
    if start is None:
        _log("Bloco 'PROFISSOES' não encontrado.")
        return pd.DataFrame()

    # Detecta melhor linha de header
    header_row = None
    channel_keywords = ["youtube", "facebook", "instagram", "email", "manychat", "redirect", "total"]
    for r in range(start + 1, min(len(df_raw), start + 16)):
        row = df_raw.iloc[r].astype(str).map(_strip_accents_lower).tolist()
        non_empty = [c for c in row if c not in ("", "nan")]
        if len(non_empty) >= 4:
            score = sum(1 for c in row if any(kw in c for kw in channel_keywords))
            if score >= 2:
                header_row = r
                break
    if header_row is None:
        header_row = start + 2

    # Primeira linha de dados com números
    data_start = header_row + 1
    while data_start < len(df_raw):
        row = df_raw.iloc[data_start]
        a0 = str(row.iloc[0]).strip()
        nums = pd.to_numeric(row[1:].astype(str).str.replace("%","",regex=False)
                                      .str.replace(".","",regex=False)
                                      .str.replace(",",".",regex=False),
                             errors="coerce")
        if a0 not in ("", "nan") and nums.notna().sum() >= 2:
            break
        data_start += 1

    # Fim
    end = data_start
    while end < len(df_raw):
        first = str(df_raw.iloc[end, 0]).strip()
        if first.lower().startswith("total geral") or df_raw.iloc[end].isna().all():
            break
        end += 1

    sub = df_raw.iloc[header_row:end].reset_index(drop=True)
    if len(sub) < 2:
        _log("PROFISSOES: estrutura inesperada (header curto).")
        return pd.DataFrame()

    sub.columns = sub.iloc[0].tolist()
    sub = sub[1:].reset_index(drop=True)
    sub = _dedupe_columns(sub).fillna("")

    if sub.columns[0] != "Profissao":
        cols = list(sub.columns); cols[0] = "Profissao"; sub.columns = cols

    # Mantém apenas colunas de QUANTIDADE
    keep = ["Profissao"]
    for c in sub.columns[1:]:
        name_norm = _strip_accents_lower(c)
        if "%" in name_norm:  # ignora percentuais
            continue
        obj = sub.loc[:, c]
        if isinstance(obj, pd.DataFrame):
            num = obj.apply(lambda s: pd.to_numeric(
                s.astype(str).str.replace("%","",regex=False)
                              .str.replace(".","",regex=False)
                              .str.replace(",",".",regex=False),
                errors="coerce"
            )).fillna(0)
            series = num.sum(axis=1)
        else:
            series = pd.to_numeric(
                obj.astype(str).str.replace("%","",regex=False)
                               .str.replace(".","",regex=False)
                               .str.replace(",",".",regex=False),
                errors="coerce"
            )
        if series.notna().mean() >= 0.5 and (series.fillna(0) != 0).any():
            keep.append(c)

    sub = sub[keep]

    # Converte para número
    for c in sub.columns:
        if c == "Profissao": continue
        sub[c] = pd.to_numeric(
            sub[c].astype(str).str.replace("%","",regex=False)
                               .str.replace(".","",regex=False)
                               .str.replace(",",".",regex=False),
            errors="coerce"
        ).fillna(0)

    sub = sub[sub["Profissao"].astype(str).str.strip().ne("")].copy()
    sub = sub[~sub["Profissao"].astype(str).str.lower().str.startswith("total geral")].reset_index(drop=True)
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
        cols = list(sub.columns); cols[0] = "Estado"; sub.columns = cols

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
        cols = list(sub.columns); cols[0] = "Regiao"; sub.columns = cols

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

# ---------- Helpers da Visão Geral ----------
def _kv(kv: dict, *aliases, default=None):
    for a in aliases:
        k = _strip_accents_lower(a).replace(" ", "_")
        if k in kv and kv[k] not in (None, "", "nan"):
            return kv[k]
    return default

def _pct_delta(atual: float | None, meta: float | None):
    try:
        if atual is None or meta in (None, 0): return None
        return (float(atual) - float(meta)) / float(meta) * 100.0
    except Exception:
        return None

def build_channel_cards(kv: dict) -> list[dict]:
    total_leads = _kv(kv, "Total_Leads", "Total de Leads")
    canais = [
        dict(key="Facebook",   leads=_kv(kv, "Facebook_Leads"),   cpl=_kv(kv, "Facebook_CPL"),   roas=_kv(kv, "Facebook_ROAS")),
        dict(key="Google Ads", leads=_kv(kv, "Google_Ads_Leads"), cpl=_kv(kv, "Google_Ads_CPL"), roas=_kv(kv, "Google_Ads_ROAS")),
        dict(key="YouTube",    leads=_kv(kv, "YouTube_Leads"),    cpl=_kv(kv, "YouTube_CPL"),    roas=_kv(kv, "YouTube_ROAS")),
    ]
    for c in canais:
        try:
            c["share"] = (float(c["leads"]) / float(total_leads) * 100.0) if (total_leads and c["leads"]) else None
        except Exception:
            c["share"] = None

    cards = []
    for c in canais:
        nome, share, roas, cpl = c["key"], c["share"], c["roas"], c["cpl"]
        if share and roas and share >= 30 and roas >= 2.2:
            cards.append(dict(title=f"{nome} Dominante",
                              body=f"{share:.1f}% dos leads com ROAS de {roas:.2f}. Canal principal com excelente performance.",
                              tone="positivo"))
        elif roas and roas >= 2.2:
            cards.append(dict(title=f"{nome} Bom ROAS",
                              body=f"ROAS de {roas:.2f} com bom retorno. Otimizar volume de leads para escalar.",
                              tone="neutro"))
        else:
            body = "Avaliar custo e retorno deste canal."
            if cpl and roas:
                body = f"CPL de {br_money(cpl)} e ROAS de {roas:.2f}. Requer otimização para melhorar retorno."
            elif cpl:
                body = f"CPL de {br_money(cpl)}. Reduzir custo por lead."
            cards.append(dict(title=f"{nome} Precisa Otimizar", body=body, tone="alerta"))
    return [x for x in cards if x]

def build_metas_status(kv: dict, total_vendas: float | int | None,
                       cpl_medio: float | None,
                       investido: float | None,
                       orc_planejado: float | None) -> list[dict]:
    metas = []
    meta_cpl = kv.get("meta_cpl") or kv.get("meta_cpl_captacao")
    if meta_cpl is not None and cpl_medio is not None:
        atual, alvo = float(cpl_medio), float(meta_cpl)
        gap = (atual - alvo) / alvo * 100 if alvo else 0
        status = "verde" if atual <= alvo else ("amarelo" if gap <= 10 else "vermelho")
        metas.append(dict(nome="CPL Médio", atual=atual, meta=alvo,
                          atingimento=100 * (alvo / atual) if atual else 0,
                          direcao="down", status=status))
    meta_vendas = kv.get("meta_quantidade_vendas_curso")
    if meta_vendas is not None and total_vendas is not None:
        atual, alvo = float(total_vendas), float(meta_vendas)
        perc = (atual / alvo * 100) if alvo else 0
        status = "verde" if atual >= alvo else ("amarelo" if perc >= 80 else "vermelho")
        metas.append(dict(nome="Vendas (Curso)", atual=atual, meta=alvo,
                          atingimento=perc, direcao="up", status=status))
    if orc_planejado is not None and investido is not None:
        exec_perc = (float(investido) / float(orc_planejado) * 100) if orc_planejado else 0
        status = "verde" if exec_perc <= 100 else ("amarelo" if exec_perc <= 110 else "vermelho")
        metas.append(dict(nome="Execução do Orçamento", atual=float(investido), meta=float(orc_planejado),
                          atingimento=exec_perc, direcao="budget", status=status))
    return metas

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
    kv     = extract_kv_metrics(df_raw)

    # Datas/duração
    dias_camp = kv.get("dias_campanha")
    if dias_camp is None:
        dt_ini = kv.get("data_inicio") or kv.get("data_inicio_")
        try:
            if dt_ini:
                d0 = pd.to_datetime(str(dt_ini), dayfirst=True)
                dias_camp = max(1, (pd.Timestamp.today().normalize() - d0.normalize()).days)
        except Exception:
            pass

    # KPIs do bloco CAMPO
    meta_cpl     = kv.get("meta_cpl") or kv.get("meta_cpl_captacao")
    cpl_atual    = kv.get("cpl_medio") or kv.get("cpl_media")
    inv_usado    = kv.get("investimento_total") or kv.get("investimento_trafego_captacao")
    orc_meta     = kv.get("meta_orcamento_trafego") or kv.get("orcamento_total") or kv.get("meta_orcamento_investimento_em_trafego")
    roas_geral   = kv.get("roas_geral") or kv.get("roas_total")
    total_leads  = kv.get("total_leads")
    taxa_conv    = kv.get("taxa_conversao")
    ticket_curso = kv.get("ticket_medio_curso") or kv.get("preco_curso")
    perc_ment    = kv.get("%_vendas_mentorias") or kv.get("percentual_vendas_mentoria")
    ticket_ment  = kv.get("ticket_medio_mentoria") or kv.get("preco_mentoria")
    seg_yt       = kv.get("numero_seguidores_youtube") or kv.get("seguidores_youtube")
    seg_insta    = kv.get("numero_seguidores_instagram") or kv.get("seguidores_instagram")
    meta_leads   = kv.get("meta_captacao_leads") or kv.get("meta_leads")

    # Fallbacks/cálculos
    qtd_vendas   = 0 if vendas.empty else len(vendas)
    fatur_liq    = float(vendas["valor_liquido"].sum()) if (not vendas.empty and "valor_liquido" in vendas.columns) else 0.0
    if roas_geral is None and inv_usado and fatur_liq:
        roas_geral = (float(fatur_liq) / float(inv_usado)) if float(inv_usado) > 0 else None

    delta_cpl    = _pct_delta(cpl_atual, meta_cpl)
    delta_orc    = _pct_delta(inv_usado, orc_meta)

    conv_global = None
    if total_leads and float(total_leads) > 0:
        conv_global = (qtd_vendas / float(total_leads)) * 100.0

    canais_cards = build_channel_cards(kv)

    kpis_cards = [
        dict(titulo="Meta Captação LEADS",  valor=meta_leads,    subtitulo=f"Atual: {int(total_leads):,}".replace(",",".") if total_leads else "—", tipo="numero"),
        dict(titulo="Taxa de Conversão",    valor=taxa_conv,     subtitulo="Premissa Histórica", tipo="percent"),
        dict(titulo="Ticket Médio Curso",   valor=ticket_curso,  subtitulo="Preço de Venda", tipo="money"),
        dict(titulo="% Vendas Mentorias",   valor=perc_ment,     subtitulo="Upsell sobre Curso", tipo="percent"),
        dict(titulo="Ticket Médio Mentoria",valor=ticket_ment,   subtitulo="Preço Premium", tipo="money"),
        dict(titulo="Meta CPL Captação",    valor=meta_cpl,      subtitulo=f"Atual: {br_money(cpl_atual)}{f' ({delta_cpl:+.1f}%)' if delta_cpl is not None else ''}", tipo="money"),
        dict(titulo="Meta Orçamento Tráfego",valor=orc_meta,     subtitulo=f"Usado: {br_money(inv_usado)}{f' ({delta_orc:+.1f}%)' if delta_orc is not None else ''}", tipo="money"),
        dict(titulo="Seguidores YouTube",   valor=seg_yt,        subtitulo="Atualizado hoje", tipo="numero"),
        dict(titulo="Seguidores Instagram", valor=seg_insta,     subtitulo="Atualizado hoje", tipo="numero"),
    ]

    topo = dict(
        dias=dias_camp,
        delta_cpl=delta_cpl, meta_cpl=meta_cpl, cpl_atual=cpl_atual,
        delta_orc=delta_orc, orc_meta=orc_meta, inv_usado=inv_usado,
        roas=roas_geral
    )

    # Metas & semáforos (opcional na UI)
    metas = build_metas_status(kv, qtd_vendas, cpl_atual, inv_usado, orc_meta)

    return render_template("visao_geral.html",
        topo=topo,
        canais_cards=canais_cards,
        kpis_cards=kpis_cards,
        metas=metas,
        qtd_vendas=qtd_vendas,
        fatur_liq=fatur_liq,
        conv_global=conv_global,
        **_ui_globals()
    )

@app.get("/origem-conversao")
def origem_conversao():
    df_raw = get_data()
    profcan = extract_profissoes_por_canal(df_raw)
    vendas  = extract_vendas_realizadas(df_raw)

    canais = []
    funil  = []

    if not profcan.empty:
        num_cols = [c for c in profcan.columns if c!="Profissao" and pd.api.types.is_numeric_dtype(profcan[c])]
        if num_cols:
            leads_por_canal = profcan[num_cols].sum()
            total_leads = float(leads_por_canal.sum())
            total_vendas = 0 if vendas.empty else len(vendas)
            vendas_por_canal = {}
            if total_leads > 0 and total_vendas > 0:
                for canal, qtd in leads_por_canal.items():
                    vendas_por_canal[canal] = (float(qtd) / total_leads) * total_vendas
            for canal in num_cols:
                leads = float(leads_por_canal.get(canal, 0.0))
                vds   = float(vendas_por_canal.get(canal, 0.0)) if vendas_por_canal else 0.0
                conv  = (vds / leads * 100.0) if leads > 0 else 0.0
                canais.append({"canal": canal, "qtde": leads})
                funil.append({"canal": canal, "leads": leads, "vendas": vds, "conv": conv})

    canais.sort(key=lambda x: x["qtde"], reverse=True)
    funil.sort(key=lambda x: x["leads"], reverse=True)

    return render_template("origem_conversao.html",
                           canais=canais, funil=funil, has_data=len(canais)>0,
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
