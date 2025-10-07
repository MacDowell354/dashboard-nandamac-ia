import os
from datetime import datetime, timedelta

from flask import Flask, render_template, request
import pandas as pd

from utils import (
    load_inputs_dashboard,
    format_ptbr_int,
    format_ptbr_money,
)

app = Flask(__name__)

# -------------------------
# Cache simples na memória
# -------------------------
_BLOB_CACHE = {"ts": None, "blob": None}
_CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "300"))  # 5 min por padrão


def _now_utc():
    return datetime.utcnow()


def get_blob():
    """Carrega a planilha com cache simples."""
    global _BLOB_CACHE
    ts = _BLOB_CACHE["ts"]
    blob = _BLOB_CACHE["blob"]

    if ts is None or blob is None or (_now_utc() - ts) > timedelta(seconds=_CACHE_TTL):
        src = os.environ.get("DATA_XLSX_PATH") or os.environ.get("GOOGLE_SHEET_CSV_URL")
        blob = load_inputs_dashboard(src)
        _BLOB_CACHE = {"ts": _now_utc(), "blob": blob}
        app.logger.info(f"[INFO] Dados (modo={blob.get('mode')}) carregados às {_BLOB_CACHE['ts']} (UTC).")

    return blob


def df_to_table(df: pd.DataFrame | None):
    """Converte DataFrame para estrutura amigável ao Jinja."""
    if df is None or isinstance(df, pd.DataFrame) and df.empty:
        return None
    df = df.copy()
    # tenta formatar números = melhor visual
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            # dinheiro heurística
            if "valor" in c.lower() or "ticket" in c.lower() or "invest" in c.lower() or "receita" in c.lower():
                df[c] = df[c].apply(format_ptbr_money)
            else:
                df[c] = df[c].apply(format_ptbr_int)
    return {
        "columns": list(map(str, df.columns)),
        "rows": df.fillna("").to_dict(orient="records"),
    }


# -------------------------
# Rotas
# -------------------------

@app.context_processor
def base_ctx():
    """Variáveis disponíveis em todos os templates."""
    blob = get_blob()
    return {
        "current_path": request.path,
        "data_mode": blob.get("mode"),
        "last_loaded": _BLOB_CACHE["ts"],
    }


@app.get("/")
def index():
    blob = get_blob()
    # se for 'blocks', lista chaves encontradas p/ debug
    blocks_info = []
    if blob.get("mode") == "blocks":
        for k, v in blob.items():
            if k == "mode":
                continue
            if isinstance(v, pd.DataFrame):
                blocks_info.append((k, len(v)))
    return render_template("index.html", blocks_info=blocks_info)


@app.get("/visao-geral")
def visao_geral():
    blob = get_blob()
    tables = []
    if blob.get("mode") == "blocks":
        # liste aqui blocos que façam sentido na visão geral (se existirem)
        for key in ("tbl_por_canal", "tbl_por_regiao", "tbl_por_estado", "tbl_taxa_canal"):
            df = blob.get(key)
            tables.append((key, df_to_table(df)))
    else:
        # modo "long": você poderia agregar aqui e gerar 1–2 tabelas
        df = blob.get("df")
        tables.append(("amostra_long", df_to_table(df.head(50) if df is not None else None)))
    return render_template("visao_geral.html", tables=tables)


@app.get("/origem-conversao")
def origem_conversao():
    blob = get_blob()
    tbl_canal = df_to_table(blob.get("tbl_por_canal"))
    tbl_taxa = df_to_table(blob.get("tbl_taxa_canal"))
    return render_template("origem_conversao.html", tbl_canal=tbl_canal, tbl_taxa=tbl_taxa)


@app.get("/profissao-por-canal")
def profissao_por_canal():
    blob = get_blob()
    tbl = df_to_table(blob.get("tbl_prof_canal"))
    return render_template("profissao_por_canal.html", tbl=tbl)


@app.get("/analise-regional")
def analise_regional():
    blob = get_blob()
    tbl_estado = df_to_table(blob.get("tbl_por_estado"))
    tbl_regiao = df_to_table(blob.get("tbl_por_regiao"))
    return render_template("analise_regional.html", tbl_estado=tbl_estado, tbl_regiao=tbl_regiao)


@app.get("/projecao-resultados")
def projecao_resultados():
    blob = get_blob()
    # série mensal (se existir)
    tbl = df_to_table(blob.get("serie_mensal"))
    return render_template("projecao_resultados.html", tbl=tbl)


# -------------------------
# Run local (opcional)
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=bool(int(os.environ.get("FLASK_DEBUG", "0"))))
