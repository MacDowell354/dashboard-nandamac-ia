# app.py
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from flask import Flask, jsonify, render_template, request
from utils import (
    load_inputs_dashboard, format_ptbr_money, format_ptbr_int
)
import pandas as pd

app = Flask(__name__, template_folder="templates", static_folder="static")

DATA_BLOB = None
LAST_LOAD = None

def _load_data():
    global DATA_BLOB, LAST_LOAD
    DATA_BLOB = load_inputs_dashboard()
    LAST_LOAD = datetime.utcnow()

# Carrega na subida
_load_data()

# ---------------------------------
# Filtros Jinja
# ---------------------------------
@app.template_filter("ptint")
def _ptint(x):
    return format_ptbr_int(x)

@app.template_filter("ptmoney")
def _ptmoney(x):
    return format_ptbr_money(x)

# ---------------------------------
# Rotas utilitárias
# ---------------------------------
@app.route("/reload")
def reload_data():
    _load_data()
    return jsonify({"ok": True, "loaded_at_utc": LAST_LOAD.isoformat()})

@app.route("/api/status")
def api_status():
    mode = DATA_BLOB.get("mode") if isinstance(DATA_BLOB, dict) else "unknown"
    return jsonify({
        "mode": mode,
        "loaded_at_utc": LAST_LOAD.isoformat() if LAST_LOAD else None,
        "has_kpis": bool(DATA_BLOB.get("kpis")) if isinstance(DATA_BLOB, dict) else False,
        "keys": list(DATA_BLOB.keys()) if isinstance(DATA_BLOB, dict) else [],
    })

@app.route("/api/blob")
def api_blob():
    # Exporta tamanhos e amostras, para debug
    out = {"mode": DATA_BLOB.get("mode")}
    for k, v in DATA_BLOB.items():
        if k == "mode":
            continue
        if isinstance(v, pd.DataFrame):
            out[k] = {
                "rows": int(v.shape[0]),
                "cols": int(v.shape[1]),
                "columns": list(map(str, v.columns)),
                "sample": v.head(5).fillna("").astype(str).to_dict(orient="records"),
            }
        else:
            out[k] = v if isinstance(v, dict) else str(type(v))
    out["loaded_at_utc"] = LAST_LOAD.isoformat() if LAST_LOAD else None
    return jsonify(out)

# ---------------------------------
# Páginas (mantém seus endpoints)
# Se você já tem templates, eles continuarão funcionando:
# usarão DATA_BLOB conforme o modo.
# ---------------------------------
@app.context_processor
def inject_globals():
    return {
        "loaded_at": LAST_LOAD,
        "mode": DATA_BLOB.get("mode"),
        "blob": DATA_BLOB,
    }

@app.route("/")
def index():
    # Mostra um sumário mínimo (seus templates podem ir além)
    blocks_info = []
    if DATA_BLOB.get("mode") == "csv":
        df = DATA_BLOB["long"]
        total_leads = int(df["leads"].sum()) if "leads" in df.columns else 0
        total_vendas = int(df["vendas"].sum()) if "vendas" in df.columns else 0
        total_valor = float(df["valor"].sum()) if "valor" in df.columns else 0.0
        blocks_info = [
            ("Total de Leads", format_ptbr_int(total_leads)),
            ("Total de Vendas", format_ptbr_int(total_vendas)),
            ("Faturamento", format_ptbr_money(total_valor)),
        ]
    else:
        # XLSX: tenta pegar do dicionário de KPIs
        kpis = DATA_BLOB.get("kpis", {})
        def getk(key):
            return kpis.get(key, "-")
        blocks_info = [
            ("Total_Leads", getk("total_leads")),
            ("cpl_medio", getk("cpl_medio")),
            ("investimento_total", getk("investimento_total")),
            ("roas_geral", getk("roas_geral")),
        ]
    return render_template("index.html", blocks_info=blocks_info)

@app.route("/visao-geral")
def visao_geral():
    return render_template("visao_geral.html")

@app.route("/origem-conversao")
def origem_conversao():
    return render_template("origem_conversao.html")

@app.route("/profissao-por-canal")
def profissao_por_canal():
    return render_template("profissao_por_canal.html")

@app.route("/analise-regional")
def analise_regional():
    return render_template("analise_regional.html")

@app.route("/projecao-resultados")
def projecao_resultados():
    return render_template("projecao_resultados.html")

@app.route("/acompanhamento-vendas")
def acompanhamento_vendas():
    return render_template("acompanhamento_vendas.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
