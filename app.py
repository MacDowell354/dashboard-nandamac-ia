import os
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
import pandas as pd

from utils import (
    load_dataframe, group_count, group_sum, group_avg,
    format_ptbr_int, format_ptbr_money
)

load_dotenv()
app = Flask(__name__)

_DATA_CACHE = { "df": pd.DataFrame(), "loaded_at": None }
CACHE_TTL_SECONDS = 300

def get_data():
    now = datetime.utcnow()
    needs_reload = (
        _DATA_CACHE["loaded_at"] is None or
        (now - _DATA_CACHE["loaded_at"]).total_seconds() > CACHE_TTL_SECONDS
    )
    if needs_reload:
        _DATA_CACHE["df"] = load_dataframe()
        _DATA_CACHE["loaded_at"] = now
        print(f"[INFO] Dados carregados às {_DATA_CACHE['loaded_at']} (UTC). Linhas: {_DATA_CACHE['df'].shape[0]}")
    return _DATA_CACHE["df"]

@app.context_processor
def inject_globals():
    current_path = request.path
    return dict(current_path=current_path, format_ptbr_int=format_ptbr_int, format_ptbr_money=format_ptbr_money)

@app.route("/")
def index():
    df = get_data()
    return render_template("index.html", total_regs=len(df), colunas=list(df.columns))

# 1) Visão Geral
@app.route("/visao-geral")
def visao_geral():
    df = get_data()
    total_linhas = len(df)
    total_vendas = df["vendas"].sum() if "vendas" in df.columns else None
    total_valor = df["valor"].sum() if "valor" in df.columns else None
    por_estado = group_count(df, ["estado"]).sort_values("total", ascending=False).head(10)
    ticket_prof = group_avg(df, ["profissao"], "valor").sort_values("media", ascending=False).head(10)
    return render_template("visao_geral.html",
        total_linhas=total_linhas,
        total_vendas=total_vendas,
        total_valor=total_valor,
        por_estado=por_estado.to_dict(orient="records"),
        ticket_prof=ticket_prof.to_dict(orient="records"),
    )

# 2) Origem e Conversão
@app.route("/origem-conversao")
def origem_conversao():
    df = get_data()
    por_canal = group_count(df, ["canal"]).sort_values("total", ascending=False)
    taxa = []
    if set(["canal","leads","convertidos"]).issubset(df.columns):
        taxa_df = (df.groupby("canal")[["leads","convertidos"]].sum().reset_index())
        taxa_df["taxa_conv"] = (taxa_df["convertidos"] / taxa_df["leads"]).replace([float('inf')], 0).fillna(0)
        taxa = taxa_df.sort_values("taxa_conv", ascending=False).to_dict(orient="records")
    return render_template("origem_conversao.html",
        por_canal=por_canal.to_dict(orient="records"),
        taxa=taxa
    )

# 3) Profissão por Canal
@app.route("/profissao-por-canal")
def profissao_por_canal():
    df = get_data()
    prof_canal = group_count(df, ["profissao","canal"]).sort_values("total", ascending=False).head(100)
    return render_template("profissao_por_canal.html",
        prof_canal=prof_canal.to_dict(orient="records")
    )

# 4) Análise Regional
@app.route("/analise-regional")
def analise_regional():
    df = get_data()
    por_regiao = group_count(df, ["regiao"])
    por_estado = group_count(df, ["estado"])
    return render_template("analise_regional.html",
        por_regiao=por_regiao.to_dict(orient="records"),
        por_estado=por_estado.to_dict(orient="records"),
    )

# 5) Insights de IA
@app.route("/insights-ia")
def insights_ia():
    df = get_data()
    insights = []
    if "profissao" in df.columns and "valor" in df.columns:
        top = (df.groupby("profissao")["valor"].mean().sort_values(ascending=False).head(5))
        for prof, media in top.items():
            insights.append(f"Profissão '{prof}' apresenta ticket médio acima da média ({format_ptbr_money(media)}).")
    if "estado" in df.columns:
        cont = df["estado"].value_counts().head(5)
        for uf, n in cont.items():
            insights.append(f"Concentração relevante de registros no estado {uf} ({format_ptbr_int(n)}).")
    if not insights:
        insights = ["Defina a planilha para habilitar insights mais robustos."]
    return render_template("insights_ia.html", insights=insights)

# 6) Projeção de Resultados
@app.route("/projecao-resultados")
def projecao_resultados():
    df = get_data()
    serie = []
    if "data" in df.columns and "valor" in df.columns:
        tmp = df.dropna(subset=["data"])
        if not tmp.empty:
            ms = (tmp
                  .assign(mes=tmp["data"].dt.to_period("M").dt.start_time)
                  .groupby("mes")["valor"].sum()
                  .reset_index()
                  .rename(columns={"valor": "total"}))
            serie = ms.to_dict(orient="records")
    return render_template("projecao_resultados.html", serie=serie)

# 7) Acompanhamento das Vendas
@app.route("/acompanhamento-vendas")
def acompanhamento_vendas():
    df = get_data()
    value_col = "valor" if "valor" in df.columns else ("vendas" if "vendas" in df.columns else None)
    total_vendas = df[value_col].sum() if (value_col and value_col in df.columns) else None
    por_profissao = group_sum(df, ["profissao"], value_col).sort_values("total", ascending=False).head(20) if value_col else pd.DataFrame()
    por_estado = group_sum(df, ["estado"], value_col).sort_values("total", ascending=False).head(20) if value_col else pd.DataFrame()
    return render_template("acompanhamento_vendas.html",
        value_col=value_col,
        total_vendas=total_vendas,
        por_profissao=por_profissao.to_dict(orient="records") if len(por_profissao) else [],
        por_estado=por_estado.to_dict(orient="records") if len(por_estado) else []
    )

# APIs auxiliares
@app.route("/api/vendas-profissao")
def api_vendas_profissao():
    df = get_data()
    value_col = "valor" if "valor" in df.columns else ("vendas" if "vendas" in df.columns else None)
    if not value_col:
        return jsonify([])
    tab = group_sum(df, ["profissao"], value_col).sort_values("total", ascending=False).head(20)
    return jsonify(tab.to_dict(orient="records"))

if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=debug)
