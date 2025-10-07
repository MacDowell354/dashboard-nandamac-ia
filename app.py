import os
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
import pandas as pd

from utils import (
    load_inputs_dashboard,
    group_count, group_sum, group_avg,
    format_ptbr_int, format_ptbr_money
)

load_dotenv()

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static"
)
app.config["TEMPLATES_AUTO_RELOAD"] = True

_DATA_CACHE = {"blob": None, "loaded_at": None}
CACHE_TTL_SECONDS = 300

def get_blob():
    """Carrega a 1ª aba da planilha (tabela longa ou blocos)."""
    now = datetime.utcnow()
    needs_reload = (
        _DATA_CACHE["loaded_at"] is None or
        (now - _DATA_CACHE["loaded_at"]).total_seconds() > CACHE_TTL_SECONDS
    )
    if needs_reload:
        src = os.environ.get("DATA_XLSX_PATH")
        blob = load_inputs_dashboard(src)
        _DATA_CACHE["blob"] = blob
        _DATA_CACHE["loaded_at"] = now
        print(f"[INFO] Dados (modo={blob['mode']}) carregados às {now} (UTC).")
    return _DATA_CACHE["blob"]

@app.context_processor
def inject_globals():
    return dict(
        current_path=request.path,
        format_ptbr_int=format_ptbr_int,
        format_ptbr_money=format_ptbr_money
    )

@app.route("/")
def index():
    blob = get_blob()
    # só para mostrar colunas quando long
    colunas = list(blob["df"].columns) if blob["mode"] == "long" else []
    total = len(blob["df"]) if blob["mode"] == "long" else None
    return render_template("index.html", total_regs=total, colunas=colunas)

# -------------------- VISÃO GERAL --------------------
@app.route("/visao-geral")
def visao_geral():
    blob = get_blob()

    if blob["mode"] == "long":
        df = blob["df"]
        total_linhas = len(df)
        value_col = "valor" if "valor" in df.columns else ("vendas" if "vendas" in df.columns else None)
        total_vendas = df[value_col].sum() if value_col else None
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

    # blocks
    total_linhas = None
    if isinstance(blob.get("kpi_total_linhas"), pd.DataFrame) and not blob["kpi_total_linhas"].empty:
        total_linhas = pd.to_numeric(blob["kpi_total_linhas"].iloc[0,0], errors="coerce")

    total_vendas = None
    if isinstance(blob.get("kpi_total_vendas"), pd.DataFrame) and not blob["kpi_total_vendas"].empty:
        total_vendas = pd.to_numeric(blob["kpi_total_vendas"].iloc[0,0], errors="coerce")

    total_valor = None
    if isinstance(blob.get("kpi_total_valor"), pd.DataFrame) and not blob["kpi_total_valor"].empty:
        total_valor = pd.to_numeric(blob["kpi_total_valor"].iloc[0,0], errors="coerce")

    por_estado = blob.get("tbl_por_estado", pd.DataFrame(columns=["estado","total"])).to_dict(orient="records")
    ticket_prof = blob.get("tbl_ticket_prof", pd.DataFrame(columns=["profissao","media"])).to_dict(orient="records")

    return render_template("visao_geral.html",
        total_linhas=int(total_linhas) if pd.notna(total_linhas) else None,
        total_vendas=float(total_vendas) if pd.notna(total_vendas) else None,
        total_valor=float(total_valor) if pd.notna(total_valor) else None,
        por_estado=por_estado,
        ticket_prof=ticket_prof,
    )

# -------------------- ORIGEM & CONVERSÃO --------------------
@app.route("/origem-conversao")
def origem_conversao():
    blob = get_blob()

    if blob["mode"] == "long":
        df = blob["df"]
        por_canal = group_count(df, ["canal"]).sort_values("total", ascending=False)
        taxa = []
        if set(["canal","leads","convertidos"]).issubset(df.columns):
            taxa_df = df.groupby("canal")[["leads","convertidos"]].sum().reset_index()
            taxa_df["taxa_conv"] = (taxa_df["convertidos"] / taxa_df["leads"]).replace([float("inf")], 0).fillna(0)
            taxa = taxa_df.sort_values("taxa_conv", ascending=False).to_dict(orient="records")
        return render_template("origem_conversao.html",
            por_canal=por_canal.to_dict(orient="records"),
            taxa=taxa
        )

    por_canal = blob.get("tbl_por_canal", pd.DataFrame(columns=["canal","total"])).to_dict(orient="records")
    taxa_df = blob.get("tbl_taxa_canal", pd.DataFrame(columns=["canal","leads","convertidos","taxa_conv"]))
    taxa = taxa_df.sort_values("taxa_conv", ascending=False).to_dict(orient="records") if not taxa_df.empty else []
    return render_template("origem_conversao.html", por_canal=por_canal, taxa=taxa)

# -------------------- PROFISSÃO x CANAL --------------------
@app.route("/profissao-por-canal")
def profissao_por_canal():
    blob = get_blob()
    if blob["mode"] == "long":
        df = blob["df"]
        prof_canal = group_count(df, ["profissao","canal"]).sort_values("total", ascending=False).head(100)
        data = prof_canal.to_dict(orient="records")
    else:
        tbl = blob.get("tbl_prof_canal", pd.DataFrame(columns=["profissao","canal","total"]))
        data = tbl.sort_values("total", ascending=False).head(100).to_dict(orient="records")
    return render_template("profissao_por_canal.html", prof_canal=data)

# -------------------- ANÁLISE REGIONAL --------------------
@app.route("/analise-regional")
def analise_regional():
    blob = get_blob()
    if blob["mode"] == "long":
        df = blob["df"]
        por_regiao = group_count(df, ["regiao"])
        por_estado = group_count(df, ["estado"])
        return render_template("analise_regional.html",
            por_regiao=por_regiao.to_dict(orient="records"),
            por_estado=por_estado.to_dict(orient="records"),
        )
    por_regiao = blob.get("tbl_por_regiao", pd.DataFrame(columns=["regiao","total"])).to_dict(orient="records")
    por_estado = blob.get("tbl_por_estado", pd.DataFrame(columns=["estado","total"])).to_dict(orient="records")
    return render_template("analise_regional.html", por_regiao=por_regiao, por_estado=por_estado)

# -------------------- INSIGHTS IA (simples) --------------------
@app.route("/insights-ia")
def insights_ia():
    blob = get_blob()
    insights = []

    if blob["mode"] == "long":
        df = blob["df"]
        if "profissao" in df.columns and "valor" in df.columns:
            top = df.groupby("profissao")["valor"].mean().sort_values(ascending=False).head(5)
            for prof, media in top.items():
                insights.append(f"Profissão '{prof}' apresenta ticket médio acima da média ({format_ptbr_money(media)}).")
        if "estado" in df.columns:
            cont = df["estado"].value_counts().head(5)
            for uf, n in cont.items():
                insights.append(f"Concentração relevante de registros no estado {uf} ({format_ptbr_int(n)}).")
    else:
        ticket = blob.get("tbl_ticket_prof", pd.DataFrame(columns=["profissao","media"]))
        for _, r in ticket.sort_values("media", ascending=False).head(5).iterrows():
            insights.append(f"Profissão '{r['profissao']}' apresenta ticket médio acima da média ({format_ptbr_money(r['media'])}).")
        estados = blob.get("tbl_por_estado", pd.DataFrame(columns=["estado","total"]))
        for _, r in estados.sort_values("total", ascending=False).head(5).iterrows():
            insights.append(f"Concentração relevante de registros no estado {r['estado']} ({format_ptbr_int(r['total'])}).")

    if not insights:
        insights = ["Defina a planilha para habilitar insights mais robustos."]
    return render_template("insights_ia.html", insights=insights)

# -------------------- PROJEÇÃO --------------------
@app.route("/projecao-resultados")
def projecao_resultados():
    blob = get_blob()

    if blob["mode"] == "long":
        df = blob["df"]
        serie = []
        if "data" in df.columns and "valor" in df.columns:
            tmp = df.dropna(subset=["data"])
            if not tmp.empty:
                ms = (tmp.assign(mes=tmp["data"].dt.to_period("M").dt.start_time)
                         .groupby("mes")["valor"].sum()
                         .reset_index()
                         .rename(columns={"valor": "total"}))
                serie = ms.to_dict(orient="records")
        return render_template("projecao_resultados.html", serie=serie)

    # blocks
    serie_df = blob.get("serie_mensal", pd.DataFrame(columns=["mes","total"]))
    serie = serie_df.sort_values("mes").to_dict(orient="records") if not serie_df.empty else []
    return render_template("projecao_resultados.html", serie=serie)

# -------------------- ACOMPANHAMENTO VENDAS --------------------
@app.route("/acompanhamento-vendas")
def acompanhamento_vendas():
    blob = get_blob()

    if blob["mode"] == "long":
        df = blob["df"]
        value_col = "valor" if "valor" in df.columns else ("vendas" if "vendas" in df.columns else None)
        total_vendas = df[value_col].sum() if value_col else None
        por_profissao = group_sum(df, ["profissao"], value_col).sort_values("total", ascending=False).head(20) if value_col else pd.DataFrame()
        por_estado = group_sum(df, ["estado"], value_col).sort_values("total", ascending=False).head(20) if value_col else pd.DataFrame()
        return render_template("acompanhamento_vendas.html",
            value_col=value_col,
            total_vendas=total_vendas,
            por_profissao=por_profissao.to_dict(orient="records") if len(por_profissao) else [],
            por_estado=por_estado.to_dict(orient="records") if len(por_estado) else []
        )

    # blocks: usa total_vendas se existir KPI e tabelas de soma se você as fornecer
    total_vendas = None
    if isinstance(blob.get("kpi_total_vendas"), pd.DataFrame) and not blob["kpi_total_vendas"].empty:
        total_vendas = pd.to_numeric(blob["kpi_total_vendas"].iloc[0,0], errors="coerce")

    por_profissao = blob.get("tbl_ticket_prof", pd.DataFrame(columns=["profissao","media"]))
    if not por_profissao.empty and "media" in por_profissao.columns and "qtd" not in por_profissao.columns:
        # se você só tem ticket médio, mostra como ranking de ticket
        por_profissao = por_profissao.rename(columns={"media":"total"}).sort_values("total", ascending=False).head(20)
    por_estado = blob.get("tbl_por_estado", pd.DataFrame(columns=["estado","total"])).sort_values("total", ascending=False).head(20)

    return render_template("acompanhamento_vendas.html",
        value_col="valor",
        total_vendas=float(total_vendas) if pd.notna(total_vendas) else None,
        por_profissao=por_profissao.to_dict(orient="records"),
        por_estado=por_estado.to_dict(orient="records")
    )

# -------------------- API --------------------
@app.route("/api/vendas-profissao")
def api_vendas_profissao():
    blob = get_blob()
    if blob["mode"] == "long":
        df = blob["df"]
        value_col = "valor" if "valor" in df.columns else ("vendas" if "vendas" in df.columns else None)
        if not value_col:
            return jsonify([])
        tab = group_sum(df, ["profissao"], value_col).sort_values("total", ascending=False).head(20)
        return jsonify(tab.to_dict(orient="records"))

    # blocks
    tbl = blob.get("tbl_ticket_prof", pd.DataFrame(columns=["profissao","media"]))
    if tbl.empty:
        return jsonify([])
    out = (tbl.rename(columns={"media":"total"})
              .sort_values("total", ascending=False)
              .head(20)
              .to_dict(orient="records"))
    return jsonify(out)

# -------------------- debug opcional --------------------
@app.route("/_debug/blob")
def _debug_blob():
    blob = get_blob()
    if blob["mode"] == "long":
        return jsonify({"mode":"long","columns":list(blob["df"].columns),"rows":len(blob["df"])})
    info = {k: {"cols": list(v.columns), "rows": len(v)} for k,v in blob.items() if k!="mode"}
    return jsonify({"mode":"blocks","blocks":info})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
