import os
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
import pandas as pd

from utils import (
    load_inputs_dashboard, group_count, group_sum, group_avg,
    format_ptbr_int, format_ptbr_money
)

# ------------------------------
# Boot
# ------------------------------
load_dotenv()

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static"
)
app.config["TEMPLATES_AUTO_RELOAD"] = True

# ------------------------------
# Cache simples em memória
# ------------------------------
_DATA_CACHE = {"blob": None, "loaded_at": None}
CACHE_TTL_SECONDS = 300  # 5 min

def get_blob():
    """
    Carrega a primeira aba da planilha.
    Retorna um dict contendo:
      - mode: 'long' | 'blocks'
      - se 'long': { 'df': DataFrame normalizado }
      - se 'blocks': chaves conforme ANCHORS (ex.: 'tbl_por_estado', 'serie_mensal' etc.)
    """
    now = datetime.utcnow()
    needs_reload = (
        _DATA_CACHE["loaded_at"] is None or
        (now - _DATA_CACHE["loaded_at"]).total_seconds() > CACHE_TTL_SECONDS
    )
    if needs_reload or _DATA_CACHE["blob"] is None:
        blob = load_inputs_dashboard(os.environ.get("DATA_XLSX_PATH"))
        _DATA_CACHE["blob"] = blob
        _DATA_CACHE["loaded_at"] = now
        print(f"[INFO] Dados (modo={blob.get('mode')}) carregados às {_DATA_CACHE['loaded_at']} (UTC).")
    return _DATA_CACHE["blob"]

# ------------------------------
# Helpers de contexto nos templates
# ------------------------------
@app.context_processor
def inject_globals():
    current_path = request.path
    blob = _DATA_CACHE["blob"]
    mode = blob.get("mode") if blob else None
    return dict(
        current_path=current_path,
        format_ptbr_int=format_ptbr_int,
        format_ptbr_money=format_ptbr_money,
        data_mode=mode
    )

# ------------------------------
# Rotas
# ------------------------------
@app.route("/")
def index():
    blob = get_blob()
    mode = blob["mode"]

    if mode == "long":
        df = blob["df"]
        total_regs = len(df)
        colunas = list(df.columns)
        return render_template("index.html",
                               total_regs=total_regs,
                               colunas=colunas)

    # mode == "blocks": mostre um “status” amigável
    available = sorted([k for k in blob.keys() if k != "mode"])
    return render_template("index.html",
                           total_regs=None,
                           colunas=available)

@app.route("/visao-geral")
def visao_geral():
    blob = get_blob()
    mode = blob["mode"]

    if mode == "long":
        df = blob["df"]
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

    # blocks
    total_linhas = None
    total_vendas = None
    total_valor = None
    por_estado = []
    ticket_prof = []

    if "kpi_total_linhas" in blob:
        try:
            total_linhas = pd.to_numeric(blob["kpi_total_linhas"].iloc[0, 0], errors="coerce")
        except Exception:
            pass
    if "kpi_total_vendas" in blob:
        try:
            total_vendas = pd.to_numeric(blob["kpi_total_vendas"].iloc[0, 0], errors="coerce")
        except Exception:
            pass
    if "kpi_total_valor" in blob:
        try:
            total_valor = pd.to_numeric(blob["kpi_total_valor"].iloc[0, 0], errors="coerce")
        except Exception:
            pass
    if "tbl_por_estado" in blob:
        por_estado = blob["tbl_por_estado"].to_dict(orient="records")
    if "tbl_ticket_prof" in blob:
        ticket_prof = blob["tbl_ticket_prof"].to_dict(orient="records")

    return render_template("visao_geral.html",
        total_linhas=total_linhas,
        total_vendas=total_vendas,
        total_valor=total_valor,
        por_estado=por_estado,
        ticket_prof=ticket_prof,
    )

@app.route("/origem-conversao")
def origem_conversao():
    blob = get_blob()
    mode = blob["mode"]

    if mode == "long":
        df = blob["df"]
        por_canal = group_count(df, ["canal"]).sort_values("total", ascending=False)
        taxa = []
        if set(["canal", "leads", "convertidos"]).issubset(df.columns):
            taxa_df = (df.groupby("canal")[["leads", "convertidos"]].sum().reset_index())
            taxa_df["taxa_conv"] = (taxa_df["convertidos"] / taxa_df["leads"]).replace([float("inf")], 0).fillna(0)
            taxa = taxa_df.sort_values("taxa_conv", ascending=False).to_dict(orient="records")
        return render_template("origem_conversao.html",
            por_canal=por_canal.to_dict(orient="records"),
            taxa=taxa
        )

    # blocks
    por_canal = blob.get("tbl_por_canal")
    taxa = blob.get("tbl_taxa_canal")
    return render_template("origem_conversao.html",
        por_canal=por_canal.to_dict(orient="records") if isinstance(por_canal, pd.DataFrame) else [],
        taxa=taxa.to_dict(orient="records") if isinstance(taxa, pd.DataFrame) else []
    )

@app.route("/profissao-por-canal")
def profissao_por_canal():
    blob = get_blob()
    mode = blob["mode"]

    if mode == "long":
        df = blob["df"]
        prof_canal = group_count(df, ["profissao", "canal"]).sort_values("total", ascending=False).head(100)
        return render_template("profissao_por_canal.html",
            prof_canal=prof_canal.to_dict(orient="records")
        )

    # blocks
    tab = blob.get("tbl_prof_canal")
    return render_template("profissao_por_canal.html",
        prof_canal=tab.to_dict(orient="records") if isinstance(tab, pd.DataFrame) else []
    )

@app.route("/analise-regional")
def analise_regional():
    blob = get_blob()
    mode = blob["mode"]

    if mode == "long":
        df = blob["df"]
        por_regiao = group_count(df, ["regiao"])
        por_estado = group_count(df, ["estado"])
        return render_template("analise_regional.html",
            por_regiao=por_regiao.to_dict(orient="records"),
            por_estado=por_estado.to_dict(orient="records"),
        )

    # blocks
    br = blob.get("tbl_por_regiao")
    be = blob.get("tbl_por_estado")
    return render_template("analise_regional.html",
        por_regiao=br.to_dict(orient="records") if isinstance(br, pd.DataFrame) else [],
        por_estado=be.to_dict(orient="records") if isinstance(be, pd.DataFrame) else [],
    )

@app.route("/insights-ia")
def insights_ia():
    blob = get_blob()
    mode = blob["mode"]

    insights = []
    if mode == "long":
        df = blob["df"]
        if "profissao" in df.columns and "valor" in df.columns:
            top = (df.groupby("profissao")["valor"].mean().sort_values(ascending=False).head(5))
            for prof, media in top.items():
                insights.append(f"Profissão '{prof}' apresenta ticket médio acima da média ({format_ptbr_money(media)}).")
        if "estado" in df.columns:
            cont = df["estado"].value_counts().head(5)
            for uf, n in cont.items():
                insights.append(f"Concentração relevante de registros no estado {uf} ({format_ptbr_int(n)}).")
    else:
        # modo blocks: sem DF “linha a linha”, gere insights simples a partir dos blocos se existirem
        if "tbl_ticket_prof" in blob:
            try:
                top = blob["tbl_ticket_prof"].copy()
                # espera colunas algo como: profissao, media (ou valor)
                col_media = "media" if "media" in top.columns else ("valor" if "valor" in top.columns else None)
                if col_media and "profissao" in top.columns:
                    top = top.sort_values(col_media, ascending=False).head(5)
                    for _, row in top.iterrows():
                        insights.append(f"Profissão '{row['profissao']}' com ticket médio elevado ({format_ptbr_money(row[col_media])}).")
            except Exception:
                pass
        if "tbl_por_estado" in blob:
            try:
                est = blob["tbl_por_estado"].copy()
                # espera colunas: estado, total
                col_total = "total" if "total" in est.columns else None
                if col_total and "estado" in est.columns:
                    est = est.sort_values(col_total, ascending=False).head(5)
                    for _, row in est.iterrows():
                        insights.append(f"Concentração no estado {row['estado']} ({format_ptbr_int(row[col_total])}).")
            except Exception:
                pass

    if not insights:
        insights = ["Estruture a primeira aba (Inputs) com as âncoras/tabelas ou dados linha-a-linha para habilitar insights."]

    return render_template("insights_ia.html", insights=insights)

@app.route("/projecao-resultados")
def projecao_resultados():
    blob = get_blob()
    mode = blob["mode"]

    serie = []
    if mode == "long":
        df = blob["df"]
        if "data" in df.columns and "valor" in df.columns:
            tmp = df.dropna(subset=["data"])
            if not tmp.empty:
                ms = (tmp
                      .assign(mes=tmp["data"].dt.to_period("M").dt.start_time)
                      .groupby("mes")["valor"].sum()
                      .reset_index()
                      .rename(columns={"valor": "total"}))
                serie = ms.to_dict(orient="records")
    else:
        if "serie_mensal" in blob:
            serie = blob["serie_mensal"].to_dict(orient="records")

    return render_template("projecao_resultados.html", serie=serie)

@app.route("/acompanhamento-vendas")
def acompanhamento_vendas():
    blob = get_blob()
    mode = blob["mode"]

    if mode == "long":
        df = blob["df"]
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

    # blocks: se não houver blocos específicos, só informa indisponibilidade
    return render_template("acompanhamento_vendas.html",
        value_col=None,
        total_vendas=None,
        por_profissao=[],
        por_estado=[]
    )

# ------------------------------
# API
# ------------------------------
@app.route("/api/vendas-profissao")
def api_vendas_profissao():
    blob = get_blob()
    if blob["mode"] != "long":
        return jsonify([])

    df = blob["df"]
    value_col = "valor" if "valor" in df.columns else ("vendas" if "vendas" in df.columns else None)
    if not value_col:
        return jsonify([])

    tab = group_sum(df, ["profissao"], value_col).sort_values("total", ascending=False).head(20)
    return jsonify(tab.to_dict(orient="records"))

# ------------------------------
# Run local
# ------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)
