from flask import render_template, request

# (opcional) deixa disponível 'current_path' se algum template antigo ainda usar
@app.context_processor
def inject_current_path():
    try:
        return {"current_path": request.path or ""}
    except Exception:
        return {"current_path": ""}

# helpers para passar infos no rodapé sem quebrar se não existirem
def _ui_globals():
    last_loaded = None
    data_mode = None
    try:
        # se você já calcula isso em outro lugar, pode reaproveitar
        last_loaded = globals().get("LAST_LOADED") or globals().get("DATA_LOADED_AT")
        data_mode = globals().get("DATA_MODE")
    except Exception:
        pass
    return {"last_loaded": last_loaded, "data_mode": data_mode}

# --- ROTAS MÍNIMAS (adicione as que faltarem no seu app) ---

@app.get("/")
def index():
    # se você já tem index(), mantenha o seu; o importante é passar _ui_globals()
    return render_template("index.html", **_ui_globals())

@app.get("/visao-geral")
def visao_geral():
    return render_template("visao_geral.html", **_ui_globals())

@app.get("/origem-conversao")
def origem_conversao():
    return render_template("origem_conversao.html", **_ui_globals())

@app.get("/profissao-por-canal")
def profissao_por_canal():
    return render_template("profissao_por_canal.html", **_ui_globals())

@app.get("/analise-regional")
def analise_regional():
    return render_template("analise_regional.html", **_ui_globals())

@app.get("/insights-ia")
def insights_ia():
    # ⭐ rota que faltava e está quebrando o menu
    return render_template("insights_ia.html", **_ui_globals())

@app.get("/projecao-resultados")
def projecao_resultados():
    return render_template("projecao_resultados.html", **_ui_globals())

@app.get("/acompanhamento-vendas")
def acompanhamento_vendas():
    return render_template("acompanhamento_vendas.html", **_ui_globals())
