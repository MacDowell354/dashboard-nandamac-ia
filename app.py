import os
from datetime import datetime, timezone
from flask import Flask, render_template, request
from utils import load_inputs_dashboard  # usa seu utils.py

# ------------------------------------------------------------------------------
# App & Estado Global simples
# ------------------------------------------------------------------------------

app = Flask(__name__)

DATA_BLOB = None       # dict com {'mode': 'blocks'|'long', ...}
DATA_MODE = None       # 'blocks' ou 'long'
LAST_LOADED_UTC = None # datetime
DATA_SRC = os.environ.get("DATA_XLSX_PATH") or os.environ.get("GOOGLE_SHEET_CSV_URL")

def _load_data():
    """Carrega a primeira aba (conforme utils.load_inputs_dashboard) e
    guarda um carimbo de data/hora para exibir no rodapé."""
    global DATA_BLOB, DATA_MODE, LAST_LOADED_UTC
    DATA_BLOB = load_inputs_dashboard(DATA_SRC)
    DATA_MODE = DATA_BLOB.get("mode")
    LAST_LOADED_UTC = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    app.logger.info(f"Dados (modo={DATA_MODE}) carregados às {LAST_LOADED_UTC}.")

# Carrega ao subir
_load_data()

# ------------------------------------------------------------------------------
# Contexto comum para templates (menu ativo, horário, modo dos dados, etc.)
# ------------------------------------------------------------------------------

@app.context_processor
def inject_globals():
    return {
        "current_path": request.path,
        "last_loaded": LAST_LOADED_UTC,
        "data_mode": DATA_MODE,
    }

# ------------------------------------------------------------------------------
# Rotas
# ------------------------------------------------------------------------------

@app.route("/")
def index():
    # Exibe um diagnóstico rápido do que o loader encontrou
    blocks_info = []
    if DATA_MODE == "blocks":
        # Mostra as chaves de blocos disponíveis
        for k in sorted([x for x in DATA_BLOB.keys() if x != "mode"]):
            blocks_info.append(k)
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

@app.route("/insights-ia")
def insights_ia():
    return render_template("insights_ia.html")

@app.route("/projecao-resultados")
def projecao_resultados():
    return render_template("projecao_resultados.html")

# NOVA ROTA para corresponder ao link do menu
@app.route("/acompanhamento-vendas")
def acompanhamento_vendas():
    return render_template("acompanhamento_vendas.html")

# Opcional: endpoint para recarregar dados manualmente (se quiser)
@app.route("/reload")
def reload_data():
    _load_data()
    return "OK - dados recarregados."


# ------------------------------------------------------------------------------
# Entry point (Render usa gunicorn, mas isso ajuda em dev local)
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # Para rodar local (python app.py)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=bool(int(os.environ.get("FLASK_DEBUG", "0"))))
