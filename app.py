import os
from datetime import datetime, timezone
from flask import Flask, render_template, request
from utils import load_inputs_dashboard  # seu utils.py

app = Flask(__name__)

DATA_BLOB = None        # dict com {'mode': 'blocks'|'long', ...}
DATA_MODE = None        # 'blocks' ou 'long'
LAST_LOADED_UTC = None  # string
DATA_SRC = os.environ.get("DATA_XLSX_PATH") or os.environ.get("GOOGLE_SHEET_CSV_URL")

def _load_data():
    global DATA_BLOB, DATA_MODE, LAST_LOADED_UTC
    DATA_BLOB = load_inputs_dashboard(DATA_SRC)
    DATA_MODE = DATA_BLOB.get("mode")
    LAST_LOADED_UTC = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    app.logger.info(f"Dados (modo={DATA_MODE}) carregados às {LAST_LOADED_UTC}.")

# Carrega ao iniciar
_load_data()

@app.context_processor
def inject_globals():
    return {
        "current_path": request.path,
        "last_loaded": LAST_LOADED_UTC,
        "data_mode": DATA_MODE,
    }

@app.route("/")
def index():
    blocks_info = []
    if DATA_MODE == "blocks":
      # Mostra as chaves de blocos disponíveis (diagnóstico)
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

# --- ROTA AJUSTADA: passa dados opcionais ao template, se existirem ---
@app.route("/acompanhamento-vendas")
def acompanhamento_vendas():
    """
    Tenta reaproveitar algum bloco como fonte de linhas de acompanhamento.
    Se nada existir, enviamos 'linhas=None' e o template mostra a msg padrão.
    """
    linhas = None
    if DATA_MODE == "blocks" and DATA_BLOB:
        # escolha preferencial de tabela (ajuste conforme seus blocos)
        preferidas = [
            "tbl_por_estado",
            "tbl_por_regiao",
            "tbl_por_canal",
            "tbl_ticket_prof",
            "tbl_taxa_canal",
        ]
        for key in preferidas:
            df = DATA_BLOB.get(key)
            if df is not None:
                try:
                    linhas = df.to_dict(orient="records")
                    break
                except Exception:
                    pass
    return render_template("acompanhamento_vendas.html", linhas=linhas)

@app.route("/reload")
def reload_data():
    _load_data()
    return "OK - dados recarregados."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=bool(int(os.environ.get("FLASK_DEBUG", "0"))))
