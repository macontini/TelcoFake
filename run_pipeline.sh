#!/bin/bash

# Ferma lo script se un comando fallisce
set -e

# Venv (Git Bash su Windows)
readonly PY_VENV="/c/Users/macontini/venvs/py314/Scripts/activate"

# Colori per output
readonly GREEN='\033[0;32m'
readonly RED='\033[0;31m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly RESET='\033[0m'

# Percorsi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly DATA_DIR="${SCRIPT_DIR}/data"
readonly REPORTS_DIR="${SCRIPT_DIR}/reports"
readonly CHARTS_DIR="${SCRIPT_DIR}/charts"
# readonly LOGS_DIR="${SCRIPT_DIR}/logs"
readonly PIPELINE_DIR="${SCRIPT_DIR}/pipeline"

# Controlla se '--test' è presente tra gli argomenti
TEST_FLAG=""
for arg in "$@"; do
    if [ "$arg" == "--test" ]; then
        TEST_FLAG="--test"
        break
    fi
done

if [ "$TEST_FLAG" == "--test" ]; then
    echo -e "${BLUE}Modalità test attivata, verrà prodotto solo il 10% dell'intero dataset previsto dalla configurazione YAML.${RESET}"
else
    echo -e "${BLUE}Modalità test disattivata, verrà prodotto l'intero dataset previsto dalla configurazione YAML.${RESET}"
fi

echo -e "${YELLOW}>>> Avvio Pipeline Dati Ferroviari...${RESET}"

# Contatore interno
SECONDS=0

# Pulizia file vecchi
rm -f "$DATA_DIR"/*.csv "$DATA_DIR"/*.parquet
# rm -rf "$LOG_DIR"
rm -rf "$REPORTS_DIR"
rm -rf "$CHARTS_DIR"

# Ambiente Virtuale
if [ -f "$PY_VENV" ]; then
    # shellcheck source=/dev/null
    source "$PY_VENV"
    echo -e "Ambiente attivato: ${YELLOW}$(which python)${RESET}"
else
    echo -e "${RED}ERRORE: Ambiente virtuale non trovato in $PY_VENV${RESET}"
    exit 1
fi

# Controllo Librerie
if ! python -c "import pandas, yaml, faker, numpy, pyarrow, fastparquet, matplotlib, seaborn" &> /dev/null; then
    echo -e "${RED}ERRORE: Librerie mancanti nel venv.${RESET}"
    exit 1
fi

# Questa funzione prende due parametri:
# il nome dello script e il messaggio di successo
run_stage() {
    local script_name=$1
    local success_msg=$2
    local extra_args=$3

    echo -e "\n${YELLOW}>>> Lancio: ${script_name}...${RESET}"

    # shellcheck disable=SC2086
    if python "$PIPELINE_DIR/$script_name" ${extra_args:-}; then
        echo -e "${GREEN}✔ Successo! ${success_msg}${RESET}"
    else
        echo -e "${RED}✘ ERRORE critico in '${script_name}'. Pipeline interrotta.${RESET}"
        exit 1
    fi
}

# Esecuzione
run_stage random_data_gen.py    "Dati generati in $DATA_DIR."   $TEST_FLAG
# Database
run_stage database.py           "Database popolato correttamente."
# Analytics (SQLite)
run_stage analytics.py          "Report analitici creati via SQLite in $REPORTS_DIR."
# Analytics (Pandas)
run_stage analytics_pandas.py   "Report analitici creati via Pandas in $REPORTS_DIR."
# Visualization
run_stage visualization.py      "Grafici salvati in $CHARTS_DIR."
# Pre-processing
run_stage preprocessing_bi.py   "Dati pronti per l'importazione in Power BI."

MINUTES=$((SECONDS / 60))
REMAINING_SECONDS=$((SECONDS % 60))
echo -e "${BLUE}Tempo di esecuzione totale: ${MINUTES}m ${REMAINING_SECONDS}s${RESET}"

echo -e "${GREEN}=======================================${RESET}"
echo -e "${GREEN}        !PIPELINE COMPLETATA!          ${RESET}"
echo -e "${GREEN}=======================================${RESET}"
