#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Game Data Platform Bootstrap Script
# ---------------------------------------------------------------------------
# This script:
#   1) Asks for Snowflake credentials
#   2) Writes them into app/.env
#   3) Creates/uses a Python virtualenv in app/.venv
#   4) Installs Python dependencies
#   5) Generates synthetic CSV data
#   6) Loads data into Snowflake RAW_* tables
#
# Usage (from game-data-platform/):
#   chmod +x run_platform.sh   # one time
#   ./run_platform.sh
# ---------------------------------------------------------------------------

# ----- Pretty output helpers ------------------------------------------------

BOLD="\033[1m"
CYAN="\033[36m"
GREEN="\033[32m"
RED="\033[31m"
RESET="\033[0m"

print_banner() {
  echo -e "${CYAN}${BOLD}"
  echo "╔═══════════════════════════════════════════════╗"
  echo "║             Game Data Platform                ║"
  echo "║       Snowflake Loader & Generator            ║"
  echo "╚═══════════════════════════════════════════════╝"
  echo -e "${RESET}"
}

print_step() {
  echo -e "${BOLD}▶ $1${RESET}"
}

print_ok() {
  echo -e "${GREEN}✔${RESET} $1"
}

print_error() {
  echo -e "${RED}✖ $1${RESET}"
}

prompt_with_default() {
  local prompt_text="$1"
  local default_value="$2"
  local var_name="$3"

  if [ -n "$default_value" ]; then
    read -r -p "$prompt_text [$default_value]: " input || true
    if [ -z "$input" ]; then
      input="$default_value"
    fi
  else
    read -r -p "$prompt_text: " input || true
  fi

  eval "$var_name=\"\$input\""
}

# ----- Main -----------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR/app"
ENV_FILE="$APP_DIR/.env"
ROOT_ENV_FILE="$SCRIPT_DIR/.env"
VENV_DIR="$APP_DIR/.venv"

print_banner

echo -e "${BOLD}This script will:${RESET}"
echo "  1) Ask for your Snowflake credentials"
echo "  2) Write them into app/.env"
echo "  3) Create/activate a Python virtualenv in app/.venv"
echo "  4) Install Python dependencies"
echo "  5) Generate synthetic game data (full load or incremental batch)"
echo "  6) Load data into Snowflake (RAW_* tables)"
echo
echo -e "${BOLD}Need help before you continue? Do the instructions in this order:${RESET}"
echo "  1) Snowflake account setup (first):"
echo "      https://github.com/makararena/game-data-platform/blob/main/instructions/snowflake-account-setup.md"
echo "  2) Pre-launch setup (second — database & schemas):"
echo "      https://github.com/makararena/game-data-platform/blob/main/instructions/pre-launch-setup.md"
echo "  3) Snowflake credentials (third — if you don't have them or can't find them):"
echo "      https://github.com/makararena/game-data-platform/blob/main/instructions/snowflake-credentials.md"
echo

read -r -p "Continue? [Y/n]: " CONTINUE || true
CONTINUE="${CONTINUE:-Y}"
if [[ ! "$CONTINUE" =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 0
fi

# ----- Collect Snowflake credentials ---------------------------------------

echo
print_step "Snowflake configuration"

# Load existing defaults from root .env (if present)
if [ -f "${ROOT_ENV_FILE}" ]; then
  # shellcheck source=/dev/null
  source "${ROOT_ENV_FILE}"
fi

# Load existing defaults from app/.env (if present)
if [ -f "${ENV_FILE}" ]; then
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
fi

SKIP_CREDENTIAL_PROMPTS="false"

# If we already have a full set of credentials, offer to reuse them and skip prompts
if [ -f "${ENV_FILE}" ] && \
   [ -n "${SNOWFLAKE_ACCOUNT:-}" ] && \
   [ -n "${SNOWFLAKE_USER:-}" ] && \
   [ -n "${SNOWFLAKE_PASSWORD:-}" ]; then
  echo
  echo "Found existing Snowflake credentials in app/.env:"
  echo "  Account:   ${SNOWFLAKE_ACCOUNT}"
  echo "  User:      ${SNOWFLAKE_USER}"
  echo "  Warehouse: ${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
  echo "  Database:  ${SNOWFLAKE_DATABASE:-GAME_ANALYTICS}"
  echo "  Schema:    ${SNOWFLAKE_SCHEMA:-RAW}"
  read -r -p "Reuse these values and skip credential prompts? [Y/n]: " REUSE_EXISTING || true
  REUSE_EXISTING="${REUSE_EXISTING:-Y}"
  if [[ "$REUSE_EXISTING" =~ ^[Yy]$ ]]; then
    SKIP_CREDENTIAL_PROMPTS="true"
  fi
fi

if [ "${SKIP_CREDENTIAL_PROMPTS}" != "true" ]; then
  prompt_with_default "Snowflake account (e.g. cwrlboz-pz37526)" "${SNOWFLAKE_ACCOUNT:-}" SNOWFLAKE_ACCOUNT
  prompt_with_default "Snowflake user" "${SNOWFLAKE_USER:-}" SNOWFLAKE_USER

  # password: allow reuse if present, otherwise prompt (hidden input)
  if [ -n "${SNOWFLAKE_PASSWORD:-}" ]; then
    read -r -p "A Snowflake password is already stored in app/.env. Reuse it? [Y/n]: " REUSE_PW || true
    REUSE_PW="${REUSE_PW:-Y}"
    if [[ ! "$REUSE_PW" =~ ^[Yy]$ ]]; then
      read -s -p "Snowflake password (input hidden): " SNOWFLAKE_PASSWORD || true
      echo
    fi
  else
    read -s -p "Snowflake password (input hidden): " SNOWFLAKE_PASSWORD || true
    echo
  fi

  prompt_with_default "Snowflake warehouse" "${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}" SNOWFLAKE_WAREHOUSE
  prompt_with_default "Snowflake database" "${SNOWFLAKE_DATABASE:-GAME_ANALYTICS}" SNOWFLAKE_DATABASE
  prompt_with_default "Snowflake schema for RAW tables" "${SNOWFLAKE_SCHEMA:-RAW}" SNOWFLAKE_SCHEMA
fi

if [ -z "${SNOWFLAKE_ACCOUNT}" ] || [ -z "${SNOWFLAKE_USER}" ] || [ -z "${SNOWFLAKE_PASSWORD}" ]; then
  print_error "Account, user and password are required."
  exit 1
fi

# ----- Write .env for the app ----------------------------------------------

print_step "Writing Snowflake credentials to app/.env"

{
  echo "SNOWFLAKE_USER=${SNOWFLAKE_USER}"
  echo "SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}"
  echo "SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}"
  echo "SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE}"
  echo "SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE}"
  echo "SNOWFLAKE_SCHEMA=${SNOWFLAKE_SCHEMA}"
} > "${ENV_FILE}"

print_ok "Saved Snowflake config to $(basename "${ENV_FILE}")"

# ----- Python virtualenv + dependencies ------------------------------------

print_step "Setting up Python virtual environment in app/.venv"

if [ ! -d "${VENV_DIR}" ]; then
  (cd "${APP_DIR}" && python -m venv .venv)
  print_ok "Created virtualenv in app/.venv"
else
  print_ok "Virtualenv already exists, reusing app/.venv"
fi

# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"

print_step "Installing Python dependencies"
pip install --upgrade pip >/dev/null
pip install -r "${APP_DIR}/requirements.txt" >/dev/null
pip install "snowflake-connector-python[pandas]" >/dev/null
print_ok "Python dependencies installed"

# ----- Run mode: full load vs incremental batch -----------------------------

echo
print_step "Run mode"
echo "  [1] Full load (default) - create/replace tables, load seed data (batch 1, seed 42)"
echo "  [2] Incremental batch - generate NEW users/sessions/events and APPEND only"
echo "      Use this to test dbt incremental models (e.g. Task 8)."
read -r -p "Choose [1/2] (default: 1): " RUN_MODE || true
RUN_MODE="${RUN_MODE:-1}"

# ----- Generate data -------------------------------------------------------

if [[ "$RUN_MODE" == "2" ]]; then
  # Auto-detect next batch and date range from Snowflake (max date + 1, max batch + 1)
  print_step "Incremental batch: detecting next batch and date range from Snowflake"
  INC_BATCH=2
  INC_START="2011-02-13"
  INC_END="2011-03-15"
  if eval "$(cd "${APP_DIR}" && python ingest/get_next_incremental.py 2>/dev/null)"; then
    :
  fi
  echo "  Batch ${INC_BATCH:-2}, dates ${INC_START:-2011-02-13}..${INC_END:-2011-03-15}"
  (
    cd "${APP_DIR}"
    python main.py --batch "${INC_BATCH:-2}" --start "${INC_START:-2011-02-13}" --end "${INC_END:-2011-03-15}" --no-ingest
  )
  print_ok "Incremental batch ${INC_BATCH:-2} generated (app/data/)"
  print_step "Appending incremental batch into Snowflake RAW tables"
  (
    cd "${APP_DIR}"
    python ingest/load_to_snowflake.py --mode append
  )
  print_ok "Incremental batch appended to Snowflake"
else
  # Full load: generate seed data, create/replace tables, load
  print_step "Generating synthetic game data (players, sessions, events)"
  (
    cd "${APP_DIR}"
    python main.py --no-ingest
  )
  print_ok "Data generated (CSV files in app/data/)"
  print_step "Loading data into Snowflake RAW tables (create/replace)"
  (
    cd "${APP_DIR}"
    python ingest/load_to_snowflake.py --mode recreate
  )
  print_ok "Data loaded into Snowflake"
fi

# ----- Final message -------------------------------------------------------

echo
echo -e "${GREEN}${BOLD}All done!${RESET}"
echo
if [[ "$RUN_MODE" == "1" ]]; then
  echo -e "Your raw tables are now available in Snowflake as:"
  echo -e "  ${BOLD}${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.RAW_PLAYERS${RESET}"
  echo -e "  ${BOLD}${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.RAW_SESSIONS${RESET}"
  echo -e "  ${BOLD}${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.RAW_GAME_EVENTS${RESET}"
  echo
  echo -e "${BOLD}Next steps for you (dbt project):${RESET}"
  echo "  1) Create your dbt project (see instructions/dbt-setup.md)."
  echo "  2) Start building sources, staging, and marts — see the course README:"
  echo "      https://github.com/makararena/game-data-platform/blob/main/README.md"
  echo
  echo -e "Good luck with the course and happy modeling!"
fi

