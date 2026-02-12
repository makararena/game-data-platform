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
echo "  5) Generate synthetic game data"
echo "  6) Load data into Snowflake (RAW_* tables)"
echo

echo -e "${BOLD}Need help before you continue?${RESET}"
echo "  - instructions/snowflake-account-setup.md   (how to create a Snowflake account)"
echo "  - instructions/snowflake-credentials.md     (which credentials you need & where to put them)"
echo "  - instructions/pre-launch-setup.md          (which database/schema to create before running)"
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

prompt_with_default "Snowflake account (e.g. xy12345.eu-central-1)" "${SNOWFLAKE_ACCOUNT:-}" SNOWFLAKE_ACCOUNT
prompt_with_default "Snowflake user" "${SNOWFLAKE_USER:-}" SNOWFLAKE_USER

# password: hidden input
read -s -p "Snowflake password (input hidden): " SNOWFLAKE_PASSWORD || true
echo
prompt_with_default "Snowflake warehouse" "${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}" SNOWFLAKE_WAREHOUSE
prompt_with_default "Snowflake database" "${SNOWFLAKE_DATABASE:-GAME_ANALYTICS}" SNOWFLAKE_DATABASE
prompt_with_default "Snowflake schema for RAW tables" "${SNOWFLAKE_SCHEMA:-RAW}" SNOWFLAKE_SCHEMA

if [ -z "${SNOWFLAKE_ACCOUNT}" ] || [ -z "${SNOWFLAKE_USER}" ] || [ -z "${SNOWFLAKE_PASSWORD}" ]; then
  print_error "Account, user and password are required."
  exit 1
fi

# ----- Write .env for the app ----------------------------------------------

print_step "Writing Snowflake credentials to app/.env"

cat > "${ENV_FILE}" <<EOF
SNOWFLAKE_USER=${SNOWFLAKE_USER}
SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}
SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE}
SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE}
SNOWFLAKE_SCHEMA=${SNOWFLAKE_SCHEMA}
EOF

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

# ----- Generate data -------------------------------------------------------

print_step "Generating synthetic game data (players, sessions, events)"
(
  cd "${APP_DIR}"
  python main.py
)
print_ok "Data generated (CSV files in app/data/)"

# ----- Load into Snowflake -------------------------------------------------

print_step "Loading data into Snowflake RAW tables"
(
  cd "${APP_DIR}"
  python ingest/load_to_snowflake.py
)
print_ok "Data loaded into Snowflake"

# ----- Final message -------------------------------------------------------

echo
echo -e "${GREEN}${BOLD}All done!${RESET}"
echo
echo -e "Your raw tables are now available in Snowflake as:"
echo -e "  ${BOLD}${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.RAW_PLAYERS${RESET}"
echo -e "  ${BOLD}${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.RAW_SESSIONS${RESET}"
echo -e "  ${BOLD}${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.RAW_GAME_EVENTS${RESET}"
echo
echo -e "${BOLD}Next steps for you (dbt project):${RESET}"
echo "  1) Go to the sibling dbt project directory (e.g. game-dbt-project/)."
echo "  2) Configure your dbt profile to point at:"
echo "       database = ${SNOWFLAKE_DATABASE}"
echo "       schema   = (your dev schema, e.g. DEV)"
echo "  3) Start building sources, staging, and marts following the course README."
echo
echo -e "Good luck with the course and happy modeling!"

