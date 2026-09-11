#!/usr/bin/env bash
# GrabTube — installer and launcher for macOS and Linux.
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# pretty output
# ─────────────────────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    R=$'\033[0m'; B=$'\033[1m'; D=$'\033[2m'
    RED=$'\033[38;5;131m'; GRN=$'\033[38;5;65m'
    YEL=$'\033[38;5;136m'; GRY=$'\033[38;5;245m'
else
    R=""; B=""; D=""; RED=""; GRN=""; YEL=""; GRY=""
fi

banner() {
    echo
    echo "  ${D}+-------------------------------------------------+${R}"
    echo "  ${D}|${R}                                                 ${D}|${R}"
    echo "  ${D}|${R}   ${B}Grab${R}${RED}&${R}${B}Tube${R}                                     ${D}|${R}"
    echo "  ${D}|${R}                                                 ${D}|${R}"
    echo "  ${D}|${R}   ${GRY}self-hosted . runs locally . no accounts${R}    ${D}|${R}"
    echo "  ${D}|${R}                                                 ${D}|${R}"
    echo "  ${D}+-------------------------------------------------+${R}"
    echo
}
step() { echo; echo "  ${RED}[$1]${R} $2"; }
ok()   { echo "      ${GRN}v${R} ${GRY}$1${R}"; }
warn() { echo "      ${YEL}!${R} ${GRY}$1${R}"; }
fail() { echo "      ${RED}x${R} ${GRY}$1${R}"; }
info() { echo "      ${D}.${R} ${D}$1${R}"; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

BIND_HOST="${GRABTUBE_HOST:-127.0.0.1}"
PORT="${GRABTUBE_PORT:-8000}"
SKIP_FFMPEG=0
NO_SETUP=0
SKIP_BROWSER=0
REINSTALL=0
EXTRA_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --port)         PORT="$2"; shift 2;;
        --host)         BIND_HOST="$2"; shift 2;;
        --skip-ffmpeg)  SKIP_FFMPEG=1; shift;;
        --skip-browser) SKIP_BROWSER=1; shift;;
        --no-setup)     NO_SETUP=1; shift;;
        --reinstall)    REINSTALL=1; shift;;
        -h|--help)
            cat <<EOF
Usage: ./install.sh [options]

  --port N           port to serve on (default 8000)
  --host ADDR        bind address (default 127.0.0.1)
  --skip-ffmpeg      don't try to install ffmpeg
  --skip-browser     don't auto-open the browser
  --no-setup         skip dependency setup, just launch
  --reinstall        force a clean setup
EOF
            exit 0;;
        *) EXTRA_ARGS+=("$1"); shift;;
    esac
done

banner

if [ ! -f "app.py" ]; then
    fail "app.py not found in $ROOT"
    exit 1
fi

VENV_DIR="$ROOT/.venv"
VENV_PY="$VENV_DIR/bin/python"
MARKER="$VENV_DIR/.installed"

if [ "$REINSTALL" -eq 1 ] && [ -f "$MARKER" ]; then
    info "--reinstall: clearing setup cache"
    rm -f "$MARKER"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 1. Python
# ─────────────────────────────────────────────────────────────────────────────
step 1 "Checking for Python 3.10+"

PY=""
for cand in python3.12 python3.11 python3.10 python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
        ver=$("$cand" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "")
        if [ -n "$ver" ]; then
            major="${ver%.*}"; minor="${ver#*.}"
            if [ "$major" = "3" ] && [ "$minor" -ge 10 ]; then
                PY="$cand"
                ok "found Python $ver ($cand)"
                break
            fi
        fi
    fi
done

if [ -z "$PY" ]; then
    fail "Python 3.10+ not found."
    echo
    echo "      Install it, then re-run this script:"
    if [ "$(uname)" = "Darwin" ]; then
        echo "        brew install python@3.12"
    elif command -v apt >/dev/null 2>&1; then
        echo "        sudo apt install python3 python3-venv python3-pip"
    elif command -v dnf >/dev/null 2>&1; then
        echo "        sudo dnf install python3 python3-pip"
    elif command -v pacman >/dev/null 2>&1; then
        echo "        sudo pacman -S python python-pip"
    else
        echo "        https://www.python.org/downloads/"
    fi
    exit 1
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2 + 3. venv + deps
# ─────────────────────────────────────────────────────────────────────────────
if [ "$NO_SETUP" -eq 1 ]; then
    step 2 "Skipping setup (--no-setup)"
elif [ -x "$VENV_PY" ] && [ -f "$MARKER" ]; then
    step 2 "Environment already set up"
    ok ".venv cached - launching directly"
    info "pass --reinstall to force a clean setup"
else
    step 2 "Setting up virtual environment"
    if [ -x "$VENV_PY" ]; then
        ok ".venv exists"
    else
        info "creating .venv"
        "$PY" -m venv "$VENV_DIR"
        if [ ! -x "$VENV_PY" ]; then
            fail "venv creation failed"
            info "On Debian/Ubuntu: sudo apt install python3-venv"
            exit 1
        fi
        ok ".venv created"
    fi

    step 3 "Installing dependencies"
    info "pip install -r requirements.txt"
    "$VENV_PY" -m pip install --upgrade pip --quiet
    "$VENV_PY" -m pip install -r requirements.txt --quiet
    ok "fastapi . uvicorn . yt-dlp . websockets . pydantic"

    touch "$MARKER"
    ok "setup cached - future launches will be instant"
fi

if [ ! -x "$VENV_PY" ]; then
    fail ".venv not found"
    exit 1
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. ffmpeg
# ─────────────────────────────────────────────────────────────────────────────
step 4 "Checking for ffmpeg"
if command -v ffmpeg >/dev/null 2>&1; then
    ok "ffmpeg already on PATH"
elif [ "$SKIP_FFMPEG" -eq 1 ]; then
    warn "skipping ffmpeg - merging and conversion will fail"
else
    warn "ffmpeg not found - merging, conversion and subtitles will fail"
    echo
    echo "      Install it with:"
    if [ "$(uname)" = "Darwin" ]; then
        echo "        brew install ffmpeg"
    elif command -v apt >/dev/null 2>&1; then
        echo "        sudo apt install ffmpeg"
    elif command -v dnf >/dev/null 2>&1; then
        echo "        sudo dnf install ffmpeg"
    elif command -v pacman >/dev/null 2>&1; then
        echo "        sudo pacman -S ffmpeg"
    fi
    echo
    printf "      Continue anyway? [y/N] "
    read -r ans </dev/tty || ans="n"
    if [ "$ans" != "y" ] && [ "$ans" != "Y" ]; then
        exit 1
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5. launch
# ─────────────────────────────────────────────────────────────────────────────
step 5 "Starting GrabTube"
echo
echo "      -> ${RED}http://${BIND_HOST}:${PORT}${R}"
echo
echo "      press ${B}Ctrl+C${R} to stop"
echo

open_browser() {
    sleep 2
    if command -v xdg-open >/dev/null 2>&1; then
        xdg-open "http://${BIND_HOST}:${PORT}" >/dev/null 2>&1 || true
    elif command -v open >/dev/null 2>&1; then
        open "http://${BIND_HOST}:${PORT}" >/dev/null 2>&1 || true
    fi
}
if [ "$SKIP_BROWSER" -eq 0 ]; then
    open_browser &
fi

exec "$VENV_PY" app.py --host "$BIND_HOST" --port "$PORT" "${EXTRA_ARGS[@]}"