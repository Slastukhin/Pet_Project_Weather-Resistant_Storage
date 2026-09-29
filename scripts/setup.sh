#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv venv
venv/bin/python -m pip install -r requirements-dev.txt
if [ ! -f .env ]; then
    cp .env.example .env
    chmod 600 .env
fi
printf '\nActivate the environment: source venv/bin/activate\nStart the project: docker compose up\n'
