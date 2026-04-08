#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AIRFLOW_HOME="$SCRIPT_DIR/airflow"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8010
