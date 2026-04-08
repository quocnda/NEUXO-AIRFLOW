#!/usr/bin/env bash

set -e
# ===== CONFIG =====
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AIRFLOW_HOME="$SCRIPT_DIR/airflow"
export PROJECT_DIR="$SCRIPT_DIR"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8010

# (optional) executor nếu bạn muốn set rõ
# export AIRFLOW__CORE__EXECUTOR=LocalExecutor

echo "====================================="
echo " Starting Apache Airflow"
echo " AIRFLOW_HOME: $AIRFLOW_HOME"
echo " Webserver Port: 8010"
echo "====================================="

# Run airflow standalone via uv
uv run airflow standalone