#!/usr/bin/env bash
#
# Build local do eixo histórico (mcmv_historico_dbt) no target staging_duckdb,
# um modelo por vez, com o DuckDB usando disco com espaço e limite de RAM —
# evita o "no space left on device" / OOM do union_by_name das bronzes.
#
# Uso:
#   ./run-historico.sh                # tudo: seed + bronzes + silvers + golds + testes
#   ./run-historico.sh bronzes        # só as 3 bronzes
#   ./run-historico.sh silvers        # só as silvers por frente + consolidado
#   ./run-historico.sh <selector>     # dbt build --select <selector> --target staging_duckdb
#
# Overrides (env var):
#   DUCKDB_MCID_PATH          arquivo .duckdb            (default /mnt/data/duckdb/mcid_staging.duckdb)
#   DUCKDB_MCID_TEMP_DIR      dir de spill do DuckDB     (default /mnt/data/duckdb/tmp)
#   DUCKDB_MCID_MEMORY_LIMIT  limite de RAM do DuckDB    (default 10GB)
#   DUCKDB_MCID_THREADS       threads do DuckDB          (default 3)
#   DBT                       binário dbt                (default: dbt no PATH)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
TARGET=staging_duckdb

# dbt-core (o dbt-fusion do PATH não parseia este repo). Ordem: $DBT explícito
# → .venv do repo → dbt do PATH.
if [ -z "${DBT:-}" ]; then
  if [ -x "$REPO_ROOT/.venv/bin/dbt" ]; then DBT="$REPO_ROOT/.venv/bin/dbt"; else DBT="dbt"; fi
fi

# --- credenciais (.env do repo; valores têm caracteres especiais → python-dotenv, não `source`) ---
if ! eval "$(python3 - "$REPO_ROOT" <<'PY'
import sys, shlex
try:
    from dotenv import dotenv_values
except ModuleNotFoundError:
    sys.exit(0)  # sem python-dotenv: assume que o ambiente já exportou as vars
d = {}
for f in ("local.env", ".env"):
    try:
        d.update(dotenv_values(f"{sys.argv[1]}/{f}"))
    except OSError:
        pass
for k, v in d.items():
    if v is not None:
        print(f"export {k}={shlex.quote(v)}")
PY
)"; then
  echo "aviso: não consegui carregar os .env automaticamente; garanta MINIO_* no ambiente" >&2
fi

# --- storage/temp do DuckDB no disco com espaço + limite de RAM ---
export DUCKDB_MCID_PATH="${DUCKDB_MCID_PATH:-/mnt/data/duckdb/mcid_staging.duckdb}"
export DUCKDB_MCID_TEMP_DIR="${DUCKDB_MCID_TEMP_DIR:-/mnt/data/duckdb/tmp}"
export DUCKDB_MCID_MEMORY_LIMIT="${DUCKDB_MCID_MEMORY_LIMIT:-10GB}"
export DUCKDB_MCID_THREADS="${DUCKDB_MCID_THREADS:-3}"
mkdir -p "$(dirname "$DUCKDB_MCID_PATH")" "$DUCKDB_MCID_TEMP_DIR"

echo "DuckDB path : $DUCKDB_MCID_PATH"
echo "DuckDB tmp  : $DUCKDB_MCID_TEMP_DIR"
echo "RAM limit   : $DUCKDB_MCID_MEMORY_LIMIT   threads: $DUCKDB_MCID_THREADS"
echo

cd "$HERE"

BRONZES=(
  bronze_mcmv_historico_empreendimento_snh
  bronze_mcmv_historico_empreendimento_sftp
  bronze_mcmv_historico_serie_executiva
)
SILVERS=(
  silver_mcmv_historico_empreendimento_far
  silver_mcmv_historico_empreendimento_fds
  silver_mcmv_historico_empreendimento_rural
  silver_mcmv_historico_empreendimento
  silver_mcmv_historico_serie_executiva
  silver_mcmv_historico_serie_anual_ogu_fgts
)
GOLDS=(
  gold_mcmv_snapshot_empreendimento_atual
  gold_mcmv_historico_serie_mensal
)

build_one() {
  echo "=================================================================="
  echo "dbt build --select $1"
  echo "=================================================================="
  "$DBT" build --select "$1" --target "$TARGET"
}

case "${1:-all}" in
  bronzes) for m in "${BRONZES[@]}"; do build_one "$m"; done ;;
  silvers) for m in "${SILVERS[@]}"; do build_one "$m"; done ;;
  golds)   for m in "${GOLDS[@]}";   do build_one "$m"; done ;;
  all)
    "$DBT" seed --select issue_118_mcmv_serie_temporal_piloto --target "$TARGET"
    for m in "${BRONZES[@]}" "${SILVERS[@]}" "${GOLDS[@]}"; do build_one "$m"; done
    echo "=================================================================="
    echo "dbt test --select mcmv_historico_dbt"
    echo "=================================================================="
    "$DBT" test --select "mcmv_historico_dbt" --target "$TARGET"
    ;;
  *) build_one "$1" ;;
esac

echo
echo "OK."
