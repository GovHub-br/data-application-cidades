#!/usr/bin/env bash
#
# Build local do schema `reloginho` (indicadores_mcmv_dbt) no target
# staging_duckdb — série SNH mensal + entregas por evento → indicadores de
# velocidade (reloginho) e de gargalo de desempenho.
#
# MODO A (dev) dos três modos da change pipeline-bronze-historica-destino-trocavel:
#   A — dev         este script (--target staging_duckdb): lê a staging MinIO e
#                   materializa em cidades.duckdb. NÃO toca o Postgres.
#   B — publicação  ./publicar-historico.sh: copia as tabelas já materializadas
#                   no arquivo local para o Postgres via ATTACH. Não relê o MinIO.
#   C — direto      --target prod_duckdb: lê a staging e escreve no Postgres na
#                   mesma execução, com o motor DuckDB FORA do banco.
# O corpo de cada modelo é idêntico nos três; só o target muda.
#
# Materializa o schema `reloginho` inteiro (2 silver + 6 gold do mapa) mais o
# upstream que ele exige: as 2 bronzes de ENTREGA por agente
# (bronze_reloginho_snh_entregas_evento_bb/_caixa) em `dados_historicos` e a
# cadeia medalhão FAR/FDS (`empreendimento_far`, `empreendimentos_fds`,
# tabelas `*_atual_*`) que os golds de gargalo leem. A série mensal SNH vem das
# bronzes por agente bronze_mcmv_historico_empreendimento_snh_bb/_caixa
# (mcmv_historico_dbt), reaproveitadas se já estiverem no arquivo.
#
# FALHA CONHECIDA: `assert_reloginho_frente_cobertura_mensal` acusa 5 combos
# (agente × frente) com buraco na série mensal. É lacuna da FONTE — os
# snapshots `historico_recente_*` da SNH faltam meses no MinIO (BB esparso:
# 2024-08/09/12, 2025-02/04/11; CAIXA só 2024-08). Não é erro do build; o
# script termina com exit != 0 por causa dela.
#
# Uso:
#   ./run-reloginho.sh                # tudo: build do schema + upstream + testes
#   ./run-reloginho.sh reloginho      # só os indicadores de velocidade (sem gargalo)
#   ./run-reloginho.sh gargalo        # só os 2 golds de gargalo + upstream
#   ./run-reloginho.sh tests          # dbt test --select indicadores_mcmv_dbt
#   ./run-reloginho.sh <selector>     # dbt build --select <selector> --target staging_duckdb
#
# Overrides (env var):
#   DUCKDB_MCID_PATH          arquivo .duckdb            (default /mnt/data/duckdb/cidades.duckdb)
#   DUCKDB_MCID_TEMP_DIR      dir de spill do DuckDB     (default /mnt/data/duckdb/tmp)
#   DUCKDB_MCID_MEMORY_LIMIT  limite de RAM do DuckDB    (default 10GB)
#   DUCKDB_MCID_THREADS       threads do DuckDB          (default 3)
#   DBT                       binário dbt                (default: dbt no PATH)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Layout atual (#128): dbt/mcid/ fica 2 níveis abaixo da raiz do repo.
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
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
export DUCKDB_MCID_PATH="${DUCKDB_MCID_PATH:-/mnt/data/duckdb/cidades.duckdb}"
export DUCKDB_MCID_TEMP_DIR="${DUCKDB_MCID_TEMP_DIR:-/mnt/data/duckdb/tmp}"
export DUCKDB_MCID_MEMORY_LIMIT="${DUCKDB_MCID_MEMORY_LIMIT:-10GB}"
export DUCKDB_MCID_THREADS="${DUCKDB_MCID_THREADS:-3}"
mkdir -p "$(dirname "$DUCKDB_MCID_PATH")" "$DUCKDB_MCID_TEMP_DIR"

echo "DuckDB path : $DUCKDB_MCID_PATH"
echo "DuckDB tmp  : $DUCKDB_MCID_TEMP_DIR"
echo "RAM limit   : $DUCKDB_MCID_MEMORY_LIMIT   threads: $DUCKDB_MCID_THREADS"
echo

cd "$HERE"

# Indicadores de velocidade (reloginho puro) — sem a cadeia de gargalo.
RELOGINHO=(
  +gold_indicadores_reloginho
  +gold_indicadores_reloginho_frente
  +gold_indicadores_reloginho_entregas
  +gold_resumo_reloginho_dashboard
)
# Indicadores de gargalo — puxam o medalhão FAR/FDS (`*_atual_*`).
GARGALO=(
  +gold_indicadores_gargalo_desempenho
  +gold_resumo_gargalo_desempenho_dashboard
)

case "${1:-all}" in
  reloginho) "$DBT" build --select "${RELOGINHO[@]}" --target "$TARGET" ;;
  gargalo)   "$DBT" build --select "${GARGALO[@]}"   --target "$TARGET" ;;
  tests)     "$DBT" test  --select indicadores_mcmv_dbt --target "$TARGET" ;;
  all)
    # Uma invocação: o dbt ordena bronze → silver → gold e roda os testes
    # (inclusive os cross-frente do gargalo) só depois de tudo materializado.
    "$DBT" build --select "${RELOGINHO[@]}" "${GARGALO[@]}" --target "$TARGET"
    ;;
  *)
    echo "dbt build --select $1"
    "$DBT" build --select "$1" --target "$TARGET"
    ;;
esac

echo
echo "OK."
