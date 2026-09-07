#!/usr/bin/env bash
#
# TESTE DE FUMAÇA — cold build completo do eixo histórico + reloginho.
#
# Dropa o arquivo cidades.duckdb (modo A, dev) e REMATERIALIZA do zero, sob o
# teto RÍGIDO de RAM do _run-common.sh, em 3 passos:
#   1. ./run-historico.sh all   (só modelos — DUCKDB_MCID_SKIP_TESTS=1)
#   2. ./run-reloginho.sh all    (só modelos)
#   3. dbt test nos DOIS domínios de uma vez (mcmv_historico_dbt tem testes que
#      leem o schema reloginho — testar antes dele existir dá Catalog Error).
#
# Serve para validar que:
#   - o build inteiro cabe no teto de memória sem derrubar a máquina;
#   - as contagens materializadas continuam batendo com a linha de base
#     (.informacoes/2026-09-05-estimativa-volume-staging-bronze-silver.md e
#      2026-09-05-linha-base-eixo-historico.md);
#   - a suíte de testes fica verde (menos a falha de FONTE conhecida
#     assert_reloginho_frente_cobertura_mensal — snapshots SNH faltando no MinIO).
#
# Uso:
#   ./rebuild-local.sh          # confirma antes de apagar
#   ./rebuild-local.sh -y       # sem confirmação (execução desassistida)
#
# Overrides (env var): ver cabeçalho de _run-common.sh (DUCKDB_MCID_*, DBT).
#
# SEM `set -e`: o veredito final (regressão ou não) é decidido pelo passo 3
# filtrando as falhas conhecidas — um exit != 0 no meio não deve abortar.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_run-common.sh
source "$HERE/_run-common.sh"
cd "$HERE"  # o passo 3 (dbt test) roda daqui; run-*.sh fazem seu próprio cd

ASSUME_YES=0
case "${1:-}" in -y | --sim | --yes) ASSUME_YES=1 ;; esac

WAL="$DUCKDB_MCID_PATH.wal"

echo "=================================================================="
echo "COLD BUILD — vai APAGAR e reconstruir do zero"
echo "=================================================================="
echo "arquivo: $DUCKDB_MCID_PATH"
[ -f "$DUCKDB_MCID_PATH" ] && echo "atual  : $(du -h "$DUCKDB_MCID_PATH" | cut -f1)"
echo
run_common_banner

if [ "$ASSUME_YES" != 1 ]; then
  read -r -p "Confirmar drop + rebuild? [y/N] " ans
  case "$ans" in y | Y | s | S) ;; *) echo "abortado."; exit 1 ;; esac
fi

# --- drop ---
rm -f "$DUCKDB_MCID_PATH" "$WAL"
find "$DUCKDB_MCID_TEMP_DIR" -mindepth 1 -delete 2>/dev/null || true
find "$REPO_ROOT" -maxdepth 2 -name 'duckdb_temp_storage_*' -delete 2>/dev/null || true
echo "arquivo removido. começando cold build…"
echo

# --- amostrador do uso de RAM da máquina, em background (a cada 3s) ---
PEAK_FILE="$(mktemp)"
(
  base=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  maxused=0
  while :; do
    avail=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
    used=$((base - avail))
    [ "$used" -gt "$maxused" ] && maxused=$used
    echo "$maxused" >"$PEAK_FILE"
    sleep 3
  done
) &
SAMPLER=$!
trap 'kill "$SAMPLER" 2>/dev/null || true' EXIT

LOG_H="$(mktemp)"
LOG_R="$(mktemp)"
LOG_T="$(mktemp)"
T0=$(date +%s)

# 1+2: materializa os MODELOS dos dois domínios (sem testes — DUCKDB_MCID_SKIP_TESTS).
#      A suíte roda uma vez só no passo 3, com tudo no lugar: mcmv_historico_dbt
#      tem testes que leem modelos do schema reloginho, então testar o histórico
#      antes do reloginho existir dá Catalog Error espúrio.
export DUCKDB_MCID_SKIP_TESTS=1
"$HERE/run-historico.sh" all 2>&1 | tee "$LOG_H"
RC_H=${PIPESTATUS[0]}
"$HERE/run-reloginho.sh" all 2>&1 | tee "$LOG_R"
RC_R=${PIPESTATUS[0]}
unset DUCKDB_MCID_SKIP_TESTS

# 3: suíte de testes dos dois domínios, agora que tudo está materializado.
echo
echo "=================================================================="
echo "dbt test --select mcmv_historico_dbt indicadores_mcmv_dbt"
echo "=================================================================="
run_dbt test --select mcmv_historico_dbt indicadores_mcmv_dbt --target "$TARGET" 2>&1 | tee "$LOG_T"
RC_T=${PIPESTATUS[0]}

T1=$(date +%s)
kill "$SAMPLER" 2>/dev/null || true
trap - EXIT
PEAK_KB=$(cat "$PEAK_FILE" 2>/dev/null || echo 0)
rm -f "$PEAK_FILE"

# `Done. PASS=.. WARN=.. ERROR=.. SKIP=.. TOTAL=..` — última linha de resumo do dbt
sum_line() { grep -aE 'PASS=[0-9]+ +WARN=' "$1" | tail -1 || true; }

# Falhas conhecidas que NÃO são regressão (lacuna de FONTE, ver run-reloginho.sh).
KNOWN_OK='assert_reloginho_frente_cobertura_mensal'
# Testes que falharam de verdade no passo 3, menos os conhecidos.
mapfile -t FAILED < <(
  grep -aoE '(Failure|Error) in test [a-z0-9_]+' "$LOG_T" |
    sed -E 's/.* test //' | sort -u |
    grep -vxF "$KNOWN_OK" || true
)

echo
echo "=================================================================="
echo "RESUMO DO COLD BUILD"
echo "=================================================================="
printf 'duração              : %d min %d s\n' $(((T1 - T0) / 60)) $(((T1 - T0) % 60))
printf 'arquivo .duckdb       : %s\n' "$(du -h "$DUCKDB_MCID_PATH" 2>/dev/null | cut -f1 || echo '??')"
printf 'pico de RAM (máquina) : +%d MiB acima do baseline\n' "$((PEAK_KB / 1024))"
echo
printf 'build histórico       : exit %s   %s\n' "$RC_H" "$(sum_line "$LOG_H")"
printf 'build reloginho       : exit %s   %s\n' "$RC_R" "$(sum_line "$LOG_R")"
printf 'testes (2 domínios)   : exit %s   %s\n' "$RC_T" "$(sum_line "$LOG_T")"
rm -f "$LOG_H" "$LOG_R" "$LOG_T"
echo

echo "--- contagens materializadas (confira contra a linha de base) ---"
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then PYBIN="$REPO_ROOT/.venv/bin/python"; else PYBIN=python3; fi
"$PYBIN" - "$DUCKDB_MCID_PATH" <<'PY' || echo "(não consegui abrir o .duckdb para contagem)"
import sys
import duckdb

con = duckdb.connect(sys.argv[1], read_only=True)
rows = con.execute(
    """
    select schema_name, table_name, estimated_size
    from duckdb_tables()
    where schema_name in (
        'dados_historicos', 'reloginho', 'serie_historica',
        'empreendimento_far', 'empreendimentos_fds', 'empreendimento_rural'
    )
    order by estimated_size desc, schema_name, table_name
    """
).fetchall()
for schema, table, n in rows:
    print(f"  {(n or 0):>13,}  {schema}.{table}")
PY
echo

echo "--- veredito ---"
echo "falha conhecida ignorada: $KNOWN_OK (lacuna de fonte SNH no MinIO)"
if [ "${#FAILED[@]}" -eq 0 ] && [ "$RC_H" -eq 0 ] && [ "$RC_R" -eq 0 ]; then
  echo "OK — cold build limpo. Nenhuma regressão."
  exit 0
fi
[ "$RC_H" -ne 0 ] && echo "REGRESSÃO: build do histórico saiu $RC_H (ver log acima)."
[ "$RC_R" -ne 0 ] && echo "REGRESSÃO: build do reloginho saiu $RC_R (ver log acima)."
if [ "${#FAILED[@]}" -gt 0 ]; then
  echo "REGRESSÃO: testes falharam além do conhecido:"
  printf '  - %s\n' "${FAILED[@]}"
fi
exit 1
