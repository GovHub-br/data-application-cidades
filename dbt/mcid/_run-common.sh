#!/usr/bin/env bash
#
# Preâmbulo comum de run-historico.sh / run-reloginho.sh / rebuild-local.sh
# (modo A, dev — target staging_duckdb). É SOURCED, não executado.
#
# O que faz:
#   1. carrega credenciais MINIO_* / DB_* do .env do repo (python-dotenv);
#   2. fixa storage, dir de spill e limite SOFT de RAM do DuckDB;
#   3. embrulha cada invocação do dbt (run_dbt) num teto RÍGIDO de memória
#      (cgroup via `systemd-run --user --scope`). Se o `union_by_name` das
#      bronzes ou a janela da silver da série executiva estourar, o OOM mata
#      só a árvore do build — nunca a máquina.
#
# Define: $DBT $REPO_ROOT $TARGET, exports DUCKDB_MCID_*, funções
#         run_dbt() e run_common_banner().
#
# Overrides (env var):
#   DUCKDB_MCID_PATH             arquivo .duckdb            (default /mnt/data/duckdb/cidades.duckdb)
#   DUCKDB_MCID_TEMP_DIR         dir de spill do DuckDB     (default /mnt/data/duckdb/tmp)
#   DUCKDB_MCID_MEMORY_LIMIT     limite SOFT de RAM DuckDB  (default 4GB — derrama em disco acima disso)
#   DUCKDB_MCID_MAX_TEMP         teto do spill em disco     (default 60GB)
#   DUCKDB_MCID_THREADS          threads do DuckDB          (default 2)
#   DUCKDB_MCID_CGROUP_MAX       teto RÍGIDO de RAM da árvore do build (default 6G)
#   DUCKDB_MCID_CGROUP_SWAP_MAX  swap liberado pra árvore   (default 0 — força spill em disco, não swap)
#   DUCKDB_MCID_NO_CGROUP        =1 desliga o teto rígido (roda dbt direto)
#   DUCKDB_MCID_SERIE_MEM / _SERIE_CGROUP_MAX / _SERIE_CGROUP_SWAP_MAX
#                               limites SÓ do silver_mcmv_historico_serie_executiva
#                               (default 6GB / 11G / 2G — o SUM/GROUP BY sobre
#                               ~10M linhas não derrama; pico medido ~10,3 GiB)
#   DBT                          binário dbt                (default: .venv/bin/dbt → dbt no PATH)

# HERE do arquivo SOURCED = dbt/mcid/ (BASH_SOURCE[0] aqui é este arquivo).
_COMMON_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Layout atual (#128): dbt/mcid/ fica 2 níveis abaixo da raiz do repo.
REPO_ROOT="$(cd "$_COMMON_HERE/../.." && pwd)"
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

# --- storage/temp do DuckDB no disco com espaço + limites de RAM ---
export DUCKDB_MCID_PATH="${DUCKDB_MCID_PATH:-/mnt/data/duckdb/cidades.duckdb}"
export DUCKDB_MCID_TEMP_DIR="${DUCKDB_MCID_TEMP_DIR:-/mnt/data/duckdb/tmp}"
export DUCKDB_MCID_MEMORY_LIMIT="${DUCKDB_MCID_MEMORY_LIMIT:-4GB}"
export DUCKDB_MCID_MAX_TEMP="${DUCKDB_MCID_MAX_TEMP:-60GB}"
export DUCKDB_MCID_THREADS="${DUCKDB_MCID_THREADS:-2}"
mkdir -p "$(dirname "$DUCKDB_MCID_PATH")" "$DUCKDB_MCID_TEMP_DIR"

DUCKDB_MCID_CGROUP_MAX="${DUCKDB_MCID_CGROUP_MAX:-6G}"
DUCKDB_MCID_CGROUP_SWAP_MAX="${DUCKDB_MCID_CGROUP_SWAP_MAX:-0}"

# --- teto rígido: cada dbt roda dentro de um scope de usuário com MemoryMax ---
# Probe único. Se `systemd-run --user --scope` não funcionar aqui, cai pro modo
# sem teto (com aviso alto). `--scope` (e não `--service`) preserva ambiente e
# cwd do chamador — MINIO_*/DUCKDB_MCID_* e o `cd dbt/mcid` continuam valendo.
# MemorySwapMax=0 tira o build do swap (já saturado nesta máquina) e força o
# spill pro temp_directory em disco.
_RUN_CGROUP=0
if [ -z "${DUCKDB_MCID_NO_CGROUP:-}" ]; then
  if command -v systemd-run >/dev/null 2>&1 &&
     systemd-run --user --scope --quiet --collect -p MemoryMax=64M -- true >/dev/null 2>&1; then
    _RUN_CGROUP=1
  else
    echo "aviso: systemd-run --user --scope indisponível — build SEM teto rígido de RAM." >&2
    echo "       feche apps pesados ou reduza DUCKDB_MCID_MEMORY_LIMIT antes de rodar." >&2
  fi
fi

# run_dbt <args...> — invoca "$DBT" com os args, dentro do teto de RAM se houver.
# Com DUCKDB_MCID_SKIP_TESTS=1, os `dbt build` rodam só os modelos/seeds
# (--exclude-resource-type test) — usado pelo rebuild-local.sh, que roda a
# suíte de testes uma vez só no fim, depois dos DOIS domínios materializados
# (há testes em mcmv_historico_dbt que leem modelos do schema reloginho).
run_dbt() {
  local args=("$@")
  if [ -n "${DUCKDB_MCID_SKIP_TESTS:-}" ] && [ "${1:-}" = build ]; then
    args+=(--exclude-resource-type test)
  fi
  if [ "$_RUN_CGROUP" = 1 ]; then
    systemd-run --user --scope --quiet --collect \
      -p MemoryMax="$DUCKDB_MCID_CGROUP_MAX" \
      -p MemorySwapMax="$DUCKDB_MCID_CGROUP_SWAP_MAX" \
      -- "$DBT" "${args[@]}"
  else
    "$DBT" "${args[@]}"
  fi
}

run_common_banner() {
  echo "DuckDB path : $DUCKDB_MCID_PATH"
  echo "DuckDB tmp  : $DUCKDB_MCID_TEMP_DIR   (spill até $DUCKDB_MCID_MAX_TEMP)"
  echo "RAM DuckDB  : soft $DUCKDB_MCID_MEMORY_LIMIT   threads $DUCKDB_MCID_THREADS"
  if [ "$_RUN_CGROUP" = 1 ]; then
    echo "RAM teto    : $DUCKDB_MCID_CGROUP_MAX rígido (cgroup; swap $DUCKDB_MCID_CGROUP_SWAP_MAX) — estouro mata só o build"
  else
    echo "RAM teto    : (sem cgroup — cuidado)"
  fi
  echo
}
