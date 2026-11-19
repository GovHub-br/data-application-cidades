#!/usr/bin/env bash
#
# MODO B — publicação do eixo histórico no Postgres `prod`.
#
# Copia as tabelas JÁ MATERIALIZADAS no arquivo DuckDB local (modo A,
# ./run-historico.sh + ./run-reloginho.sh) para o Postgres, uma família por vez,
# em ordem crescente de volume, com verificação entre cargas. NÃO relê a staging
# MinIO; o motor DuckDB roda neste processo, FORA do banco.
#
# Os três modos da change pipeline-bronze-historica-destino-trocavel:
#   A — dev         ./run-historico.sh / ./run-reloginho.sh (--target staging_duckdb)
#                   lê a staging MinIO, materializa em cidades.duckdb, não toca o prod
#   B — publicação  ESTE script: arquivo local -> Postgres via ATTACH
#   C — direto      dbt build --target prod_duckdb: staging MinIO -> Postgres na
#                   mesma execução, com o motor fora do banco
# O corpo de cada modelo é idêntico nos três; só o target muda.
#
# Uso:
#   ./publicar-historico.sh --listar                    # ordem de publicação
#   ./publicar-historico.sh --grupo bronzes --dry-run   # mede sem escrever
#   ./publicar-historico.sh --tabela bronze.bronze_dhist_serie_entrada_bb
#   ./publicar-historico.sh --grupo bronzes             # as 13 bronzes por família
#   ./publicar-historico.sh --grupo silvers             # silvers e golds do eixo
#   ./publicar-historico.sh --grupo tudo
#
# Cada carga registra uma linha em `lake._bronze_historico_log`. Divergência de
# contagem entre a tabela publicada e a local interrompe a publicação.
#
# Overrides (env var):
#   DUCKDB_MCID_PATH   arquivo .duckdb de origem (default /mnt/data/duckdb/cidades.duckdb)
#   DB_DW_*_MCID       credenciais do Postgres (mesmas do target `prod`)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

if [ -x "$REPO_ROOT/.venv/bin/python" ]; then PY="$REPO_ROOT/.venv/bin/python"; else PY=python3; fi

# --- credenciais (.env do repo; valores têm caracteres especiais → python-dotenv) ---
if ! eval "$("$PY" - "$REPO_ROOT" <<'PYEOF'
import sys, shlex
try:
    from dotenv import dotenv_values
except ModuleNotFoundError:
    sys.exit(0)
d = {}
for f in ("local.env", ".env"):
    try:
        d.update(dotenv_values(f"{sys.argv[1]}/{f}"))
    except OSError:
        pass
for k, v in d.items():
    if v is not None:
        print(f"export {k}={shlex.quote(v)}")
PYEOF
)"; then
  echo "aviso: não consegui carregar os .env automaticamente; garanta DB_DW_*_MCID no ambiente" >&2
fi

export DUCKDB_MCID_PATH="${DUCKDB_MCID_PATH:-/mnt/data/duckdb/cidades.duckdb}"

exec "$PY" "$HERE/scripts/publicar_historico.py" "$@"
