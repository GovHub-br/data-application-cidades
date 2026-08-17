#!/usr/bin/env bash
# Gera o artefato A: roda o raw_para_staging.py DE PRODUÇÃO contra o MinIO/Postgres da POC.
#
# Por que exportar tudo: o script chama load_dotenv(), que sobe a árvore de diretórios e
# encontra o .env de PRODUÇÃO. python-dotenv usa override=False, então variável exportada
# vence. Sem isto, o baseline seria escrito no lake real.
set -euo pipefail

POC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAIZ="$(cd "$POC_DIR/.." && pwd)"

set -a
# shellcheck disable=SC1091
. "$POC_DIR/.env"
set +a

export MINIO_ENDPOINT="$POC_MINIO_ENDPOINT"
export MINIO_ACCESS_KEY="$POC_MINIO_ACCESS_KEY"
export MINIO_SECRET_KEY="$POC_MINIO_SECRET_KEY"
export MINIO_BUCKET="$POC_MINIO_BUCKET"
export DB_DW_HOST_MCID="$POC_PG_HOST"
export DB_DW_PORT_MCID="$POC_PG_PORT"
export DB_DW_USER_MCID="$POC_PG_USER"
export DB_DW_PASSWORD_MCID="$POC_PG_PASSWORD"
export DB_DW_DBNAME_MCID="$POC_PG_DBNAME"
export LAKE_LOCAL_ARTIFACTS=0
export LAKE_TMPDIR=/var/tmp

# Trava dura: se estas não forem locais, alguém apontou o baseline para produção.
case "$MINIO_ENDPOINT" in
  localhost*|127.0.0.1*) ;;
  *) echo "ABORTADO: MINIO_ENDPOINT=$MINIO_ENDPOINT não é local." >&2; exit 1 ;;
esac
case "$DB_DW_HOST_MCID" in
  localhost|127.0.0.1) ;;
  *) echo "ABORTADO: DB_DW_HOST_MCID=$DB_DW_HOST_MCID não é local." >&2; exit 1 ;;
esac

echo "MinIO: $MINIO_ENDPOINT/$MINIO_BUCKET  |  PG: $DB_DW_HOST_MCID:$DB_DW_PORT_MCID/$DB_DW_DBNAME_MCID"
exec "$POC_DIR/.venv/bin/python" "$RAIZ/scripts/raw_para_staging.py" "$@"
