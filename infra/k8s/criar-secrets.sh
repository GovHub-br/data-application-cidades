#!/usr/bin/env bash
# Cria os Secrets do Cidades no namespace (padrão: cidades).
#
# Idempotente: só cria o que não existe, então rodar de novo não troca senha
# nenhuma. As senhas são geradas aqui e vivem só no cluster; para ler uma:
#   kubectl -n cidades get secret <nome> -o jsonpath='{.data.<chave>}' | base64 -d
#
# Uso:
#   infra/k8s/criar-secrets.sh rustfs                 # antes do helm do RustFS
#   infra/k8s/criar-secrets.sh postgres <secret-app>  # antes do cluster.yaml
#   infra/k8s/criar-secrets.sh openmetadata           # antes do helm do OpenMetadata
#   infra/k8s/criar-secrets.sh airflow [extra.env]    # antes do helm do Airflow
#
# <secret-app> é o arquivo com a secret do usuário app-cidades do RustFS.
# [extra.env] é opcional: variáveis das DAGs (SFTP, OpenMetadata...) no
# formato CHAVE=valor, somadas ao Secret cidades-airflow-env.
set -euo pipefail

NS="${NAMESPACE:-cidades}"
LAKE_ENDPOINT="${LAKE_ENDPOINT:-rustfs-svc.${NS}.svc:9000}"
LAKE_BUCKET="${LAKE_BUCKET:-data-lake-mcid}"
LAKE_ACCESS_KEY="${LAKE_ACCESS_KEY:-app-cidades}"
PG_HOST="${PG_HOST:-cidades-pg-rw.${NS}.svc}"

existe() { kubectl -n "$NS" get secret "$1" >/dev/null 2>&1; }
senha() { openssl rand -hex 24; }
ler() { kubectl -n "$NS" get secret "$1" -o jsonpath="{.data.$2}" | base64 -d; }

basic_auth() {  # nome usuario
  existe "$1" && { echo "ok: $1 já existe"; return; }
  kubectl -n "$NS" create secret generic "$1" --type=kubernetes.io/basic-auth \
    --from-literal=username="$2" --from-literal=password="$(senha)" >/dev/null
  echo "criado: $1"
}

generico() {  # nome chave=valor...
  local nome=$1; shift
  existe "$nome" && { echo "ok: $nome já existe"; return; }
  local args=()
  for kv in "$@"; do args+=(--from-literal="$kv"); done
  kubectl -n "$NS" create secret generic "$nome" "${args[@]}" >/dev/null
  echo "criado: $nome"
}

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

case "${1:-}" in
  rustfs)
    generico rustfs-admin RUSTFS_ACCESS_KEY=rustfs-admin RUSTFS_SECRET_KEY="$(senha)"
    ;;

  postgres)
    app_secret_file="${2:?informe o arquivo com a secret do app-cidades}"
    basic_auth cidades-pg-dw cidades
    basic_auth cidades-pg-airflow airflow
    basic_auth cidades-pg-leitura usuario_leitura
    basic_auth cidades-pg-openmetadata openmetadata
    generico cidades-lake \
      endpoint="$LAKE_ENDPOINT" bucket="$LAKE_BUCKET" \
      access_key="$LAKE_ACCESS_KEY" secret_key="$(tr -d '\n' < "$app_secret_file")"
    ;;

  openmetadata)
    # Fernet do OpenMetadata: 32 bytes em base64. NÃO troque depois.
    generico openmetadata-fernet fernet-key="$(openssl rand -base64 32)"
    # A política de senha exige maiúscula, minúscula, número e especial.
    generico openmetadata-admin password="$(openssl rand -hex 12)Aa1!"
    # Chaves que assinam os tokens; as da imagem são iguais em toda instalação.
    if existe openmetadata-jwt-keys; then
      echo "ok: openmetadata-jwt-keys já existe"
    else
      chaves="$(mktemp -d)"
      trap 'rm -rf "$chaves"' EXIT
      openssl genrsa -out "$chaves/private_key.pem" 2048 2>/dev/null
      openssl pkcs8 -topk8 -inform PEM -outform DER -in "$chaves/private_key.pem" -out "$chaves/private_key.der" -nocrypt
      openssl rsa -in "$chaves/private_key.pem" -pubout -outform DER -out "$chaves/public_key.der" 2>/dev/null
      kubectl -n "$NS" create secret generic openmetadata-jwt-keys \
        --from-file="$chaves/private_key.der" --from-file="$chaves/public_key.der" >/dev/null
      echo "criado: openmetadata-jwt-keys"
    fi
    ;;

  airflow)
    extra_env="${2:-}"
    airflow_pw="$(ler cidades-pg-airflow password)"
    dw_pw="$(ler cidades-pg-dw password)"
    generico airflow-metadata \
      connection="postgresql://airflow:${airflow_pw}@${PG_HOST}:5432/airflow"
    # Fernet: 32 bytes em base64 url-safe. NÃO troque depois: cifra as Connections.
    generico airflow-fernet-key fernet-key="$(openssl rand -base64 32 | tr '+/' '-_')"
    generico airflow-api-secret-key api-secret-key="$(senha)"
    generico airflow-jwt-secret jwt-secret="$(senha)"
    generico airflow-admin password="$(senha)"

    if existe cidades-airflow-env; then
      echo "ok: cidades-airflow-env já existe"
    else
      env_file="$(mktemp)"
      trap 'rm -f "$env_file"' EXIT
      cat > "$env_file" <<EOF
DB_DW_HOST_MCID=${PG_HOST}
DB_DW_PORT_MCID=5432
DB_DW_DBNAME_MCID=cidades
DB_DW_USER_MCID=cidades
DB_DW_PASSWORD_MCID=${dw_pw}
AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://cidades:${dw_pw}@${PG_HOST}:5432/cidades
MINIO_ENDPOINT=$(ler cidades-lake endpoint)
MINIO_ACCESS_KEY=$(ler cidades-lake access_key)
MINIO_SECRET_KEY=$(ler cidades-lake secret_key)
MINIO_BUCKET=$(ler cidades-lake bucket)
EOF
      [ -n "$extra_env" ] && grep -vE '^\s*(#|$)' "$extra_env" >> "$env_file"
      kubectl -n "$NS" create secret generic cidades-airflow-env --from-env-file="$env_file" >/dev/null
      echo "criado: cidades-airflow-env"
    fi
    ;;

  *)
    sed -n '2,16p' "$0"
    exit 1
    ;;
esac
