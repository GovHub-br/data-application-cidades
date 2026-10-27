#!/usr/bin/env bash
#
# Gera infra/docker/airflow/requirements.lock.txt.
#
# Por que existe: o `pip install` resolve um pacote de cada vez e nao revisa
# decisoes anteriores. A imagem oficial do Airflow ja chega com ~440 pacotes
# instalados, e o openmetadata-ingestion tem opiniao sobre a versao de varios
# deles. Instalar `requirements.txt` solto por cima rebaixa alguns e deixa o
# ambiente internamente inconsistente -- o `uv pip check` do Dockerfile pega
# isso e derruba o build.
#
# A correcao e resolver tudo de uma vez: os pins que queremos MAIS o inventario
# inteiro da imagem base, num unico passe. O resultado vira este lock, que o
# Dockerfile instala literalmente. Mesmo modelo do data-application-minc.
#
# Rode sempre que mexer em requirements.txt ou subir AIRFLOW_VERSION, e
# commite o lock junto com a mudanca.
#
# O lock atual entra como preferencia: o resolvedor so troca a versao de um
# pacote quando o requirements.txt obriga. UPGRADE=1 descarta a preferencia e
# atualiza tudo -- faca isso num PR proprio.
#
# Uso:
#   ./infra/docker/airflow/compile-lock.sh
#   UPGRADE=1 ./infra/docker/airflow/compile-lock.sh

set -euo pipefail

AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.3.2}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
BASE_IMAGE="apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
requirements="${repo_root}/requirements.txt"
lockfile="${repo_root}/infra/docker/airflow/requirements.lock.txt"
workdir="$(mktemp -d)"
# O container roda como o usuario `airflow` (UID 50000), que precisa gravar aqui.
chmod a+rwx "${workdir}"
trap 'rm -rf "${workdir}"' EXIT

echo "==> Base: ${BASE_IMAGE}"

# Inventario da imagem base. Sem --entrypoint o container cai na CLI do Airflow
# e imprime o help em vez da lista de pacotes.
docker run --rm --entrypoint python "${BASE_IMAGE}" \
  -m pip list --format=freeze \
  > "${workdir}/base_freeze.txt"
cut -d= -f1 "${workdir}/base_freeze.txt" | sort -u > "${workdir}/base_pkgs.txt"

echo "==> Pacotes ja presentes na imagem base: $(wc -l < "${workdir}/base_pkgs.txt")"

{
  echo "apache-airflow==${AIRFLOW_VERSION}"
  cat "${requirements}"
  echo
  # Os nomes da base entram SEM pin: o resolvedor escolhe versoes que fechem
  # com o que estamos pedindo. Sao excluidos os que ja pinamos acima e os do
  # proprio toolchain de instalacao.
  grep -viE '^(apache-airflow|openmetadata-ingestion|pip|setuptools|wheel|uv)$' "${workdir}/base_pkgs.txt"
} > "${workdir}/lock.in"

echo "==> Requisitos de entrada: $(grep -cE '^[a-zA-Z]' "${workdir}/lock.in")"

# O uv le o arquivo de saida que ja existe como preferencia de versoes. Sem
# lock anterior, a preferencia e a propria imagem base: senao o resolvedor sobe
# para a ultima versao todos os ~440 nomes sem pin, e o primeiro lock traria
# centenas de upgrades que ninguem pediu.
upgrade_flag=""
if [[ "${UPGRADE:-0}" == "1" ]]; then
  upgrade_flag="--upgrade"
  echo "==> UPGRADE=1: ignorando o lock atual, todas as versoes serao atualizadas"
elif [[ -f "${lockfile}" ]]; then
  cp "${lockfile}" "${workdir}/lock.out"
else
  cp "${workdir}/base_freeze.txt" "${workdir}/lock.out"
fi

echo "==> Resolvendo (pode levar alguns minutos)..."

# O uv da propria imagem base resolve; nao precisa de outra imagem.
docker run --rm \
  -v "${workdir}:/w" \
  --entrypoint bash \
  "${BASE_IMAGE}" \
  -c "uv pip compile --quiet --python-version ${PYTHON_VERSION} ${upgrade_flag} \
        --output-file /w/lock.out /w/lock.in"

{
  echo "# GERADO POR infra/docker/airflow/compile-lock.sh -- NAO EDITE A MAO."
  echo "#"
  echo "# Resolvido contra ${BASE_IMAGE}."
  echo "# Para mudar uma dependencia, edite requirements.txt e rode o script."
  echo "#"
  grep -v '^#' "${workdir}/lock.out"
} > "${lockfile}"

echo "==> Escrito ${lockfile#"${repo_root}/"} com $(grep -cE '^[a-zA-Z]' "${lockfile}") pacotes"
