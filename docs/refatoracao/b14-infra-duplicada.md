# B14 · Infra sem duplicação

> Parte do [guia das refatorações](README.md) · Prioridade P2 · Status: issue aberta · Issue: [#261](https://github.com/GovHub-br/data-application-cidades/issues/261) · PR: —

## Problema

- **Três cópias da configuração do Airflow.** As envs `AIRFLOW__*`, `PYTHONPATH` e `LAKE_*` estão repetidas em:
  - `infra/docker-compose.yml:34-73`;
  - `infra/playbooks/airflow/templates/docker-compose.yaml.j2:16-39`;
  - `infra/k8s/airflow/values.yaml:46-87`.

  O k8s ainda diverge, com `LAKE_TMPDIR=/tmp/lake`, enquanto o compose diz para não usar `/tmp`.
- **Playbooks sem roles nem `group_vars/all`.** Três blocos aparecem copiados:
  - a instalação do Docker, em airflow e openmetadata;
  - o bloco UFW, nos quatro playbooks;
  - as `allowed_networks`, nos quatro `group_vars`.
- **`infra/env/.env.example` desatualizado:**
  - `MINIO_BUCKET=data-lake-ipea`;
  - `MINIO_ENDPOINT=minio:9000`, um serviço que nenhum compose define;
  - `AIRFLOW_IMAGE_NAME` sem uso;
  - falta o `MASKING_HMAC_SECRET`, que é obrigatório.
- **Compose local:**
  - segredos padrão: `SUPERSET_SECRET_KEY`, admin/admin e Jupyter sem token;
  - o Postgres publicado em todas as interfaces;
  - o volume `airflow_logs` sem uso;
  - o texto "Ipea - Local Dev!".
- **O `infra/README.md:62` diz "três VMs"**, mas são quatro playbooks.

## Correção proposta

- **Envs:** uma fonte só para as envs do Airflow, gerando o compose, a VM e o k8s.
- **Playbooks:** roles compartilhadas para Docker e UFW, e um `group_vars/all`.
- **Exemplos e textos:** `.env.example` e README atualizados.

## Relação

Avulso. Não bloqueia nada.
