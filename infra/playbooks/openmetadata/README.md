# OpenMetadata

Sobe o [OpenMetadata](https://open-metadata.org) numa VM Debian 12, com Docker
Compose. É o catálogo de dados do projeto: tabelas do DW, modelos e lineage do
dbt, DAGs do Airflow, glossário e classificações.

O que fica nesta VM e o que fica fora:

| Peça | Onde |
| --- | --- |
| servidor OpenMetadata 1.13.3 (UI + API, porta 8585) | esta VM |
| Elasticsearch 9.3.0 (busca do catálogo) | esta VM, só no loopback |
| banco de metadados | database `openmetadata` na VM do Postgres |
| ingestão (quem lê o DW, o dbt e o Airflow e publica no catálogo) | o Airflow do projeto, pela `openmetadata_ingestion_dag` |

Não há container de ingestão do próprio OpenMetadata: o cliente de pipeline fica
desligado (`PIPELINE_SERVICE_CLIENT_ENABLED=false`) e a ingestão roda no nosso
Airflow, como já roda hoje. A versão do servidor acompanha a linha do cliente
fixado em `helpers/openmetadata/config.py` (`openmetadata-ingestion==1.13.3.x`):
cliente e servidor de linhas diferentes divergem no schema das entidades.

Este playbook é autocontido: tem o próprio `inventory.yaml` e o próprio
`group_vars/`. Rode sempre de dentro desta pasta. A ordem entre os playbooks
está em [`infra/README.md`](../../README.md#playbooks-producao).

## Os arquivos

```
infra/playbooks/openmetadata/
├── inventory.yaml                    # o IP da VM
├── group_vars/openmetadata.yaml      # <-- TUDO que você configura fica aqui
├── install-openmetadata.yaml         # o playbook
├── templates/
│   ├── docker-compose.yaml.j2        # Elasticsearch, migração e servidor
│   └── openmetadata.env.j2           # configuração do servidor
└── README.md
```

---

## Como rodar

Pré-requisito: o playbook do Postgres já rodou **desta mesma máquina**. Ele cria
o banco `openmetadata` e grava a senha em
`~/.clusterlab/credentials/postgres0/openmetadata_password`.

Antes da primeira vez:

- troque `<IP_DA_VM>` no `inventory.yaml`;
- troque `<IP_DO_POSTGRES>` em `group_vars/openmetadata.yaml`;
- no `group_vars` do Postgres, inclua a VM do OpenMetadata nas redes
  autorizadas (`postgres_allowed_networks`).

```bash
cd infra/playbooks/openmetadata
ansible-playbook -i inventory.yaml install-openmetadata.yaml
```

A primeira execução baixa as imagens (~2 GB) e roda a migração do banco: conte
uns 10 minutos.

Depois, rode o playbook do Airflow com `openmetadata_url` preenchido em
`infra/playbooks/airflow/group_vars/airflow.yaml`
(`http://<IP_DO_OPENMETADATA>:8585`, **sem** `/api`). Ele leva a URL e o token do
ingestion-bot para o Airflow.

---

## Senhas e tokens

Tudo gerado na primeira execução e gravado **fora do repositório**:

```
~/.clusterlab/credentials/openmetadata0/
├── admin_password         # login admin@open-metadata.org na UI
├── fernet_seed            # cifra os segredos no banco. NÃO perca, NÃO troque
└── ingestion_bot_token    # JWT do ingestion-bot; o Airflow usa este
```

- **Admin.** O OpenMetadata nasce com `admin` / `admin`. O playbook troca pela senha
  gerada na primeira execução e, nas seguintes, só confere o login.
- **Chaves JWT.** As chaves que assinam os tokens são geradas na VM, em
  `/opt/cidades-openmetadata/keys/`. As que vêm na imagem são iguais em toda
  instalação do OpenMetadata, e quem as tem consegue forjar um token. Trocar estas
  chaves invalida os tokens emitidos: rode o playbook do OpenMetadata de novo e
  depois o do Airflow.
- **Token do ingestion-bot.** O playbook lê o token pela API e grava em
  `ingestion_bot_token`. Ele vira `OPENMETADATA_JWT_TOKEN` no Airflow.

---

## Requisitos da VM

- 4 vCPU e 8 GB de RAM: 2 GB de heap para o servidor e 1 GB para o
  Elasticsearch (`openmetadata_heap` e `elasticsearch_heap`).
- `vm.max_map_count >= 262144`, que o Elasticsearch exige. O playbook ajusta só se
  estiver abaixo.
- Menos de 85% do disco ocupado: acima disso o Elasticsearch para de alocar
  índices, e acima de 95% os deixa somente leitura.

---

## Comandos úteis

```bash
# na VM
cd /opt/cidades-openmetadata
docker compose ps
docker compose logs -f openmetadata-server
docker compose logs execute-migrate-all      # se a migração falhar
curl -s http://127.0.0.1:8585/api/v1/system/version
```
