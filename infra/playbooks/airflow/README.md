# Airflow

Sobe o Airflow 3 do projeto numa VM Debian 12, com Docker Compose. A imagem é a
mesma do ambiente local (`infra/docker/airflow/Dockerfile`), construída na VM a
partir de um clone deste repositório. As DAGs, os plugins e o dbt são montados
direto desse clone.

A VM do Airflow não tem banco. O banco de metadados fica no database `airflow` da
VM do Postgres, e o lake fica no RustFS.

Este playbook é autocontido: tem o próprio `inventory.yaml` e o próprio
`group_vars/`. Rode sempre de dentro desta pasta. A ordem entre os playbooks
está em [`infra/README.md`](../../README.md#playbooks-producao).

## Os arquivos

```
infra/playbooks/airflow/
├── inventory.yaml                  # o IP da VM
├── group_vars/airflow.yaml         # <-- TUDO que você configura fica aqui
├── install-airflow.yaml            # o playbook
├── templates/
│   ├── docker-compose.yaml.j2      # os containers do Airflow
│   └── airflow.env.j2              # variáveis de ambiente (senhas, endereços)
└── README.md
```

---

## Como rodar

Pré-requisito: o playbook do Postgres já rodou **desta mesma máquina**. As
senhas dos usuários `airflow` e `cidades` são lidas de
`~/.clusterlab/credentials/postgres0/`.

Antes da primeira vez:

- troque `<IP_DA_VM>` no `inventory.yaml`;
- troque `<IP_DO_POSTGRES>` e `<IP_DO_RUSTFS>` em `group_vars/airflow.yaml`;
- crie o `extra.env` (abaixo).

```bash
cd infra/playbooks/airflow
ansible-playbook -i inventory.yaml install-airflow.yaml
```

A URL e a senha do usuário `airflow` são impressas no final.

---

## Senhas e variáveis de ambiente

O playbook escreve `/opt/cidades-airflow/airflow.env` na VM a partir de três
fontes, todas **fora do repositório**:

| De onde | O quê |
| --- | --- |
| `~/.clusterlab/credentials/airflow0/` | geradas na 1ª execução: senha do login, fernet key, secret da API e do JWT |
| `~/.clusterlab/credentials/postgres0/` | senhas do banco `airflow` e do data warehouse `cidades` |
| `~/.clusterlab/credentials/rustfs0/app-cidades_secret_key` | secret do usuário do lake |
| `~/.clusterlab/credentials/openmetadata0/ingestion_bot_token` | token do OpenMetadata (só com `openmetadata_url` preenchido) |

A connection `postgres_default`, que as DAGs usam para escrever no data
warehouse, também vem daí: é a variável `AIRFLOW_CONN_POSTGRES_DEFAULT`. Ela
funciona, mas não aparece na lista de Connections da UI.

O que não é infra, como SFTP, OpenMetadata ou mascaramento, vai num arquivo seu,
no formato `CHAVE=valor`. Ele é copiado para o fim do `airflow.env`:

```bash
mkdir -p ~/.clusterlab/credentials/airflow0
cat > ~/.clusterlab/credentials/airflow0/extra.env <<'EOF'
SFTP_HOST=
SFTP_PORT=
SFTP_USER=
SFTP_PASSWORD=
MASKING_HMAC_SECRET=
EOF
chmod 600 ~/.clusterlab/credentials/airflow0/extra.env
```

A lista completa e o que cada uma faz estão em `infra/env/.env.example`.

**OpenMetadata.** As variáveis `OPENMETADATA_*` não vão no `extra.env`: com
`openmetadata_url` preenchido no `group_vars` (`http://<IP_DO_OPENMETADATA>:8585`,
sem `/api`), o playbook as escreve sozinho, com o token do ingestion-bot que o
playbook do OpenMetadata gravou em
`~/.clusterlab/credentials/openmetadata0/ingestion_bot_token`. Rode o do
OpenMetadata antes deste.

**Não perca a fernet key** (`fernet_seed`). Ela cifra as Connections gravadas no
banco. Se ela mudar, as Connections viram lixo e precisam ser recadastradas.

---

## Os containers

| Container | O quê |
| --- | --- |
| `airflow-init` | migra o banco de metadados (`airflow db migrate`) e sai |
| `airflow-apiserver` | UI e API REST, porta 8080 |
| `airflow-scheduler` | agenda e executa as tasks (LocalExecutor) |
| `airflow-dag-processor` | lê os arquivos de DAG |
| `airflow-triggerer` | tasks deferrable |
| `lake-tmp-init` | acerta o dono do volume de temporários do lake e sai |

Diferenças em relação ao `infra/docker-compose.yml` local:

- um container por componente, em vez do `airflow standalone`;
- sem Postgres, Superset e Jupyter;
- `network_mode: host`. Porta publicada pelo Docker (`ports:`) escapa do ufw,
  porque o Docker escreve as próprias regras de iptables. Na rede do host, quem
  decide quem chega na 8080 é o ufw, como nas outras VMs.

---

## Atualizando o código

As DAGs vêm do clone em `/opt/cidades-airflow/repo`. Para publicar uma versão
nova, troque `airflow_repo_version` (branch, tag ou commit) e rode de novo. O
playbook:

1. faz checkout da ref, descartando qualquer mudança feita à mão na VM;
2. reconstrói a imagem, o que só demora se o `Dockerfile` ou o `requirements.txt`
   mudaram;
3. recria os containers cuja imagem ou cujo ambiente mudaram.

Mudanças só em DAG não precisam de restart: o dag-processor relê os arquivos.

---

## Comandos úteis

```bash
# na VM
cd /opt/cidades-airflow
docker compose ps
docker compose logs -f airflow-scheduler
docker compose exec airflow-apiserver airflow dags list

# importar Variables (o arquivo precisa estar dentro do container)
docker compose cp variables.json airflow-apiserver:/tmp/variables.json
docker compose exec airflow-apiserver airflow variables import /tmp/variables.json
```
