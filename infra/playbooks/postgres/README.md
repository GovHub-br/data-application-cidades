# PostgreSQL + pg_duckdb

Sobe o PostgreSQL numa VM Debian 12 com o [pg_duckdb](https://github.com/duckdb/pg_duckdb)
instalado e já apontado para o lake no RustFS. Com isso o data lake pode ser
consultado em SQL direto do Postgres:

```sql
SELECT * FROM read_parquet('s3://data-lake-mcid/staging/<fonte>/<arquivo>.parquet');
```

É a mesma montagem do homolog (`10.0.0.50`): PostgreSQL 18 do PGDG e pg_duckdb
1.2.0 compilado do fonte. É por esse caminho que o dbt materializa a prata e o
ouro (`SET LOCAL duckdb.force_execution = true` no `dbt_project.yml`).

Este playbook é autocontido: tem o próprio `inventory.yaml` e o próprio
`group_vars/`. Rode sempre de dentro desta pasta. A ordem entre os playbooks
está em [`infra/README.md`](../../README.md#playbooks-producao).

## Os arquivos

```
infra/playbooks/postgres/
├── inventory.yaml            # o IP da VM
├── group_vars/postgres.yaml  # <-- TUDO que você configura fica aqui
├── install-postgres.yaml     # o playbook
└── README.md
```

---

## O que ele faz

1. Instala o PostgreSQL do repositório do PGDG.
2. Clona o pg_duckdb no commit fixado em `pg_duckdb_version` e compila. **A primeira
   execução demora de 30 a 60 minutos**, porque compila o DuckDB inteiro. Cada
   versão fica em `/usr/local/src/pg_duckdb-<commit>/`, então rodar de novo não
   recompila.
3. Configura `conf.d/90-cidades.conf` (`shared_preload_libraries`, `duckdb.*`,
   `listen_addresses`) e libera as redes autorizadas no `pg_hba.conf`.
4. Cria os usuários e os bancos (tabela abaixo), inclusive o do OpenMetadata.
5. Cria a extensão `pg_duckdb` no banco `cidades`, baixa o `httpfs` do DuckDB e grava
   o secret S3 do lake.
6. Fecha o firewall (ufw) e confere, como o usuário `cidades`, se o pg_duckdb lista o
   bucket.

---

## Como rodar

Antes da primeira vez:

- troque `<IP_DA_VM>` no `inventory.yaml`;
- troque `<IP_DO_RUSTFS>` em `group_vars/postgres.yaml`;
- crie no RustFS o bucket e o usuário de aplicação (abaixo).

O pg_duckdb e o Airflow acessam o lake com o usuário `app-cidades`. Gere a
secret dele **na sua máquina**, no caminho em que os playbooks procuram:

```bash
mkdir -p ~/.clusterlab/credentials/rustfs0
openssl rand -hex 24 > ~/.clusterlab/credentials/rustfs0/app-cidades_secret_key
chmod 600 ~/.clusterlab/credentials/rustfs0/app-cidades_secret_key
```

Depois, na VM do RustFS, crie o bucket, a policy e o usuário com essa secret,
seguindo a receita de [`../rustfs/policies/README.md`](../rustfs/policies/README.md)
com os nomes do Cidades:

```bash
rc mb rustfs/data-lake-mcid
rc admin policy create rustfs/ data-lake-mcid-readwrite ./data-lake-mcid-readwrite.json
rc admin user add rustfs/ app-cidades '<conteúdo de app-cidades_secret_key>'
rc admin policy attach rustfs/ data-lake-mcid-readwrite --user app-cidades
```

O JSON da policy é o do exemplo `datalake-readwrite`, trocando `datalake` por
`data-lake-mcid`.

```bash
cd infra/playbooks/postgres
ansible-playbook -i inventory.yaml install-postgres.yaml
```

O playbook usa a coleção `community.postgresql`. Ela já vem no pacote `ansible`;
quem instalou só o `ansible-core` precisa de
`ansible-galaxy collection install community.postgresql`.

Se o build cair por falta de memória, reduza os jobs:
`--extra-vars pg_duckdb_build_jobs=2`. O padrão é um job a cada 2 GB de RAM.

---

## Usuários e bancos

| Usuário | Banco | Para quê |
| --- | --- | --- |
| `cidades` | `cidades` (dono) | data warehouse: o dbt e as DAGs escrevem aqui; usa o pg_duckdb |
| `airflow` | `airflow` (dono) | banco de metadados do Airflow |
| `openmetadata` | `openmetadata` (dono) | banco do catálogo OpenMetadata |
| `usuario_leitura` | `cidades` (leitura) | Superset e analistas |
| `duckdb_users` | — | sem login; quem é membro executa DuckDB (`duckdb.postgres_role`) |

As senhas são geradas na primeira execução e gravadas **fora do repositório**,
na sua máquina:

```
~/.clusterlab/credentials/postgres0/cidades_password
~/.clusterlab/credentials/postgres0/airflow_password
~/.clusterlab/credentials/postgres0/openmetadata_password
~/.clusterlab/credentials/postgres0/usuario_leitura_password
```

Os playbooks do Airflow e do OpenMetadata leem as senhas deles daí. Por isso,
rode o do Postgres antes, da mesma máquina.

O `usuario_leitura` enxerga o que o `cidades` criar **depois** do playbook. É
assim por causa dos `ALTER DEFAULT PRIVILEGES`: o dbt cria os schemas como
`cidades`.

---

## Como o pg_duckdb fala com o RustFS

O "secret" do DuckDB, no pg_duckdb, é guardado no próprio Postgres, como objetos
do foreign data wrapper `duckdb`:

```sql
-- o endereço (um por banco)
CREATE SERVER lake_s3 TYPE 's3' FOREIGN DATA WRAPPER duckdb
  OPTIONS (endpoint '<IP_DO_RUSTFS>:9000', region 'us-east-1',
           url_style 'path', use_ssl 'false');

-- a credencial (uma por usuário do Postgres)
CREATE USER MAPPING FOR cidades SERVER lake_s3
  OPTIONS (key_id 'app-cidades', secret '...');
```

O playbook cria e mantém esses dois. Alguns detalhes que pegam:

- **O secret é por usuário.** Só quem tem `USER MAPPING` lê o lake. Até o `postgres`
  (superusuário) recebe 403 do RustFS, porque não tem mapping. Para dar acesso a outro
  usuário, inclua-o em `pg_duckdb_users` e rode de novo.
- **`url_style 'path'` é obrigatório.** Sem domínio configurado, o RustFS só aceita
  `http://host:9000/bucket/obj`.
- **`endpoint` sem `http://`.** O DuckDB monta a URL sozinho, e `use_ssl 'false'` diz
  que é http.
- **Não use `duckdb.create_simple_secret()` para atualizar.** Cada chamada cria um
  `SERVER` novo (`simple_s3_secret`, `_1`, `_2`...). Foi o que aconteceu no homolog,
  que tem três.

Para trocar a chave ou o endereço do lake, mude o `group_vars` ou o arquivo da
secret e rode o playbook de novo. Ele só recria o secret quando há diferença.

---

## Consultando o lake

Conectado como `cidades`:

```sql
-- um arquivo
SELECT * FROM read_parquet('s3://data-lake-mcid/staging/abecip/2025-10/financiamentos_sbpe_mensal.parquet') LIMIT 10;

-- vários arquivos, com colunas que mudam entre eles
SELECT * FROM read_parquet('s3://data-lake-mcid/staging/ibge/**/*.parquet', union_by_name => true);

-- o que existe no bucket
SELECT * FROM duckdb.query($$ SELECT file FROM glob('s3://data-lake-mcid/staging/**') $$);

-- materializar no Postgres
CREATE TABLE bronze.exemplo AS
SELECT * FROM read_parquet('s3://data-lake-mcid/staging/<fonte>/<arquivo>.parquet');
```

O `read_parquet` devolve linhas do tipo `duckdb.row`. O `SELECT *` expande as
colunas normalmente. Para usar uma coluna em `WHERE` ou num `JOIN`, use
`r['coluna']`:

```sql
SELECT r['uf'], r['valor']
FROM read_parquet('s3://data-lake-mcid/staging/<...>.parquet') r
WHERE r['uf'] = 'DF';
```

---

## Sobre a versão

| Variável | Valor | O quê |
| --- | --- | --- |
| `postgres_version` | `18` | a do homolog; o pg_duckdb suporta de 14 a 18 |
| `pg_duckdb_version` | `ee38d3b` (main de 2026-07-17) | instala a extensão `1.2.0`, a mesma do homolog |

O último release com tag do pg_duckdb é o `v1.1.1`, de dez/2025. O homolog roda
a `1.2.0`, que só existe na `main`, por isso o commit fixo. Para atualizar, troque
`pg_duckdb_version` e rode de novo. O playbook compila a versão nova numa pasta
nova, reinicia o Postgres e roda `ALTER EXTENSION pg_duckdb UPDATE`.

---

## Comandos úteis

```bash
ansible postgres -i inventory.yaml -b -m command -a "systemctl status postgresql@18-main"
ansible postgres -i inventory.yaml -b -m command -a "tail -n 50 /var/log/postgresql/postgresql-18-main.log"

# na VM
sudo -u postgres psql -d cidades -c "SELECT srvname, srvoptions FROM pg_foreign_server;"
sudo -u postgres psql -d cidades -c "SELECT srvname, usename FROM pg_user_mappings;"
```
