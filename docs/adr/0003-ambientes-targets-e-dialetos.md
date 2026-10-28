# 0003. Ambientes, targets e dialetos

- **Status:** Proposto
- **Data:** 2026-10-19
- **Relacionados:**
  - ADR-0001 (arquitetura de referência)
  - `dbt/mcid/profiles.yml`, `dags/dbt/mcid_cosmos_dag.py`,
  `dbt/mcid/macros/get_custom_schema.sql`, `infra/playbooks/postgres/`

## Contexto

O ADR-0001 define um warehouse só, o Postgres. O dbt, porém, chega até ele por três
targets:

- `staging_duckdb`: um arquivo DuckDB local, lendo a staging do lake, usado pelo time
  para testar modelos na própria máquina;
- `prod`: o Postgres com pg_duckdb;
- `prod_duckdb`: um processo DuckDB que lê o lake e grava no Postgres anexado via
  `ATTACH`.

O terceiro existe porque três domínios (`mcmv_historico`, `reforma_casa_brasil`,
`linha_financiada`) foram escritos em SQL nativo do DuckDB (`try_cast`,
`union_by_name`, `describe`), que o parser do Postgres rejeita. Para não
reescrevê-los, a DAG geral roda esses domínios num segundo task group, com esse
target (`dags/dbt/mcid_cosmos_dag.py`).

Não havia regra escrita sobre o papel de cada target. E o target chamado `prod`
aponta para o banco que está de pé na infra, que é o de **homologação**. Ainda não
existe ambiente de produção do ministério.

## Decisão

### Ambientes

O warehouse é um só: toda tabela de bronze, prata e ouro que alguém consome é
materializada no Postgres. O que varia é o motor que executa o SQL até gravar lá.

| Ambiente | Target dbt | Motor | Para que serve |
|---|---|---|---|
| Local | `staging_duckdb` | DuckDB num arquivo local, lendo a staging do lake | Testar modelos na máquina de quem desenvolve, sem depender do banco compartilhado |
| Homologação | `homolog` (hoje `prod`) | Postgres + pg_duckdb | Domínios em SQL de Postgres: `far`, `fds`, `rural`, `conjuntura`, `indicadores_mcmv`, `metadata` |
| Homologação | `homolog_duckdb` (hoje `prod_duckdb`) | DuckDB no processo do dbt, gravando no Postgres via `ATTACH` | Domínios em SQL nativo do DuckDB: `mcmv_historico`, `reforma_casa_brasil`, `linha_financiada` |
| Produção | a definir | a definir | Ambiente do ministério, montado com os mesmos playbooks |

Os targets passam a ter o nome do ambiente para onde apontam. `prod` fica
reservado para o ambiente do ministério.

### Regras

- **O que é consumido sai do Postgres.** O arquivo DuckDB local é só área de
  teste. Nenhum modelo pode existir apenas nele.
- **Cada domínio tem um dialeto, e só um.** A lista oficial de quais domínios rodam
  em `homolog_duckdb` é o `SELETOR_DUCKDB` em `dags/dbt/mcid_cosmos_dag.py`. Os
  demais rodam em `homolog`.
- **Domínio novo nasce em SQL de Postgres.** O `homolog_duckdb` não recebe domínio
  novo; a lista do `SELETOR_DUCKDB` só pode diminuir. Os três domínios atuais ficam
  em DuckDB até as medições do ADR-0008 dizerem se a migração compensa.
- **Os dois targets de homologação gravam no mesmo banco e nos mesmos schemas.** O
  `macros/get_custom_schema.sql` já roteia igual nos dois, e um domínio pode ler
  pelo `ref()` a tabela que o outro target materializou. A DAG geral roda primeiro
  o grupo do Postgres e depois o do DuckDB, porque o eixo histórico lê tabelas que o
  primeiro grupo materializa.
- **O time desenvolve usando os dois ambientes.** O target local continua no
  projeto. Se o teste local e a homologação derem resultados diferentes para o mesmo
  modelo, vale a homologação, e a diferença é tratada como defeito.

## Alternativas consideradas

**Reescrever os três domínios DuckDB em SQL de Postgres agora.** Deixaria um
dialeto só, mas é reescrita sem ganho para quem consome, e parte desses modelos
(`union_by_name` sobre muitos parquets) ainda não tem equivalente medido no
pg_duckdb. O que se decide agora é só não deixar o problema crescer.

**Remover o target local e desenvolver direto contra o Postgres de homologação.**
Um dialeto e um ambiente a menos, mas todo teste passaria a disputar o banco
compartilhado, e quem desenvolve dependeria da rede e do banco estarem de pé.

**Manter os nomes `prod` e `prod_duckdb`.** Evita mexer nas DAGs, mas o nome
engana quem lê o `profiles.yml` e deixa sem nome o ambiente do ministério quando
ele existir.

## Trade-offs

| Ganha | Paga |
|---|---|
| Os domínios DuckDB rodam sem reescrita | Dois dialetos de SQL em homologação, com regras diferentes para cada domínio |
| O problema dos dois dialetos para de crescer | Os três domínios DuckDB continuam diferentes até a migração |
| Teste local rápido, sem disputar o banco compartilhado | Manter os modelos rodando nos dois ambientes custa esforço |
| Nome do target diz para onde ele aponta | Um PR que mexe em perfis, DAGs e macro ao mesmo tempo |

O DuckDB anexado também roda serializado (pool `duckdb_historico`, de 1 slot, por
causa do lock do arquivo) e exige guardas de memória em `profiles.yml`.

## Consequências

- Como os schemas são os mesmos, a troca de nome não muda nenhum modelo; mudam o
  `profiles.yml`, as DAGs e a macro de schema.
- PR com domínio novo em `SELETOR_DUCKDB` não passa na revisão.

## Plano de migração

1. Renomear os targets em um PR só:
   - `dbt/mcid/profiles.yml`: `prod` → `homolog`, `prod_duckdb` → `homolog_duckdb`;
   - `dags/dbt/mcid_cosmos_dag.py` e `dags/conjuntura/conjuntura_dag.py`:
     `target_name`;
   - `dbt/mcid/macros/get_custom_schema.sql`: lista `targets_com_schema_custom` e
     comentários;
   - comentários em `dbt/mcid/macros/historico/corpos_bronze.sql` e nos documentos.

## Validação operacional

- A DAG geral passa nos dois task groups: `dbt build --target homolog` sem o
  seletor DuckDB e `dbt build --target homolog_duckdb` com ele.
- O `SELETOR_DUCKDB` não ganha entradas novas (conferido na revisão de PR).

## Questões em aberto

Nenhuma.
