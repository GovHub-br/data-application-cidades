# 0003. Ambientes, targets e dialetos

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (decisão trocada: um dialeto e um target por ambiente, no lugar de dois dialetos em homologação)
- **Relacionados:**
  - ADR-0001 (arquitetura de referência), ADR-0002 (contrato entre camadas),
    ADR-0008 (materialização e execução), ADR-0012 (catálogo)
  - Issues #243 (F1) e #245 (refatoração do dbt), PR #241 (seletores)
  - `dbt/mcid/profiles.yml`, `dbt/mcid/selectors.yml`, `dags/dbt/mcid_cosmos_dag.py`,
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
`union_by_name`, `describe` na compilação), que o Postgres rejeita. Desde o PR #241,
o `selectors.yml` separa o projeto em dois seletores: `eixo_historico` (esses três
domínios) e `dominios_postgres` (o resto). A DAG geral roda os dois em sequência,
`dominios_postgres` e depois `eixo_historico`, este num pool de 1 slot por causa do
lock do arquivo DuckDB.

A versão de 19/10 deste ADR mantinha os dois dialetos e só impedia que o segundo
crescesse. A conferência de 29/10 mostrou que isso não se sustenta:

- **Não existe ordem certa para os dois grupos.** As dependências cruzam nos dois
  sentidos:
  - do grupo do Postgres para o histórico: `indicadores_mcmv_dbt`
    (`prata_historico_snh_apf_mes` e `prata_historico_snh_entregas_mes`, e por elas
    as ouros do reloginho) lê os `bronze_dhist_*`, que só são refeitos depois. O
    reloginho sempre usa o bronze da execução anterior, e num banco zerado falha;
  - do histórico para o Postgres: `prata_fds_historico_empreendimento` lê
    `prata_fds_dim_empreendimento`, e as pratas da `linha_financiada` leem os
    `bronze_geavo_*` da conjuntura.
- **SQL do DuckDB já vazou para o Postgres.** `prata_historico_snh_apf_mes`, no grupo
  do Postgres, usa `try_cast`.
- **O catálogo fica incompleto.** O `describe` roda na compilação, então o
  `docs generate` precisa do seletor `dominios_postgres`, e os três domínios ficam
  sem catálogo no OpenMetadata (ADR-0012).
- **Dois conjuntos de limites de memória.** Os guardas do DuckDB ficam no
  `profiles.yml`; os do pg_duckdb, no Postgres. O pg_duckdb 1.2.0 de homologação tem
  `duckdb.memory_limit` de 4 GB, mas `duckdb.max_temp_directory_size` vazio (sem
  limite) e `duckdb.threads = -1`. O piloto do pg_duckdb derrubou o banco em
  2026-09-05.

E o target chamado `prod` aponta para o banco que está de pé na infra, que é o de
**homologação**. Ainda não existe ambiente de produção do ministério. A macro
`get_custom_schema.sql` só roteia schema para os nomes `prod`, `prod_duckdb` e
`staging_duckdb`: qualquer outro target grava tudo no schema de fallback.

## Decisão

### Um dialeto: SQL de Postgres, com o pg_duckdb só na leitura do lake

| Camada | Como roda |
|---|---|
| Bronze | `select * from {{ fonte_lake('<fonte>') }}`, lido pelo pg_duckdb dentro do Postgres. A `fonte_lake` usa `union_by_name` para juntar cargas com colunas diferentes. |
| Prata e ouro | SQL de Postgres, executado pelo Postgres. |

- Nenhum modelo usa função que só existe no DuckDB (`try_cast`, `describe`,
  `union_by_name` fora da `fonte_lake`). A tipagem usa `case` com expressão regular e
  `cast`, ou as macros do projeto (`parse_*`).
- Nada roda contra o banco na compilação (`run_query`, `describe`): o SQL compilado
  não depende do estado do banco. Coluna que existe só em parte das cargas é tratada
  na prata com `coalesce` explícito sobre as colunas declaradas no contrato da fonte
  (ADR-0006).

### Um target por ambiente

| Ambiente | Target | Banco |
|---|---|---|
| Homologação | `homolog` | Postgres + pg_duckdb de homologação (o que hoje se chama `prod`) |
| Produção | `prod` | Ambiente do ministério, montado com os mesmos playbooks |

- Todos os domínios rodam no mesmo target, numa DAG com um grupo só. A ordem é a do
  grafo do dbt, inclusive entre domínios.
- Saem, depois da migração: os targets `prod_duckdb` e `staging_duckdb`, os seletores
  `eixo_historico` e `dominios_postgres`, o pool `duckdb_historico` e os scripts
  `_run-common.sh`, `run-historico.sh`, `run-reloginho.sh`, `rebuild-local.sh` e
  `publicar-historico.sh` de `dbt/mcid/`.
- A `get_custom_schema.sql` roteia o schema igual em `homolog` e `prod`.
- Domínio novo nasce no padrão. Durante a migração, nenhum domínio entra no
  `eixo_historico`.

### Limites do pg_duckdb antes da migração

Antes de qualquer domínio do eixo histórico rodar no Postgres, o playbook do
Postgres passa a fixar `duckdb.max_temp_directory_size` e `duckdb.threads`, além do
`duckdb.memory_limit` que já existe. Os valores saem de medição na homologação
(ADR-0008).

## Alternativas consideradas

**Manter dois dialetos, com a lista do DuckDB só diminuindo (versão de 19/10 deste
ADR).** Evita reescrever agora, mas as dependências cruzadas não têm ordem certa
entre dois grupos, o catálogo fica sem três domínios, e o time mantém dois conjuntos
de limites de memória e um pool serial.

**Tudo em DuckDB, gravando no Postgres via `ATTACH`.** Também é um dialeto só, mas o
eixo histórico já mostrou o custo: lock do arquivo, execução serial, guardas de
memória no processo do dbt e `describe` na compilação.

**Manter os nomes `prod` e `prod_duckdb`.** Evita mexer nas DAGs, mas o nome engana
quem lê o `profiles.yml` e deixa sem nome o ambiente do ministério quando ele
existir.

## Trade-offs

| Ganha | Paga |
|---|---|
| Uma DAG, um grupo, a ordem dada pelo grafo do dbt | Reescrever três domínios; só o `mcmv_historico` tem 26 modelos |
| Catálogo completo no OpenMetadata | O que hoje é uma função do DuckDB vira SQL de Postgres mais longo |
| Um conjunto de limites de memória, no banco | Toda leitura do lake passa pelo pg_duckdb dentro do Postgres: uma recarga grande pesa no banco que também serve o consumo |
| SQL compilado não depende do estado do banco | Coluna que varia entre cargas precisa estar declarada no contrato da fonte |

## Consequências

- A DAG geral passa a ter um `DbtTaskGroup` só, sem seletor, e o `docs generate` da
  DAG do OpenMetadata roda sem seletor (ADR-0012).
- O `try_cast` de `indicadores_mcmv_dbt` sai junto com a migração.
- Os guardas de memória deixam o `profiles.yml` e passam a ser parâmetros do
  pg_duckdb (ADR-0008).

## Plano de migração

1. Fixar os limites do pg_duckdb no playbook do Postgres de homologação e medir.
2. Renomear o target `prod` para `homolog` num PR só: `profiles.yml`, as DAGs
   (`mcid_cosmos_dag.py`, `conjuntura_dag.py`), a `get_custom_schema.sql`, os
   comentários e os documentos.
3. Migrar um domínio por PR, nesta ordem: `linha_financiada` → `reforma_casa_brasil`
   → `mcmv_historico`. Cada PR aplica também o contrato do ADR-0002 naquele domínio e
   compara os números da ouro antes e depois.
4. Com os três migrados, remover o `prod_duckdb`, o `staging_duckdb`, os seletores, o
   pool e os scripts, e deixar a DAG com um grupo só.

## Validação operacional

- `dbt build --target homolog` roda o projeto inteiro numa execução só.
- Nenhum modelo usa `try_cast`, `describe` ou `run_query` (teste no CI; a
  `fonte_lake` é a única exceção, pelo `union_by_name`).
- O `docs generate` sem seletor passa, e todos os domínios aparecem no catálogo com
  os tipos das colunas.

## Questões em aberto

1. **Desenvolvimento local.** Sem o `staging_duckdb`, quem desenvolve testa contra um
   Postgres com pg_duckdb no compose local ou contra a homologação, com um schema
   por pessoa. Hoje o compose (`infra/docker-compose.yml`) sobe `postgres:17-alpine`,
   sem pg_duckdb, então a primeira opção exige trocar a imagem. Decidir antes de
   remover o target.
2. **Valores dos limites do pg_duckdb.** Saem da medição do passo 1.
