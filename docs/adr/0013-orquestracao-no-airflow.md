# 0013. Orquestração no Airflow

- **Status:** Proposto
- **Data:** 2026-10-29
- **Relacionados:**
  - ADR-0004 (DAG de fonte com `extract` e `transform`), ADR-0011 (segredos),
    ADR-0014 (CI com DagBag)
  - Issues #243 (F1), #244 (refatoração das ingestões) e #260 (orquestração dos DAGs)
  - `plugins/schedule_loader.py`, `dags/dbt/mcid_cosmos_dag.py`,
    `dags/conjuntura/conjuntura_dag.py`, `dags/data_ingest/`

## Contexto

O ADR-0004 decide o que cada DAG de fonte faz. Falta decidir como as DAGs são
escritas e encadeadas. A varredura de 29/10 achou:

- **Código que roda no parse.**
  - `ibge_ingest_dag` lê a Variable `IBGE_CONFIGURACOES` no topo do módulo. Uma
    vírgula sobrando no JSON derruba a DAG inteira.
  - As DAGs do dbt leem `os.environ['AIRFLOW_REPO_BASE']` sem valor padrão e
    chamam `os.makedirs` no topo.
  - O parse das DAGs do Cosmos roda `dbt ls`, e o timeout de import já foi
    aumentado para 300 s.
- **Horário fora da DAG.** 20 DAGs pegam o horário da Variable `dynamic_schedules`
  por `get_dynamic_schedule`, com chaves que nem sempre batem com o `dag_id`. A
  Variable não está no repositório, e o `README` do playbook do Airflow manda
  importar um `variables.json` que não existe.
- **Segredos em Variables.** `email_credentials` (um JSON com senha),
  `dados_fgv_email`/`dados_fgv_password` e `api_key_alphavantage`.
- **Execução verde sem dado.** icst, incc, mrv, caged, bacen, credito_pib, ibge e
  sidra só registram um aviso quando a fonte não devolve nada. Só abecip e fipe
  falham.
- **Espera ocupando worker.** O `conjuntura_dag` dispara 13 DAGs com
  `wait_for_completion=True` e segura 13 slots enquanto espera.
- **Execuções sobrepostas do dbt.** O `mcid_cosmos_dag` não tem `max_active_runs` e
  é disparado tanto pelo horário quanto pela `minio_transform_dag`.
- **Imports do Airflow 2.** A `minio_transform_dag` e o `mcid_cosmos_dag` ainda
  importam de `airflow.operators` e `airflow.decorators`, e a DAG do Tesouro é a
  única no estilo clássico, passando o CSV inteiro pelo XCom.

## Decisão

1. **Nada roda no parse.** O topo do módulo da DAG só importa e declara. Não lê
   Variable, não lê variável de ambiente sem valor padrão, não faz I/O e não cria
   pasta. Imports pesados (pandas, psycopg2) ficam dentro das tasks.
2. **O horário é um cron literal na DAG.** `get_dynamic_schedule` e a Variable
   `dynamic_schedules` saem. Mudar um horário é um PR.
3. **A configuração de cada DAG fica versionada no repositório,** no módulo da DAG
   ou num arquivo ao lado dele, e é validada por teste no CI. Isso vale para as
   listas que mudam por fonte (agregados do IBGE, séries do BACEN). Variable não
   guarda configuração.
4. **Segredo fica em Connection, nunca em Variable.** A Connection é definida por
   variável de ambiente (`AIRFLOW_CONN_*`) ou por secrets backend, e o Airflow mascara
   o valor nos logs.
5. **Sem dado nunca é verde.**
   - Fonte sem nada novo desde a última carga: a `extract` termina `skipped`
     (`AirflowSkipException`), e a `transform` é pulada junto.
   - Fonte que deveria trazer dado e não trouxe, ou trouxe menos que o esperado: a
     task falha.
6. **Disparo e espera.**
   - Quem espera outra DAG usa `deferrable=True`, sem ocupar worker.
   - A DAG do dbt tem `max_active_runs=1` e roda só no próprio horário: as
     conversões não a disparam (ADR-0004).
7. **Só o SDK do Airflow 3.** DAGs e tasks vêm de `airflow.sdk` (`DAG`, `task`,
   `get_current_context`), e os operators vêm dos providers. Todas as DAGs são
   escritas com TaskFlow, e entre as tasks passam só chaves e contagens, nunca
   conteúdo.
8. **Um teste de DagBag no CI** (ADR-0014) importa todas as DAGs, falha em qualquer
   erro de import e mede o tempo de parse. É ele que garante as regras 1 e 7.

## Alternativas consideradas

**Horário em Variable (prática atual).** Permite mudar o horário sem deploy, mas a
mudança não passa por revisão, a Variable não está no repositório, e um erro nela
quebra a DAG no parse.

**Configuração em Variable, lida dentro da task.** Tira o risco do parse e permite
mudar sem deploy, mas continua fora de revisão e sem histórico, e quem lê a DAG não
vê a configuração.

**Manter o aviso quando a fonte não traz dado.** Evita alarme em fonte que atrasa,
mas a execução verde esconde semanas sem dado novo. O `skipped` dá a mesma
visibilidade sem fingir sucesso.

**Esperar com `wait_for_completion` sem `deferrable`.** É o comportamento padrão,
mas cada espera prende um slot de worker pelo tempo inteiro.

## Trade-offs

| Ganha | Paga |
|---|---|
| A DAG não quebra no parse por causa de configuração | Mudar horário ou configuração exige PR e deploy |
| Horário e configuração revisados e com histórico | As 20 DAGs com horário em Variable precisam ser alteradas |
| Falta de dado aparece como falha ou `skipped` | Fonte que atrasa gera falha que alguém precisa olhar |
| Esperas sem ocupar worker | O triggerer do Airflow precisa estar de pé |

## Consequências

- O `plugins/schedule_loader.py` e a Variable `dynamic_schedules` saem.
- `IBGE_CONFIGURACOES` e `BACEN_SERIES` passam a ser arquivos versionados,
  validados no CI.
- As Variables com segredo viram Connections.
- O `conjuntura_dag` e o `mcid_cosmos_dag` ganham `deferrable=True` e
  `max_active_runs=1`, e o disparo do dbt pela `minio_transform_dag` sai.

## Plano de migração

1. Criar o teste de DagBag (ADR-0014). Ele começa com uma lista de exceções
   versionada, que só pode diminuir.
2. Corrigir já o que não depende da migração das DAGs: `deferrable=True` no
   `conjuntura_dag`, `max_active_runs=1` no `mcid_cosmos_dag`, e os imports do
   Airflow 2 nas duas DAGs.
3. Em cada DAG migrada para `extract` → `transform` (ADR-0004): cron literal,
   configuração versionada, Connection no lugar da Variable e a regra de sem dado.
4. Quando nenhuma DAG usar `get_dynamic_schedule`, remover o `schedule_loader.py` e
   as Variables.

## Validação operacional

- O teste de DagBag passa sem exceções, e nenhuma DAG passa do tempo de parse
  definido.
- Nenhum módulo em `dags/` chama `Variable.get` fora de uma task nem importa
  `get_dynamic_schedule` (teste no CI).
- Não existe Variable com segredo no Airflow de homologação.
- Nenhuma execução de DAG de fonte termina `success` com zero objetos gravados.

## Questões em aberto

Nenhuma.
