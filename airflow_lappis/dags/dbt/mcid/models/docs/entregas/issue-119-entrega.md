# Issue #119 — Entrega: Padronizacao Silver e Base para Marts de Dashboard

## Resumo para colar na issue

Foi implementada a primeira versao padronizada da camada `mcmv_silver`, usando o glossario MCID como referencia semantica para nomes, grao, chaves, datas, territorios, status, valores e campos tecnicos.

Foram criados modelos dbt por frente do MCMV, mantendo contrato comum para consumo futuro por dashboards e tabelas gold/marts:

- `silver_mcmv_minha_casa_minha_vida_base`: 223.498 registros
- `silver_mcmv_frentes_base`: 223.545 registros
- `silver_mcmv_far_base`: 1.646 registros
- `silver_mcmv_entidades_base`: 343 registros
- `silver_mcmv_rural_base`: 9.474 registros
- `silver_mcmv_classe_media_base`: 110.979 registros
- `silver_mcmv_cidades_base`: 197 registros
- `silver_mcmv_reforma_base`: 100.859 registros
- `silver_mcmv_conjuntura_base`: 47 registros
- `silver_mcmv_sub50_base`: modelo implementado a partir das fontes FNHIS/SUB50, com 8.328 registros esperados nas fontes raw inventariadas
- `silver_mcmv_pro_moradia_base`: 0 registros, contrato criado como placeholder

## O que foi entregue

- Inventario de tabelas silver e marts existentes.
- Glossario inicial MCID.
- Matriz entre termos de negocio e campos fisicos.
- Priorizacao de modelos a ajustar.
- Modelos dbt `mcmv_silver_dbt` por frente operacional.
- Schema dbt `mcmv_silver` materializado no Postgres.
- Padrao comum de colunas para APF, contrato, empreendimento, municipio, UF, IBGE, responsavel, UH, valores, execucao, status e datas.
- Testes dbt para o modelo consolidado `silver_mcmv_frentes_base`.
- Reforco de teste de chave em `empreendimento_far_dbt/silver/schema.yml` para `empreendimento.apf`.

## Evidencias no repositorio

- `airflow_lappis/dags/dbt/mcid/models/docs/issue-119-padrao-silver-marts-dashboard.md`
- `airflow_lappis/dags/dbt/mcid/models/docs/glossario-mcid.md`
- `airflow_lappis/dags/dbt/mcid/models/docs/evidencias/issue-119-inventario-silver-marts.csv`
- `airflow_lappis/dags/dbt/mcid/models/docs/evidencias/issue-119-matriz-glossario-campos.csv`
- `airflow_lappis/dags/dbt/mcid/models/docs/evidencias/issue-119-priorizacao-modelos.csv`
- `airflow_lappis/dags/dbt/mcid/models/docs/evidencias/issue-119-mcmv-silver-frentes.csv`
- `airflow_lappis/dags/dbt/mcid/models/docs/evidencias/issue-119-mcmv-silver-fontes-validacao.csv`

## Como validar

No DBeaver:

```text
Databases > cidades > Schemas > mcmv_silver > Tables
```

No terminal:

```bash
cd airflow_lappis/dags/dbt/mcid
dbt run --select mcmv_silver_dbt
dbt test --select silver_mcmv_frentes_base
```

Validacao executada nesta entrega:

- `dbt run --select mcmv_silver_dbt`: executado com sucesso.
- `dbt test --select silver_mcmv_frentes_base`: 3 testes `not_null` executados com sucesso.

## Observacoes importantes

- As marts finais de dashboard devem ficar em gold/marts, e nao dentro da silver. A silver criada aqui e a base tratada/padronizada para alimentar essas marts.
- `SUB50/FNHIS` foi conectado ao contrato silver usando as fontes `novo_mcmv_fnhis_sub_50_propostas_apresentadas` e `novo_mcmv_fnhis_sub_50_propostas_selecionadas` quando materializadas em `__dados_brutos`.
- `Pro-Moradia` segue como lacuna controlada: o modelo existe com contrato comum, mas a fonte definitiva ainda precisa ser localizada.
- A frente `Rural` considera arquivos com separador pipe das bases `INT057` e `INT065`.
- Dados GEAVO/FGTS foram considerados para `Classe Media`, `Reforma Casa Brasil` e `Conjuntura`.

## Pendencias recomendadas

- Materializar e validar `SUB50/FNHIS` no Postgres quando a conexao VPN estiver disponivel para o dbt.
- Localizar a fonte definitiva de `Pro-Moradia`.
- Criar marts gold/dashboard em cima da `mcmv_silver`, preservando aliases quando houver dashboard existente no Superset.
