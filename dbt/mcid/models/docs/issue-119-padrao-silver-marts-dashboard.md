# Issue #119 — Padrão para Silver e Marts de Dashboard

## Contexto Arquitetural

A arquitetura considerada é:

1. Fonte envia dados para Object Storage.
2. MinIO mantém `raw/` e `staging/`.
3. Airflow orquestra a leitura.
4. dbt, com DuckDB adapter, lê obrigatoriamente `staging/` no MinIO.
5. Bronze é cópia fiel/projeção mínima da staging.
6. Silver é a camada tratada: nomes, tipos, chaves, datas, valores e campos técnicos. A origem da transformação deve ser sempre MinIO `staging/` via DuckDB.
7. Gold/marts alimentam Superset e planilhas.

Esta issue não cria indicadores de gargalo/desempenho. O objetivo é definir e
aplicar padrão para a base semântica usada por qualquer dashboard.

## Inventário Inicial

Foram inventariados os modelos dbt MCID existentes:

- Silver: 23 modelos.
- Gold/marts: 31 modelos.
- Schema docs encontrados: `conjuntura_dbt`, `empreendimento_far_dbt`, `entidades_dbt` e `indicadores_mcmv_dbt`.
- Glossário dedicado: não havia arquivo único; foi criado `models/docs/glossario-mcid.md` como referência inicial.

Evidências:

- `evidencias/issue-119-inventario-silver-marts.csv`
- `evidencias/issue-119-matriz-glossario-campos.csv`
- `evidencias/issue-119-priorizacao-modelos.csv`
- `evidencias/issue-119-mcmv-silver-frentes.csv`
- `evidencias/issue-119-mcmv-silver-fontes-validacao.csv`

## Padrão Proposto

### Camada Silver

- Grão sempre explícito no `description` do modelo.
- Nomes de campos devem usar snake_case e o glossário como referência.
- Campos mínimos recomendados quando aplicáveis:
  - chave: `apf`, `contrato`, `id_*`;
  - localização: `municipio`, `uf`, `codigo_ibge_municipio`;
  - volume: `quantidade_uh`, `quantidade_uh_entregues`;
  - financeiro: `valor_contratado`, `valor_desembolsado`;
  - evolução: `percentual_execucao_fisica`, `percentual_execucao_financeira`;
  - tempo: `dt_referencia`, `dt_contratacao`, `dt_ingest`, `dt_silver`;
  - status: `status_operacional`.
- Chaves primárias/lógicas devem ter `not_null` e `unique` quando o grão for 1:1.
- Datas obrigatórias devem ter `not_null` quando a fonte garantir preenchimento.
- Códigos com zeros à esquerda devem ser `text`.

### Marts de Dashboard

- Marts devem declarar consumidor no `meta`, por exemplo `consumidor: dashboard`.
- Marts devem declarar o grão: nacional, UF, município, empreendimento, APF, entidade, mês etc.
- Campos usados diretamente em Superset devem ser estáveis; renome físico só com view/alias de compatibilidade.
- Métricas derivadas devem documentar fórmula no `schema.yml`.

## Fora do Padrão Identificado

- `empreendimento_far_dbt/silver/empreendimento.sql` e `evolucao_financeira.sql` não têm prefixo de camada no nome físico, enquanto Conjuntura usa `silver_*`.
- FAR e Entidades usam schemas por domínio (`empreendimento_far`, `entidades_fds`) para silver e gold, enquanto Conjuntura separa `conjuntura_silver` e `conjuntura_gold`.
- Campos equivalentes têm nomes diferentes: `cod_ibge`, `cod_municipio_ibge`, `codigo_ibge_do_municipio`, `co_municipio_ibge`.
- Status equivalentes aparecem como `situacao_empreendimento`, `situacao_gefus`, `status_execucao`, `status_operacional`.
- Campos técnicos `dt_ingest`, `dt_silver` e `dt_gold` ainda não aparecem de forma uniforme em todos os modelos.
- Marts de dashboard usam nomes como `*_chart`, `ficha_*`, `panorama_*`, `resumo_*`, mas não há marcação uniforme de consumidor.

## Ajuste Aplicado Nesta Entrega

- Criado glossário inicial MCID.
- Criadas evidências de inventário, matriz semântica e priorização.
- Reforçado teste de chave em `empreendimento_far_dbt/silver/schema.yml`:
  - `empreendimento.apf`: `unique` e `not_null`.
- Criado módulo dbt `mcmv_silver_dbt`, agora condicionado ao target DuckDB para evitar execução indevida lendo Postgres.
- Criadas bases padronizadas por frente:
  - `silver_mcmv_minha_casa_minha_vida_base`
  - `silver_mcmv_far_base`
  - `silver_mcmv_entidades_base`
  - `silver_mcmv_rural_base`
  - `silver_mcmv_classe_media_base`
  - `silver_mcmv_cidades_base`
  - `silver_mcmv_reforma_base`
  - `silver_mcmv_conjuntura_base`
  - `silver_mcmv_pro_moradia_base`
  - `silver_mcmv_sub50_base`
  - `silver_mcmv_frentes_base`
- Rural/PNHR foi tratado explicitamente como fonte com separador pipe nas tabelas `INT057` e `INT065`.
- Dados GEAVO/FGTS foram incorporados nas frentes Classe Media/Faixa 3, Reforma Casa Brasil e Conjuntura FGTS.
- SUB50/FNHIS foi conectado ao contrato padronizado por meio das fontes `novo_mcmv_fnhis_sub_50_propostas_apresentadas` e `novo_mcmv_fnhis_sub_50_propostas_selecionadas`, localizadas no inventario MinIO e esperadas em `__dados_brutos`.
- Pro-Moradia permanece como lacuna controlada porque o inventario local ainda nao encontrou tabela para `pro_moradia`, `promoradia` ou variacoes de `moradia`.

### Evidencia de Fontes no Postgres

- `empreendimento_far.empreendimento`: 1.646 registros.
- `entidades_fds.fds_empreendimento`: 343 registros.
- `conjuntura_silver.silver_fgts_financiamentos_habitacionais`: 47 registros.
- `sftp.int057_ministeriocidades_pnhr_bb_empreendimentos_20241031`: 1.087 linhas; 1.085 com separador pipe completo.
- `sftp.int065_ministeriocidades_pnhr_caixa_empreendimentos_20240830`: 8.419 linhas; 8.389 com separador pipe completo.
- `sftp.pmcmv_faixa3_mcid_2026_06_26`: 110.979 registros com contrato.
- `sftp.pmcmv_reformas_mcid_2026_06_26`: 100.859 registros com contrato.
- `sftp.pmcmv_cidades_mcid_2026_03_01`: 197 registros com contrato.

## Onde Ver no DBeaver

Depois do `dbt run`, abrir a conexão do banco `cidades` e navegar em:

```text
Databases > cidades > Schemas > mcmv_silver > Tables
```

As "pastas" por frente existem no repositório em `models/mcmv_silver_dbt/silver/<frente>/`.
No Postgres/DBeaver elas aparecem como tabelas dentro do schema `mcmv_silver`.

## Priorização

1. P0: FAR e Entidades, porque já alimentam dashboards operacionais.
2. P1: Indicadores MCMV, porque dependem das golds de FAR/FDS e devem herdar o padrão.
3. P1: Conjuntura, porque tem maior volume de modelos e já separa schemas por camada.
4. P2: Renomeações físicas, sempre com compatibilidade para Superset.

## Validação Recomendada

```bash
cd airflow_lappis/dags/dbt/mcid
dbt run --select mcmv_silver_dbt
dbt test --select silver_mcmv_frentes_base
```

Para validar as silvers legadas usadas como fonte:

```bash
dbt run --select empreendimento_far_dbt.silver entidades_dbt.silver
dbt test --select empreendimento_far_dbt.silver entidades_dbt.silver
dbt docs generate
```

Antes de qualquer renome físico usado por dashboard:

```bash
dbt run --select +<mart_dashboard>
dbt test --select +<mart_dashboard>
```

Também comparar no Superset:

- row count antes/depois;
- existência dos campos usados por cards, filtros, mapas e tabelas;
- atualização da data máxima de referência;
- cards principais sem erro.
