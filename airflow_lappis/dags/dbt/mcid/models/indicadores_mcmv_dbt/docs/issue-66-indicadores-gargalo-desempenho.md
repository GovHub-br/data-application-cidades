# Issue #66 — Indicadores de Gargalo e Desempenho MCMV

## Objetivo

Criar indicadores para identificar atraso, baixa execução, gargalo financeiro,
gargalo territorial e problemas por responsável nas frentes FAR e FDS do MCMV.

## Entrega Técnica

Foram adicionados dois modelos gold no dbt MCID:

- `mcmv_indicadores.indicadores_gargalo_desempenho`: uma linha por empreendimento/APF.
- `mcmv_indicadores.resumo_gargalo_desempenho_dashboard`: agregações para dashboard por nacional, frente, UF, município e responsável.

Arquivos:

- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/gold/indicadores_gargalo_desempenho.sql`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/gold/resumo_gargalo_desempenho_dashboard.sql`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/gold/schema.yml`

## Bases Usadas

As bases foram conferidas no PostgreSQL `cidades`:

- FAR: `empreendimento_far.ficha_empreendimento`, `empreendimento_far.evolucao_financeira`, `empreendimento_far.execucao_fisica_financeira_chart`.
- FDS: `entidades_fds.fds_ficha_empreendimento`, `entidades_fds.fds_empreendimento`, `entidades_fds.fds_evolucao_financeira_chart`.

## Checagem MinIO

Tentativa somente leitura realizada:

- `MINIO_ENDPOINT=minio:9000`: falhou porque o host `minio` é nome interno do Docker e não resolve no host.
- `localhost:9000`: falhou com conexão recusada.
- `docker-compose.yml`: a composição atual não expõe serviço MinIO nesta branch.
- `docker ps`: não havia container em execução no ambiente local no momento da checagem.

Conclusão: o MinIO não estava acessível diretamente a partir do ambiente atual. A modelagem foi feita sobre as tabelas dbt já materializadas no PostgreSQL, que são as saídas estruturadas das cargas do MCMV. Quando o serviço MinIO estiver exposto, a checagem complementar recomendada é listar o bucket configurado em `MINIO_BUCKET` e comparar prefixos de origem contra as sources do dbt.

## Regras de Indicadores

| Indicador | Regra |
|---|---|
| Obra atrasada | Previsão de conclusão/entrega vencida e execução física menor que 100%. |
| Obra paralisada | Data de paralisação ou situação textual contendo paralisação. |
| Sem atualização recente | Obra não concluída sem liberação/medição há mais de 90 dias ou sem data de atualização. |
| Baixa execução física | Execução física 10 p.p. abaixo do previsto, previsão vencida sem conclusão, ou contrato com mais de 365 dias abaixo de 30% físico. |
| Baixa execução financeira | Execução financeira 10 p.p. abaixo da física, ou contrato com mais de 365 dias abaixo de 30% financeiro. |
| Gargalo financeiro | Pelo menos 30% do contrato ainda não desembolsado e execução física abaixo de 95%. |
| Contrato sem evolução | Contrato com mais de 180 dias sem execução física nem financeira. |
| Entrega em risco | Obra não concluída com atraso, paralisação, baixa execução ou falta de atualização. |

## Score de Gargalo

| Componente | Peso |
|---|---:|
| Atraso | 2 |
| Paralisação | 2 |
| Sem atualização recente | 1 |
| Baixa execução física | 1 |
| Baixa execução financeira | 1 |
| Gargalo financeiro | 1 |
| Contrato sem evolução | 1 |

Classificação:

- `Baixo`: score 0.
- `Médio`: score 1 a 2.
- `Alto`: score 3 a 4.
- `Crítico`: score maior ou igual a 5.

## Uso no Dashboard

Cards recomendados:

- Total de empreendimentos.
- Total de UHs.
- Valor contratado, desembolsado e saldo a desembolsar.
- Percentual de entregas em risco.
- Percentual de casos críticos.
- Atraso médio em dias.

Visuais recomendados:

- Ranking de responsáveis por `qtd_casos_criticos` e `media_score_gargalo`.
- Mapa por UF usando `nivel_agregacao='uf'`.
- Barras por município com `qtd_entregas_em_risco`.
- Série de evolução físico-financeira usando as golds já existentes de evolução.
- Tabela operacional por APF usando `indicadores_gargalo_desempenho`.

Filtros principais:

- `frente`
- `uf`
- `municipio`
- `responsavel_nome`
- `classificacao_gargalo`
- `indicadores_acionados`

## Próxima Validação Recomendada

1. Expor o MinIO para o host ou executar a checagem dentro do container/rede Docker.
2. Rodar:

```bash
cd airflow_lappis/dags/dbt/mcid
dbt run --select indicadores_mcmv_dbt
dbt test --select indicadores_mcmv_dbt
```

3. Validar amostra dos casos `Crítico` com a área de negócio antes de publicar o dashboard.
