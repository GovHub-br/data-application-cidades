# Cobertura documental dos modelos MCID

Auditoria realizada em 2026-07-19 sobre todos os modelos SQL em
`airflow_lappis/dags/dbt/mcid/models`.

## Critério de aprovação

Um modelo entra nas recipes de Postgres somente quando atende a todos os itens:

1. possui entrada em um `schema.yml` dbt `version: 2`;
2. possui descrição substantiva do modelo, com pelo menos 80 caracteres;
3. possui tags de camada, domínio e `mcid`;
4. possui `meta.openmetadata` com tier e certificação coerentes com a camada;
5. as colunas documentadas no YAML coincidem exatamente com a projeção SQL final;
6. 100% das colunas possuem descrição não vazia;
7. não possui placeholders como `TODO`, `TBD`, `a definir` ou `sem descrição`.

## Resultado

| Schema físico | Modelos auditados | Aprovados | Colunas documentadas |
|---|---:|---:|---:|
| `conjuntura_bronze` | 11 | 11 | 99/99 |
| `conjuntura_silver` | 19 | 19 | 224/224 |
| `conjuntura_gold` | 20 | 20 | 200/200 |
| `empreendimento_far` | 13 | 13 | 308/308 |
| `entidades_fds` | 11 | 11 | 384/384 |
| `metadata` | 1 | 0 | 0/7 |
| **Total** | **75** | **74** | **1.215/1.222** |

- Cobertura de modelos: **74/75 (98,7%)**.
- Cobertura global de colunas: **1.215/1.222 (99,4%)**.
- Cobertura dentro da allowlist aprovada: **1.215/1.215 (100%)**.
- Não foram encontradas entradas YAML órfãs, colunas duplicadas ou divergências
  entre a projeção SQL final e o YAML nos modelos aprovados.

## Modelos aprovados

### `conjuntura_bronze`

`bronze_abecip_poupanca_sbpe`, `bronze_bacen_financiamentos_imobiliarios`,
`bronze_fgv_icst`, `bronze_fgv_incc_m`, `bronze_fipezap_locacao`,
`bronze_ibge_pib_construcao_civil`, `bronze_ibge_pnadc_ocupados_construcao`,
`bronze_ibge_pnadc_rendimento_construcao`, `bronze_ibge_sinapi`,
`bronze_imob_infomoney`, `bronze_novo_caged`.

### `conjuntura_silver`

`silver_abecip_novos_financiamentos_imobiliarios`,
`silver_abecip_poupanca_sbpe`,
`silver_abecip_sbpe_financiamentos_habitacionais`, `silver_abramat_indice`,
`silver_bacen_financiamentos_imobiliarios`, `silver_balancos_empresas`,
`silver_cbic_lancamentos_vendas`, `silver_fgts_financiamentos_habitacionais`,
`silver_fgv_icst`, `silver_fgv_incc_m`, `silver_financiamentos_habitacionais`,
`silver_fipezap_locacao`, `silver_ibge_pib_construcao_civil`,
`silver_ibge_pnadc_ocupados_construcao`,
`silver_ibge_pnadc_rendimento_construcao`, `silver_ibge_sinapi`,
`silver_imob_infomoney`, `silver_novo_caged`, `silver_ticket_medio_empresas`.

### `conjuntura_gold`

`gold_abecip_novos_financiamentos_imobiliarios`,
`gold_balancos_empresas_lancamentos`, `gold_balancos_empresas_vendas`,
`gold_cbic_lancamentos`, `gold_cbic_lancamentos_regiao`, `gold_cbic_vendas`,
`gold_cbic_vendas_regiao`, `gold_empregos_construcao`,
`gold_fgts_renda_familiar`, `gold_financiamento_pf_por_faixa`,
`gold_financiamentos_habitacionais`, `gold_financiamentos_imobiliarios`,
`gold_ibge_pnad_construcao`, `gold_indicadores_financiamento_pf`,
`gold_indices_mercado_imobiliario`, `gold_pib_construcao_civil`,
`gold_precos_construcao`, `gold_saldo_caderneta_poupanca`,
`gold_ticket_medio_vs_incc`, `gold_uh_condicao_uso`.

### `empreendimento_far`

`cadastro_pj`, `consolidado`, `dados_prioritarios_caixa`, `financeiro_mensal`,
`obra_mensal`, `empreendimento`, `evolucao_financeira`,
`evolucao_financeira_chart`, `execucao_fisica_financeira_chart`,
`ficha_empreendimento`, `mapa_nacional`, `panorama_estadual`,
`resumo_gerencial`.

### `entidades_fds`

`fds_cadastro_pj`, `fds_dados_prioritarios_entregas`,
`fds_financeiro_mensal`, `fds_int_059_caixa_pj`, `fds_obra_mensal`,
`fds_trabalho_social`, `fds_empreendimento`, `fds_evolucao_financeira`,
`fds_evolucao_financeira_chart`, `fds_ficha_empreendimento`,
`fds_panorama_entidade`.

## Modelo excluído

`metadata.models_metadata` ficou fora das três recipes porque não possui entrada
em `schema.yml`, descrição de modelo, tags, `meta.openmetadata` nem documentação
para suas sete colunas: `schema_name`, `table_name`, `database_name`,
`materialization`, `description`, `dt_transform` e `run_id`.

As allowlists exatas estão replicadas em `postgres_metadata.yaml`,
`postgres_profiler.yaml` e `postgres_classifier.yaml` e devem permanecer
idênticas.
