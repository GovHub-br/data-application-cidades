# Issue #118 — Entrega Final: Estrategia para Dados Historicos

## Resumo para colar na issue

Foi definida e implementada a primeira versao da estrategia de dados historicos
do MCMV. A entrega documenta bases prioritarias, granularidade, retencao,
versionamento, campos tecnicos de auditoria, regra de reprocessamento e uma base
piloto dbt para validar consultas temporais e backtest do relogio/alertas.

## Decisao de Estrategia

O padrao adotado e:

1. Preservar arquivos historicos no MinIO em `raw/` e `staging/`.
2. Usar `snapshot_date`, `source_file` e `source_path` como rastreio minimo da origem.
3. Criar `hash_linha` com os campos de negocio relevantes para detectar mudancas.
4. Manter historico por versao com `dt_valid_from`, `dt_valid_to` e `is_current`.
5. Reprocessar por snapshot sem sobrescrever evidencia anterior.
6. Expor para silver/gold apenas registros tratados, com chave logica estavel.

## O que foi implementado

Foi criado o modulo dbt:

```text
models/mcmv_historico_dbt/
```

Com a base piloto:

```text
mcmv_historico.historico_mcmv_serie_temporal_snapshot
```

E o seed de evidencia:

```text
seeds/mcmv_historico/issue_118_mcmv_serie_temporal_piloto.csv
```

A base piloto gera uma linha por ano e linha historica:

- `OGU/Subsidiado`
- `FGTS/Financiado`

Essa separacao e importante porque a serie mostra queda/hiato em OGU/subsidiado
em parte da janela recente, mas FGTS/financiado continua com volume relevante.
O dashboard nao deve misturar essas leituras.

## Campos Tecnicos Padronizados

- `id_historico_snapshot`
- `id_negocio_historico`
- `dt_referencia`
- `snapshot_date`
- `source_file`
- `source_path`
- `hash_linha`
- `dt_ingest`
- `dt_valid_from`
- `dt_valid_to`
- `is_current`
- `estrategia_versionamento`
- `regra_retencao`

## Evidencias no Repositorio

- `models/docs/evidencias/issue-118-bases-prioritarias-historico.csv`
- `models/docs/evidencias/issue-118-campos-tecnicos-historico.csv`
- `models/docs/evidencias/issue-118-reprocessamento-piloto.csv`
- `models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-uh-contratadas.csv`
- `models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-ogu-barras-fgts-linha.png`
- `models/mcmv_historico_dbt/piloto/historico_mcmv_serie_temporal_snapshot.sql`
- `models/mcmv_historico_dbt/piloto/schema.yml`
- `tests/mcmv_historico/`

## Bases Prioritarias Mapeadas

- `raw/dados_historicos`
- `novo_mcmv_far_*`
- `novo_mcmv_fds_*`
- `novo_mcmv_rural_*`
- `novo_mcmv_fnhis_sub_50_*`
- `PMCMV_FAIXA3_MCID_*`
- `PMCMV_REFORMAS_MCID_*`
- `dados_abertos_mcmv_ogu_empreendimentos`
- `dados_abertos_mcmv_fgts_sintetico`

Retencao recomendada: indeterminada para bases oficiais e evidencias
historicas usadas em auditoria, backtest, comparacao temporal e reprocessamento.

## Testes Criados

Testes genericos no `schema.yml`:

- `not_null`
- `unique`
- `accepted_values`

Testes singulares:

- `assert_historico_sem_duplicidade_corrente.sql`
- `assert_historico_janela_temporal_valida.sql`
- `assert_historico_quantidade_nao_negativa.sql`

## Como Rodar

```bash
cd airflow_lappis/dags/dbt/mcid
dbt build --select issue_118_mcmv_serie_temporal_piloto+ historico_mcmv_serie_temporal_snapshot
dbt test --select historico_mcmv_serie_temporal_snapshot
```

Para rodar apenas parsing/validacao de projeto:

```bash
dbt parse --profiles-dir .
```

## Criterios de Aceite

| Criterio | Status |
|---|---|
| Bases prioritarias e requisitos historicos mapeados | Atendido |
| Estrategia de retencao e versionamento definida | Atendido |
| Base piloto implementada | Atendido |
| Consultas historicas e reprocessamento testaveis | Atendido por modelo e testes dbt |
| Padrao documentado para reutilizacao | Atendido |

## Observacao

A base piloto usa a serie historica anual ja validada na entrega dos indicadores.
O proximo passo natural e aplicar o mesmo contrato historico nas fontes
operacionais por APF/contrato, especialmente FAR, Entidades, Rural, Reforma,
Classe Media e SUB50/FNHIS.
