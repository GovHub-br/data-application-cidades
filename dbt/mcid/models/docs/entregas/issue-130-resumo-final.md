# Issue #130 - Resumo Final Consolidado

> **Atualizacao 2026-09-02:** apos este resumo, o reloginho (grupo A) foi
> refatorado para aderir a arquitetura medalhao — o gold que lia parquet direto
> virou bronze -> silver -> gold (`bronze_reloginho_snh_serie_mensal` ->
> `silver_reloginho_snh_apf_mes` -> `indicadores_reloginho` +
> `indicadores_reloginho_frente` + `resumo_reloginho_dashboard`). Tambem foi
> verificada a cobertura historica mensal por frente (FAR / Entidades / Rural).
> Docs: `issue-130-aderencia-arquitetura-medalhao-reloginho.md`,
> `issue-130-refatoracao-medalhao-reloginho.md`,
> `../arquitetura-medalhao-mcid.md`.

## Resumo executivo

A issue #130 ("Validar indicadores dos dados historicos para o reloginho") foi
atacada em frentes de documentacao, validacao empirica e implementacao de
modelagem. Abaixo o consolidado do que foi entregue, validado e o que permanece
pendente.

O trabalho se dividiu em:

1. **Fase 0-1** — dicionario de indicadores + matriz indicador x fonte x campo x regra.
2. **Checklist** — 19 decisoes de negocio para validar com a area.
3. **Fases 2-4** — cobertura historica, regras de calculo e calculos em amostra (com acesso ao banco `cidades` via VPN).
4. **Estrategia APF x fases** — definicao + revisao pelo @oracle + implementacao dbt.

## O que foi entregue (artefatos)

### Documentos (`models/docs/entregas/` e `models/docs/evidencias/`)

| Artefato | Conteudo |
|---|---|
| `issue-130-dicionario-indicadores.md` | Dicionario com 10 indicadores do reloginho (grupo A) + 9 de gargalo (grupo B), cada um com os 14 campos pedidos na issue |
| `evidencias/issue-130-matriz-indicador-fonte-campo-regra.csv` | Matriz compacta indicador x fonte x tabela x campos x regra (19 indicadores) |
| `issue-130-checklist-validacao-negocio.md` | Checklist de 19 decisoes de negocio em 4 blocos (metas, regras do reloginho, limiares do gargalo, outras) |
| `evidencias/issue-130-decisoes-pendentes-validacao.csv` | Planilha das 19 decisoes para registro das respostas da area |
| `issue-130-estrategia-apf-fases.md` | Estrategia de identidade de empreendimento (APF variavel por fase), revisada pelo @oracle |
| `issue-130-validacao-tecnica-fases-2-4.md` | Validacao tecnica (cobertura, regras, calculos + comparacao com referencia #66) |

### Implementacao dbt (estrategia APF/fases)

| Arquivo | Tipo | Descricao |
|---|---|---|
| `seeds/entidades_fds/seed_apf_fase_fds.csv` | seed | Mapeamento curado APF x fase (888 empreendimentos, 168 multi-fase) |
| `entidades_dbt/bronze/fds_mudanca_fase_eventos.sql` | novo | Eventos de mudanca de fase (`ic_mudanca_fase`) |
| `entidades_dbt/silver/dim_empreendimento.sql` | novo | Dimensao 1:N que resolve `id_empreendimento` (hash do APF-ancora) |
| `entidades_dbt/bronze/fds_int_059_caixa_pj.sql` | mod | + `apf_nao_obra` (vinculo de fase do INT059) |
| `entidades_dbt/silver/fds_empreendimento.sql` | mod | + `id_empreendimento` + `fase_empreendimento` |
| `mcmv_silver_dbt/.../silver_mcmv_entidades_base.sql` | mod | Agrupa por `id_empreendimento` (regra UH/financeiro = `max`) |
| `entidades_dbt/{bronze,silver}/schema.yml` | mod | Docs + testes (not_null, unique, accepted_values) |
| `dbt_project.yml` | mod | Config do seed `entidades_fds` |
| `tests/entidades_dbt/` (4 arquivos) | novo | Testes singulares de integridade da dim |

## Validacoes empiricas realizadas (banco `cidades`, VPN)

| # | Validacao | Resultado |
|---|---|---|
| 1 | Deduplicacao das tabelas prioritarias | **2x duplicacao por APF** (CAIXA 29.846->14.923; BB 2.576->1.288). `sum` sem dedup DOBRA os totais. |
| 2 | Consistencia com referencia #66 | Dedup jan/2026 (contratadas 1.857.372; entregues 1.534.167) coerente com jun/2026 (1.874.623 / 1.543.432) |
| 3 | Cobertura temporal | Anual 2009-2025 (contratadas OGU/FGTS); **mensal 2024-06 a 2026-03** (contratadas + entregues, CAIXA+BB, por APF/UF/municipio) |
| 4 | Regra de UH entre fases | **UH duplicadas, nao particionadas** -> `max` correto (19 pares iguais, 0 diferentes, 106 so-obra) |
| 5 | Regra financeira entre fases | **Totais unicos por empreendimento** -> `max` correto (`vr_investimento > vr_projeto + vr_obra` em 125/125) |
| 6 | Fonte do vinculo de fase | INT059 `nu_apf_nao_obra` existe **so no legado** (125 PMCMV-E, 0 NOVO); seed xlsx cobre o mapeamento completo |
| 7 | Materializacao no banco | Presente: FAR/FDS/Rural/Conjuntura/mcmv_silver + **mcmv_indicadores (gargalo) materializado** (1.989 linhas). Ausente: **mcmv_historico (piloto #118)** (seed-based, aguarda staging dados_historicos) |
| 8 | Teste `unique` do gargalo | **FAIL** `unique id_indicador` (823 duplicatas): FAR `ficha_empreendimento` com 2x exata por APF (1.646 linhas/823 APFs). FDS limpa (343/343). Totais FAR dobrados. Detalhe em `issue-130-validacao-tecnica-fases-2-4.md`. |

## Decisoes de negocio pendentes (para a area - Fase 5)

| # | Decisao | Default recomendado |
|---|---|---|
| 1 | Meta oficial de UHs do ciclo (so existe "meta visual" 2.214.810) | Definir tabela oficial de metas |
| 2 | Meta por frente/programa/ano | Definir |
| 3 | Regra FNHIS/SUB50 (proposta vs contrato vs UH) | A definir com a area |
| 4 | Campo oficial de valor financiado (`vr_evento` vs `vr_investimento`) | A definir |
| 5 | Total de entregas (1.543.432 vs 1.518.598) | A definir caminho oficial |
| 6 | Normalizacao `RURAL`/`Rural` e faixas `001/002/003` | Padronizar |
| 7 | Separacao de dashboards (relogio executivo vs mesa de alertas) | Separar |
| 8 | Fallback de ancora sem Fase Projeto | APF mais antigo |
| 9 | Auto-merge vs curador para `ic_mudanca_fase` | Curador (seed vence) |
| 10 | Schema da dim (`entidades_fds` vs `entidades_fds_ref`) | `entidades_fds` (adotado) |

## Pendencias para fechar

1. **Rodar dbt** — CONCLUIDO (dbt-core 1.12 via venv isolado; seed + entidades_dbt + gargalo + 24 testes OK). Resta `silver_mcmv_entidades_base` (target duckdb, aguarda staging sftp).
2. **Materializar `mcmv_indicadores`** (gargalo) — CONCLUIDO (1.989 linhas). **`mcmv_historico`** (piloto #118) segue pendente (seed-based; aguarda staging dados_historicos).
3. **Reloginho (grupo A) em camadas medalhao** — CONCLUIDO no codigo (bronze/silver/gold, `dbt parse`/`compile` OK). Pendente `dbt run`/`dbt test` da linhagem com credencial MinIO e anexar a matriz `indicadores_reloginho_frente` como evidencia. Ver `issue-130-refatoracao-medalhao-reloginho.md`.
4. **Fase 5** — validar as 10 decisoes acima com a area responsavel.
5. **Fase 6** — documentar regras finais, limitacoes e evidencias de negocio.
6. **Duplicacao 2x por APF na frente FAR** — achado do teste `unique id_indicador` do gargalo (823 duplicatas). `ficha_empreendimento` FAR com 2x exata por APF; FDS limpa. **Levar aos colegas** para decidir onde deduplicar (bronze/silver/gargalo). Registrado em `issue-130-validacao-tecnica-fases-2-4.md`. (No reloginho a dedup 2x por APF ja e neutralizada na `silver_reloginho_snh_apf_mes`.)

## Como validar a implementacao

```bash
cd airflow_lappis/dags/dbt/mcid
dbt seed --select seed_apf_fase_fds
dbt run --select dim_empreendimento fds_empreendimento silver_mcmv_entidades_base
dbt test --select dim_empreendimento entidades_dbt
```

Testes de integridade da `dim_empreendimento`:
- `unique`/`not_null` em `apf` e `id_empreendimento`; `accepted_values` em `fase_empreendimento`.
- Singulares: `assert_apf_fase_uniqueness`, `assert_empreendimento_tem_ancora`, `assert_dim_grao_unique`, `assert_cobertura_apf_cadastro`.

## Observacoes importantes

- O seed foi gerado **apenas da aba ABR26** do xlsx (a aba JAN26 usa `FASE_1/2/3` generico, mapeamento nao confiavel).
- O INT059 (`nu_apf_nao_obra`) nao entrou como fonte da dim: os vinculos sao todos legado (redundantes com o seed) e nao ha vinculo no NOVO.
- `id_empreendimento = md5('empreendimento-fds|' || apf_ancora)`, com `apf_ancora` = APF da Fase Projeto (fallback: APF mais antigo).
- SCD2 plena adiada: colunas `dt_valid_from/to`, `is_current`, `hash_linha` herdadas do piloto #118 de forma trivial (v1).
