# Issue #130 — Proposta: novas camadas bronze a partir de `staging/dados_historicos`

> **SUPERSEDED (2026-09-02):** implementado. Ver
> `issue-130-entrega-series-historicas-tier1-tier2.md`. Este documento fica como
> registro do desenho inicial.
>
> Data: 2026-09-02. Rascunho para revisão. Depende do re-tratamento do
> `staging/dados_historicos` estar concluído.
> Contexto: `issue-130-refatoracao-medalhao-reloginho.md`, `arquitetura-medalhao-mcid.md`.

Hoje só a família `historico_recente_*` (contratação SNH) alimenta o bronze do
reloginho. Esta proposta cobre duas fatias adicionais do dump histórico.

---

## Tier 1 — Entregas por evento SNH

**Objetivo:** caminho alternativo do total de entregas (decisão #5: 1.518.598 vs
1.543.432) e base do `ritmo_recente` (fluxo mensal real, não o acumulado).

**Fontes** (`staging/dados_historicos/`, 2024-06 → 2026-03):

| Arquivo | Colunas úteis |
|---|---|
| `o_recente_YYYYMM_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | `apf`, `dt_entrega`, `qt_uh_entregues` |
| `_YYYYMM_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` (+ truncados `024_10_…`) | `apf`, `dt_ass_doc`, `numero_de_unidades_entregues` |

### `models/indicadores_mcmv_dbt/bronze/bronze_reloginho_snh_entregas_evento.sql`

```sql
{{ config(materialized="table") }}

-- BRONZE do reloginho — entregas por EVENTO (fluxo), CAIXA + BB.
-- Complementa bronze_reloginho_snh_serie_mensal (que traz o acumulado).
-- Grão: um evento de entrega (agente, apf, dt_evento).
-- Fontes: o_recente_*_entregas (CAIXA) e *_entrega_da_unidade_af_b (BB).
-- Target obrigatório: staging_duckdb.

with fonte as (
    select
        *,
        filename as source_file,
        strptime(
            regexp_extract(
                regexp_replace(filename, '(20\d{2})_(\d{2})', '\1\2'),
                '(\d{6})', 1
            ), '%Y%m'
        )::date as dt_referencia,
        case
            when lower(filename) like '%af_caixa%' then 'CAIXA'
            when lower(filename) like '%af_b%'     then 'BB'
        end as agente_arquivo,
        current_timestamp as dt_ingest
    from {{ read_minio_staging_parquet_series(
        'dados_historicos/*snh_pmcmv_dados_prioritarios*entrega*.parquet'
    ) }}
)

select
    *,
    -- helpers harmonizados (a tipagem/normalização fina fica na silver)
    coalesce(dt_entrega, dt_ass_doc)                    as dt_evento_raw,
    coalesce(qt_uh_entregues, numero_de_unidades_entregues) as qt_uh_entregues_raw,
    md5(concat_ws('|',
        coalesce(agente_financeiro::text, agente_arquivo, ''),
        coalesce(apf::text, ''),
        coalesce(dt_entrega::text, dt_ass_doc::text, ''),
        coalesce(qt_uh_entregues::text, numero_de_unidades_entregues::text, '')
    )) as hash_linha
from fonte
```

> **Validar:** o glob `*…dados_prioritarios*entrega*` pega as duas famílias e NÃO
> pega `historico_recente_*` (sem "entrega" no nome) — conferir após o
> re-tratamento. `union_by_name` cobre o fato de CAIXA ter `dt_entrega`/`qt_uh_entregues`
> e BB ter `dt_ass_doc`/`numero_de_unidades_entregues`.

### Silver: `silver_reloginho_snh_entregas_mes.sql` (esboço)

```sql
-- Agrega o fluxo por (agente, apf, mês de dt_evento), deduplicando eventos
-- repetidos por hash_linha, e produz a entrega LÍQUIDA do mês por APF.
with base as (select * from {{ ref('bronze_reloginho_snh_entregas_evento') }}),
tipado as (
    select
        coalesce(upper(nullif(trim(agente_financeiro::text),'')), agente_arquivo) as agente_financeiro,
        nullif(trim(apf::text),'')                       as apf,
        try_cast(dt_evento_raw as date)                  as dt_evento,
        date_trunc('month', try_cast(dt_evento_raw as date))::date as mes_evento,
        try_cast(nullif(trim(qt_uh_entregues_raw::text),'') as bigint) as qt_uh_entregues,
        hash_linha
    from base
    where nullif(trim(apf::text),'') is not null
),
dedup as (
    select *, row_number() over (partition by hash_linha order by dt_evento) as rn
    from tipado
)
select
    agente_financeiro, apf, mes_evento,
    sum(qt_uh_entregues) as uh_entregues_evento_mes,
    count(*)             as n_eventos
from dedup
where rn = 1 and mes_evento is not null
group by agente_financeiro, apf, mes_evento
```

Depois: um gold `indicadores_reloginho_entregas_evento` (soma por agente/mês) e o
cruzamento com `indicadores_reloginho` para expor os DOIS totais lado a lado até a
área decidir o oficial (decisão #5).

---

## Tier 2 — Série executiva histórica (pré-2024)

**Objetivo:** estender contratadas/entregues/execução/split OGU-FGTS de 2024-06
para trás (~2010-2018), por UF/município/faixa, e **aposentar o seed do piloto
#118** (`issue_118_mcmv_serie_temporal_piloto`).

**Fontes** (`staging/dados_historicos/`):

| Família | Glob aprox. | Grão | Período |
|---|---|---|---|
| `bases_relatório_executivo` | `*bases_relat*rio_executivo*` | empreendimento | ~2011-2018 |
| `min_cidades` (pj/pf/pnhr) | `*min_cidades*` | empreendimento (pj) / contrato (pf) / empreendimento (pnhr) | 2011-2018 |
| `entrada_bb` | `*entrada_bb*` | empreendimento BB | 2010-2014 |
| `bext` (CAIXA base extrato) | `*bext*` | agregado UF×faixa×ano-mês | 2012-2018 |

### Mapa de colunas → contrato comum

| Contrato comum | `bases_relatório_executivo` | `min_cidades_pj` | `min_cidades_pf_pf` | `pnhr` | `entrada_bb` | `bext` |
|---|---|---|---|---|---|---|
| `uh_contratadas` | `unidades` \| `uh` | `qtd_unidade_habitacional` | `qtd_uh` | `qt_und_eprd` | `qde_unidades` | `uh` |
| `uh_entregues` | `unidades_entregues` \| `iqde_unidades_entregues` | `qtd_unidade_entregue` | `qtd_entregue` | — | `entrega_do_empreendimento` (?) | `entregues` |
| `uh_concluidas` | `unidades_concluidas` \| `uh_concluidas` | `qtd_unidade_concluida` | `qtd_concluida` | — | — | `uh_concluidos` |
| `uh_em_obras` | `unidades_em_obras` \| `uh_em_obras` | — | — | — | — | `uh_em_obras` |
| `valor_investimento` | `valor_total_do_investimento` | `vlr_emprestimo` | `vlr_financiamento` | `vlr_emprestimo` | `valor_global_de_venda_vgv` | `valor_do_emprestimo` |
| `valor_liberado` | `valor_total_liberado` | `vlr_total_liberado` | — | — | — | — |
| `subsidio_fgts` | `siaci_valorsubsidio_fgts` \| `subsidio_fgts` | — | `vlr_subsidio_fgts` | — | `complemento_fgts` | — |
| `subsidio_ogu` | `siaci_valorsubsidio_ogu` \| `subsidio_ogu` | — | `vlr_subsidio_ogu` | — | `complemento_ogu` | — |
| `percentual_execucao_fisica` | `de_obra_executada` | `prc_execucao_obra` | — | `acompanhmento_de_obra` | `percentual_de_obra` | `obra` |
| `faixa` | `faixa` | `num_faixa` | `num_faixa` | `faixa` | `publico_alvo` (?) | `faixa` |
| `codigo_ibge_municipio` | `codmunicibge` \| `cod_munic_ibge` | `cod_municipio` | `cod_municipio` | `cd_mun_ibge` | `codigo_do_ibge` | — |
| `uf` | `uf` | — (derivar do IBGE) | — (derivar) | `sg_uf` | `uf` | `uf` |
| `apf` | `codapf` \| `cod_apf` | `cod_empreendimento` | `cod_contrato` | `cod_eprd` \| `nr_prpt` | `contrato_bb` \| `contrato_caixa` | — |
| `dt_contratacao` | `data_contratacao` \| `data_prevista_termino_obra` (?) | `dat_contratacao` | `dat_contratacao` | `dt_ass_eft_ctr` | `data_da_contratacao_bb` | — |
| `dt_referencia` | nome do arquivo / `report_date` | idem | idem | idem | idem | `anomes` |

`—` = ausente; `(?)` = a confirmar contra o header do ano.

### `models/mcmv_historico_dbt/serie_executiva/bronze_mcmv_serie_executiva_historica.sql` (esqueleto)

```sql
{{ config(materialized="table") }}

-- BRONZE — série executiva histórica MCMV (pré-2024), cópia fiel por família.
-- SEM harmonização aqui (bronze = cru); o contrato comum é montado na silver
-- silver_mcmv_serie_executiva_historica a partir do mapa de colunas do doc
-- issue-130-proposta-bronze-series-historicas.md.
-- Grão: 1 linha por linha de origem. dt_referencia derivada do nome do arquivo.
-- Target obrigatório: staging_duckdb.

with

bases_rel as (
    select *, 'bases_relatorio_executivo' as fonte_familia, filename as source_file
    from {{ read_minio_staging_parquet_series('dados_historicos/*bases_relat*rio_executivo*.parquet') }}
),
min_cidades as (
    select *, 'min_cidades' as fonte_familia, filename as source_file
    from {{ read_minio_staging_parquet_series('dados_historicos/*min_cidades*.parquet') }}
),
entrada_bb as (
    select *, 'entrada_bb' as fonte_familia, filename as source_file
    from {{ read_minio_staging_parquet_series('dados_historicos/*entrada_bb*.parquet') }}
),
bext as (
    select *, 'bext' as fonte_familia, filename as source_file
    from {{ read_minio_staging_parquet_series('dados_historicos/*bext*.parquet') }}
),

unido as (
    select * from bases_rel
    union all by name
    select * from min_cidades
    union all by name
    select * from entrada_bb
    union all by name
    select * from bext
)

select
    *,
    strptime(
        regexp_extract(regexp_replace(source_file, '(20\d{2})_(\d{2})', '\1\2'), '(\d{6})', 1),
        '%Y%m'
    )::date as dt_referencia,
    try_cast(nullif(trim(report_date::text), '') as date) as report_date_parsed,
    current_timestamp as dt_ingest,
    md5(source_file || '|' || coalesce(content_hash::text, '') || '|' || rowid::text) as hash_linha
from unido
```

> **Riscos conhecidos (por isso é esqueleto):**
> - `UNION ALL BY NAME` sobre famílias com dezenas de colunas distintas gera uma
>   tabela larga e esparsa — aceitável para bronze, mas conferir o custo.
> - Se **nenhum** arquivo de uma família tiver uma coluna referenciada, o
>   `read_parquet` falha — as CTEs usam só `select *`, então o risco fica todo na
>   silver (onde o `coalesce` do mapa precisa de guarda por família).
> - Muitos arquivos são **recargas semanais** do mesmo mês (`_v2`, `_corrigido`,
>   `_reprocessado`) → dedup por (família, mês, apf) com prioridade de versão na
>   silver, como no reloginho.
> - `min_cidades` e `bext` sem `uf` → derivar de `codigo_ibge_municipio` via
>   `seed`/`referencia_ibge`.
> - `entrada_bb.entrega_do_empreendimento` pode ser data, não contagem — validar.
> - Nomes de família com mojibake (`relat_rio` vs `relatório`) — o glob usa `*`
>   entre os trechos; confirmar após o re-tratamento canônico.

### Silver: `silver_mcmv_serie_executiva_historica.sql` (esboço)

```sql
-- Aplica o mapa de colunas (doc issue-130-proposta-bronze-series-historicas.md),
-- um bloco por fonte_familia, montando o contrato comum. Tipagem BR + dedup.
with b as (select * from {{ ref('bronze_mcmv_serie_executiva_historica') }}),

bases_rel as (
    select
        'Minha Casa Minha Vida'::text as programa,
        fonte_familia,
        nullif(trim(coalesce(codapf, cod_apf)::text), '')            as apf,
        upper(nullif(trim(uf::text), ''))                            as uf,
        nullif(trim(coalesce(codmunicibge, cod_munic_ibge)::text),'') as codigo_ibge_municipio,
        nullif(trim(faixa::text), '')                                as faixa,
        try_cast(nullif(trim(coalesce(unidades, uh)::text), '') as bigint)                       as uh_contratadas,
        try_cast(nullif(trim(coalesce(unidades_entregues, iqde_unidades_entregues)::text),'') as bigint) as uh_entregues,
        try_cast(nullif(trim(coalesce(unidades_concluidas, uh_concluidas)::text),'') as bigint) as uh_concluidas,
        {{ 'parse_br_number' }}(coalesce(siaci_valorsubsidio_fgts, subsidio_fgts)) as subsidio_fgts,
        {{ 'parse_br_number' }}(coalesce(siaci_valorsubsidio_ogu, subsidio_ogu))   as subsidio_ogu,
        dt_referencia, report_date_parsed, source_file, hash_linha
    from b where fonte_familia = 'bases_relatorio_executivo'
),
min_cidades as ( /* ... bloco análogo com qtd_unidade_* / vlr_* ... */ select null limit 0 ),
entrada_bb  as ( /* ... */ select null limit 0 ),
bext        as ( /* ... agregado: sem apf, grão uf×faixa×mês ... */ select null limit 0 ),

uniao as (
    select * from bases_rel
    -- union all by name ... (demais blocos quando prontos)
),
dedup as (
    select *, row_number() over (
        partition by fonte_familia, apf, dt_referencia
        order by report_date_parsed desc nulls last, source_file
    ) as rn
    from uniao
)
select * exclude (rn) from dedup where rn = 1
```

Depois, a silver do reloginho (`silver_reloginho_snh_apf_mes`) ganha um
`union all` com esta série para `dt_referencia < 2024-06`, e o piloto #118
(`historico_mcmv_serie_temporal_snapshot`) passa a ler daqui (`sum(subsidio_ogu>0)`
→ linha OGU/Subsidiado; `subsidio_fgts>0` → FGTS/Financiado) em vez do seed.

---

## Config `dbt_project.yml`

```yaml
    indicadores_mcmv_dbt:
      bronze:
        # bronze_reloginho_snh_serie_mensal + bronze_reloginho_snh_entregas_evento
        +enabled: "{{ target.type == 'duckdb' }}"

    mcmv_historico_dbt:
      serie_executiva:
        +materialized: table
        +schema: mcmv_historico
        +enabled: "{{ target.type == 'duckdb' }}"
```

## Questões em aberto

1. **Prioridade:** Tier 1 é barato e desbloqueia a decisão #5. Tier 2 é grande
   (drift de schema em ~15 anos) — vale só se a área precisar de série longa /
   backtest / OGU-FGTS. Confirmar com negócio antes de investir.
2. **Re-tratamento:** ambos dependem de `staging/dados_historicos` já canônico
   (< 500 parquets) e da resolução dos 8 arquivos que falharam a conversão.
3. **`entrada_bb.entrega_do_empreendimento`** — data ou contagem?
4. **Macro `parse_br_number`** — não existe ainda para o target DuckDB; hoje o
   parse BR é inline (`replace(replace(x,'.',''),',','.')`). Criar a macro ou
   repetir inline.
5. **Faixa 3 / PMCMV-3** (`pmcmv_3_relatório_executivo`, 2015-2018) — entra numa
   terceira família ou fica para o grupo C?
