{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: FGTS-PJ (Apoio à Produção), via sistema
-- GEAVO da Caixa — substitui o Canal FGTS antigo/CEAG. Página 2/5.
-- Ver docs/conjuntura-fontes-dbt.md (local, fora do repositório) pra
-- detalhes de como essa fonte foi validada.
--
-- cod_linha 33/AK = "HAB / PROG DE APOIO PRODUCAO DE HABITACOES" e sua
-- variante "OPERACOES ESPECIAIS" — as duas juntas são o FGTS-PJ. Usa
-- vlr_liberado (desembolso de verdade, valor efetivamente liberado ao
-- longo da obra) — decisão de 2026-08-24: não usar valor contratado.
--
-- ATENÇÃO: o nome do arquivo muda toda sexta-feira (MC{data}__...). Este
-- model aponta pro mais recente conhecido em 2026-08-25 (21/08/2026) —
-- precisa atualizar o caminho manualmente até existir uma DAG que copie
-- sempre o arquivo mais novo pra um caminho fixo.

with contratos as (
    select
        r['cod_contrato']::text as cod_contrato
    from read_parquet('s3://data-lake-mcid/staging/sftp/caixa.geavo/GEAVO/MC20260821__MCidades_AO_1__tab_contratos_fgts.parquet') as r
    where r['cod_area']::text = '2'
      and r['cod_linha']::text in ('33', 'AK')
),

desembolsos as (
    select
        r['cod_contrato']::text     as cod_contrato,
        r['dte_ano']::text::int     as ano,
        r['dte_mes_ref']::text::int as mes,
        r['vlr_liberado']::text::numeric as valor
    from read_parquet('s3://data-lake-mcid/staging/sftp/caixa.geavo/GEAVO/MC20260821__MCidades_AO_2__tab_desembolsos_fgts.parquet') as r
)

select
    d.ano,
    (floor((d.mes - 1) / 3))::int + 1        as trimestre,
    sum(d.valor)                             as fgts_pj_desembolsado,
    count(distinct d.cod_contrato)           as fgts_pj_qtd_contratos,
    current_timestamp                        as dt_silver
from desembolsos d
inner join contratos c on c.cod_contrato = d.cod_contrato
group by d.ano, (floor((d.mes - 1) / 3))::int + 1
