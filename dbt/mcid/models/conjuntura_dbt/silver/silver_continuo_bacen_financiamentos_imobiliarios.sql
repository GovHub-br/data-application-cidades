{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: financiamentos imobiliários PF/PJ (BACEN SGS).
-- Página 2/3, seção 3.
--
-- Reescrita em 2026-08-28 pra nova arquitetura: a bronze materializa o
-- parquet de staging (espelho do raw) e a silver ACHATA + TIPA.
--
-- Formato do raw (BACEN SGS): uma única linha, com uma coluna por série e
-- a série inteira aninhada como JSON dentro da coluna:
--   pf_concessoes_rs_mi = [{"data": "01/03/2011", "valor": "5841"}, ...]
-- O achatamento despivota as 7 séries pra formato longo (data, tipo, valor),
-- que é o contrato que o gold espera.
--
-- `data` vem como texto DD/MM/YYYY no raw e é convertido pra date aqui —
-- sem isso o `order by data desc` do gold ordenaria alfabeticamente.

with bronze as (
    select * from {{ ref('bronze_continuo_bacen_financiamentos_imobiliarios') }}
),

series as (
    select t.tipo, t.serie
    from bronze b
    cross join lateral (values
        ('pf_concessoes_rs_mi',        b.pf_concessoes_rs_mi),
        ('pf_taxa_juros_aa',           b.pf_taxa_juros_aa),
        ('pf_inadimplencia_pct',       b.pf_inadimplencia_pct),
        ('pj_concessoes_rs_mi',        b.pj_concessoes_rs_mi),
        ('pj_taxa_juros_aa',           b.pj_taxa_juros_aa),
        ('pj_inadimplencia_pct',       b.pj_inadimplencia_pct),
        ('indice_imobiliario_por_pib', b.indice_imobiliario_por_pib)
    ) as t(tipo, serie)
)

select
    to_date(elem ->> 'data', 'DD/MM/YYYY')      as data,
    series.tipo                                 as tipo,
    nullif(elem ->> 'valor', '')::numeric       as valor
from series
cross join lateral jsonb_array_elements(series.serie::jsonb) as elem
