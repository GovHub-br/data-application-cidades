{{ config(materialized="table") }}

-- SILVER — contratos Pró-Moradia, via recorte por `cod_linha = '26'` dentro
-- do Canal FGTS (D1 da change frentes-restantes-mcmv-historico — a única
-- frente das 5 sem arquivo dedicado). A bronze
-- (bronze_shpt_fgts_contratos) é fiel, sem filtro; é AQUI que a regra de
-- negócio "linha 26 = Pró-Moradia" é aplicada.
--
-- Enriquecida via `left join` com bronze_shpt_fgts_empreendimentos
-- (cod_empreendimento) e com bronze_shpt_fgts_dom_linha (resolve `cod_linha`
-- para texto — não exclusivo de Pró-Moradia, mas usado aqui).
--
-- `dte_assinatura` do Canal FGTS vem em formato `MM/DD/YY HH:MM:SS`
-- (americano, não BR) — parse dedicado via `strptime`, sem reusar
-- `parse_date_br` (que assume DD/MM/YYYY).
--
-- Cobertura temporal: concentração 1995-2009 e retomada 2023+ (hiato
-- 2009-2023) é característica da FONTE, não indício de bug — documentado no
-- schema.yml (task 11.2), sem teste de cobertura mínima por ano.
{% set contratos = ref('bronze_shpt_fgts_contratos') %}
{% set empreendimentos = ref('bronze_shpt_fgts_empreendimentos') %}
{% set dom_linha = ref('bronze_shpt_fgts_dom_linha') %}

select
    'Minha Casa Minha Vida'::text as programa,
    'Pró-Moradia'::text as frente_mcmv,
    'Subsidiada'::text as grupo_linha,
    coalesce(nullif(trim(dl.linha), ''), 'HAB / PRO-MORADIA')::text as linha_mcmv,
    nullif(trim(c.cod_contrato), '')::text as codigo_contrato,
    nullif(trim(c.cod_empreendimento), '')::text as codigo_empreendimento,
    nullif(trim(e.txt_nome_empreendimento), '')::text as nome_empreendimento,
    nullif(trim(e.cod_municipio), '')::text as codigo_ibge_municipio,
    nullif(trim(e.txt_localidade), '')::text as municipio,
    nullif(trim(c.uf), '')::text as uf,
    {{ parse_hist_numeric('c.vlr_contratado') }} as valor_contratado,
    {{ parse_hist_numeric('c.vlr_investimento') }} as valor_investimento,
    try_strptime(
        nullif(nullif(trim(c.dte_assinatura), ''), 'None'), '%m/%d/%y %H:%M:%S'
    )::date as dt_contratacao,
    nullif(trim(c.dte_orcamento_ano), '')::text as ano_orcamento,
    nullif(trim(c.cod_situacao_contrato), '')::text as codigo_situacao_contrato,
    nullif(trim(c.cod_modalidade), '')::text as codigo_modalidade,
    {{ parse_hist_bigint('e.qtd_unidades_financiadas') }} as quantidade_uh,
    c.dt_referencia,
    c.source_file,
    c.hash_linha,
    c.dt_ingest
from {{ contratos }} as c
left join {{ empreendimentos }} as e on c.cod_empreendimento = e.cod_empreendimento
left join {{ dom_linha }} as dl on c.cod_linha = dl.codigo
where c.cod_linha = '26' and nullif(trim(c.cod_contrato), '') is not null
