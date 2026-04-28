select 
    cast(dt_referencia as date) as dt_referencia,
    cast(coibge as varchar) as codigo_ibge,
    replace(cnpj, '''', '') as cnpj, -- Remove a aspa simples que costuma vir do Excel
    municipio,
    uf,
    cast({{ parse_financial_value('populacao') }} as bigint) as populacao,
    cast(reg_metrop as varchar(3)) as reg_metrop,
    cast(nullif(nullif(split_part(ano_rel_gestao, '.', 1), ''), 'NaN') as integer) as ano_relatorio_gestao,
    to_date(nullif(dt_lei_flhis_analise_cefus, ''), 'DD/MM/YYYY') as dt_lei_flhis,
    to_date(nullif(dt_lei_cgflhis_analise_cefus, ''), 'DD/MM/YYYY') as dt_lei_cgflhis,
    to_date(nullif(dt_termo_adesao_analise_cefus, ''), 'DD/MM/YYYY') as dt_termo_adesao,
    to_date(nullif(dt_plano_habit_analise_cefus, ''), 'DD/MM/YYYY') as dt_plano_habit,
    to_date(nullif(dt_rel_gestao_analise_cefus, ''), 'DD/MM/YYYY') as dt_rel_gestao,
    -- Status e Categorias
    trim(situacao_lei_flhis) as situacao_lei_flhis,
    trim(situacao_lei_cgflhis) as situacao_lei_cgflhis,
    trim(situacao_termo_adesao) as situacao_termo_adesao,
    trim(situacao_plano_habit) as situacao_plano_habit,
    trim(situacao_rel_gestao) as situacao_rel_gestao,
    trim(situacao_municipio) as situacao_municipio,
    arquivo_origem,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source('dados_abertos_cidades', 'dados_abertos_snhis_regularidade_entes') }}
