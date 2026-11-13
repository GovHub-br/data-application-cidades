{{ config(materialized="table") }}

-- Prata: Operações do MCMV acompanhadas pela SNH em TODAS as modalidades exceto FNHIS — a
-- experiência anterior dos municípios com obra habitacional, usada no histórico do ente que
-- propôs ao FNHIS Sub-50 (ouro_fnhis_ente_historico).
-- Fonte: bronze_shpt_dados_prioritarios_snh_empreendimentos (a mesma bronze do Rural, que traz
-- todas as modalidades). Grão: uma operação (linha da SNH).
--
-- Antes esta conformação ficava dentro da ouro, lendo a bronze direto; veio para a prata para
-- a ouro ler só prata. O SQL é o mesmo.

select
    left(regexp_replace(codigo_ibge_do_municipio::text, '[^0-9]', '', 'g'), 6) as cod_ibge_6,
    trim({{ target.schema }}.corrigir_mojibake(modalidade::text)) as modalidade,
    trim({{ target.schema }}.corrigir_mojibake(situacao_da_empreendimento_agrupada::text)) as situacao,
    {{ target.schema }}.parse_date_br(nullif(trim(data_da_contratacao::text), '')) as dt_contratacao,
    {{ parse_int("regexp_replace(unidades_contratadas::text, '[^0-9]', '', 'g')") }} as uh_contratadas,
    {{ parse_int("regexp_replace(unidades_entregues::text, '[^0-9]', '', 'g')") }} as uh_entregues,
    {{ parse_int("regexp_replace(unidades_distratadas::text, '[^0-9]', '', 'g')") }} as uh_distratadas
from {{ ref("bronze_shpt_dados_prioritarios_snh_empreendimentos") }}
where upper(trim(modalidade::text)) <> 'FNHIS'
