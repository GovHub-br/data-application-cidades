{{ config(materialized="table") }}

-- Gold: Cada contrato do Pró-Moradia ligado ao ATO que o originou (seleção PAC, processo,
-- ofício, portaria...). Responde "quais propostas são de quais contratos" e "conectar
-- financiamentos a ato público", até onde o Canal FGTS permite.
-- Grão: contrato.
--
-- A única ligação contrato → seleção no Canal FGTS é `cod_ident_externo` (identificador_selecao
-- na prata), texto livre e inconsistente ("PAC - 3A SELECAO", "PAC 4A SELECAO 2009",
-- "80000.015496/2003-14", "OF 279/97, DE 30/04/1997"...). Aqui ele é classificado por tipo de
-- ato e normalizado para agrupar variantes do mesmo ato. Não há carta-consulta nem proposta.

with
    c as (select * from {{ ref("prata_pro_moradia_contrato") }}),

    classif as (
        select
            c.*,
            upper(regexp_replace(trim(c.identificador_selecao), '\s+', ' ', 'g')) as ident,
            case
                when nullif(trim(c.identificador_selecao), '') is null then 'Sem identificação'
                when c.identificador_selecao ~* 'PAC' then 'Seleção PAC'
                when c.identificador_selecao ~* 'portaria' then 'Portaria'
                when c.identificador_selecao ~ '^\s*\d{5}\.\d{6}/\d{4}' then 'Processo administrativo'
                when c.identificador_selecao ~* '^\s*(OF\b|OF\.|OFICIO|OF[IÍ]CIO)' then 'Ofício'
                when c.identificador_selecao ~* 'VOTO|^\s*CI\b' then 'Voto / comunicação interna'
                else 'Outro'
            end as tipo_ato
        from c
    )

select
    cod_contrato,
    contrato,
    tomador_nome,
    tomador_esfera,
    tomador_cnpj,
    uf,
    municipio,
    tipo_intervencao,
    ano_orcamento,
    dt_assinatura,
    situacao_contrato,
    ic_contrato_vigente,
    valor_contratado,
    quantidade_uh_financiadas,
    populacao_beneficiada,
    ic_pac,
    identificador_selecao,
    tipo_ato,
    case
        when tipo_ato = 'Seleção PAC' then
            'PAC' || case when ident ~ 'NPCF' then ' NPCF' else '' end
            || coalesce(' - ' || substring(ident from '(\d)\s*A\s*SEL') || 'ª seleção', '')
            || coalesce(' ' || substring(ident from '(20\d\d)'), '')
        when tipo_ato = 'Portaria' then 'Portaria ' || coalesce(substring(ident from '(\d+/\d{2,4})'), ident)
        when tipo_ato = 'Processo administrativo' then substring(ident from '(\d{5}\.\d{6}/\d{4}(-\d{2})?)')
        when tipo_ato = 'Ofício' then 'Ofício ' || coalesce(substring(ident from '(\d+/\d{2,4})'), substring(ident from '(\d+)'), '')
        when tipo_ato = 'Sem identificação' then null
        else ident
    end as ato_normalizado,
    substring(ident from '(\d)\s*A\s*SEL')::int as numero_selecao_pac,
    coalesce(
        substring(ident from '(19\d\d|20\d\d)')::int,
        case
            when substring(ident from '/(\d{2})\M') is null then null
            when substring(ident from '/(\d{2})\M')::int > 50 then 1900 + substring(ident from '/(\d{2})\M')::int
            else 2000 + substring(ident from '/(\d{2})\M')::int
        end
    ) as ano_ato
from classif
