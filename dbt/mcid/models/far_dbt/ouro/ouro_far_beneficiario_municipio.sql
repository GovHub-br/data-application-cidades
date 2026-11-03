{{ config(materialized="table") }}

-- Ouro: contratos das famílias do FAR por município e geração (Novo MCMV / legado).
-- Responde onde os contratos se concentram, onde a inadimplência é maior e se renda
-- menor anda junto com inadimplência. Taxas recalculadas no grão, nunca somadas:
-- inadimplência é sobre os contratos ativos, porque liquidado e distratado não pagam prestação.
select
    geracao,
    uf,
    cod_ibge,
    max(municipio) as municipio,
    max(municipio) || '/' || uf as municipio_uf,
    count(*) as qt_contratos,
    count(*) filter (where ic_ativo) as qt_ativos,
    count(*) filter (where ic_inadimplente) as qt_inadimplentes,
    round(100.0 * count(*) filter (where ic_inadimplente) / nullif(count(*) filter (where ic_ativo), 0), 1) as pct_inadimplencia,
    count(*) filter (where situacao_contrato like 'Liquidado%') as qt_liquidados,
    count(*) filter (where situacao_contrato = 'Distratado') as qt_distratados,
    count(*) filter (where situacao_contrato = 'Cancelado') as qt_cancelados,
    count(*) filter (where ic_retomado) as qt_retomados,
    round(100.0 * count(*) filter (where sexo_responsavel = 'Feminino') / count(*), 1) as pct_responsavel_mulher,
    round(avg(renda_familiar) filter (where renda_familiar > 0), 2) as renda_media,
    round(100.0 * count(*) filter (where renda_familiar = 0) / nullif(count(renda_familiar), 0), 1) as pct_sem_renda,
    sum(valor_em_atraso) filter (where ic_inadimplente) as valor_em_atraso
from {{ ref("prata_far_contrato_pf") }}
where cod_ibge is not null
group by geracao, uf, cod_ibge
