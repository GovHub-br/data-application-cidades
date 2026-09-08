-- dim_empreendimento_historico DEVE ter exatamente a mesma chave
-- (frente_mcmv, codigo_empreendimento) de gold_snapshot_empreendimento_atual:
-- todo empreendimento do snapshot tem 1 linha na dim e vice-versa.
--
-- Change: colunas-orfas-bronze-historico (dim-empreendimento-historico), task 5.3.
with
    dim as (
        select frente_mcmv, codigo_empreendimento
        from {{ ref('dim_empreendimento_historico') }}
    ),
    snap as (
        select frente_mcmv, codigo_empreendimento
        from {{ ref('gold_snapshot_empreendimento_atual') }}
    )
select 'so_na_dim' as lado, frente_mcmv, codigo_empreendimento from (
    select * from dim except select * from snap
)
union all
select 'so_no_snapshot' as lado, frente_mcmv, codigo_empreendimento from (
    select * from snap except select * from dim
)
