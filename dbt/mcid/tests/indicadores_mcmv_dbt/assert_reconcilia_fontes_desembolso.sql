{{ config(severity='warn') }}

-- Teste singular: reconciliação entre as duas fontes de desembolso do
-- ouro_reloginho_indicadores_gargalo_desempenho, para o conjunto de APF em que AS DUAS
-- existem:
--   valor_liberado_historico       — ficha GEFUS/CAIXA (primário, D4)
--   valor_desembolsado_componentes — agregado do *_financeiro_mensal (SharePoint)
--
-- Emite `warn` quando a razão Σ(SharePoint) / Σ(GEFUS) sai da banda
-- [0,1 ; 10]. O `warn` REFLETE A COBERTURA PARCIAL do feed SharePoint (só
-- liberações pós-2024), não um defeito de cálculo — enquanto o feed for
-- parcial, este teste tende a ficar em `warn`. Change:
-- vocabulario-e-qualidade-financeira-historica (D7).

with base as (
    select
        frente,
        sum(valor_liberado_historico) as soma_gefus,
        sum(valor_desembolsado_componentes) as soma_sharepoint,
        count(*) as n_apf
    from {{ ref('ouro_reloginho_indicadores_gargalo_desempenho') }}
    where valor_liberado_historico is not null
        and valor_desembolsado_componentes is not null
        and valor_liberado_historico > 0
    group by frente
)

select
    frente,
    n_apf,
    round(soma_gefus, 2) as soma_gefus,
    round(soma_sharepoint, 2) as soma_sharepoint,
    round(soma_sharepoint / nullif(soma_gefus, 0), 3) as razao
from base
where soma_sharepoint / nullif(soma_gefus, 0) < 0.1
    or soma_sharepoint / nullif(soma_gefus, 0) > 10
