{{ config(materialized="table") }}

-- Gold: portarias e atos do MCMV FNHIS Sub-50, com o que cada um representa no dado. Grão: ato.
-- Para a abertura da Seleção 2024 (673/2024), conta as propostas apresentadas; para o resultado (1.310/2024),
-- as selecionadas — as duas vêm da mesma base do TransfereGov (prata_fnhis_propostas), que é desse ciclo.
-- Os demais ciclos (2025, Calamidade RS 2024) ainda não têm lista de propostas carregada.

with
    cat as (select * from {{ ref("prata_fnhis_portarias") }}),
    p as (select * from {{ ref("prata_fnhis_propostas") }}),
    contagem as (
        select '673_2024'::text as portaria_codigo, count(*) as qt_propostas, sum(quantidade_uh) as uh_propostas
        from p
        union all
        select '1310_2024', count(*), sum(quantidade_uh)
        from p
        where ic_selecionada
    )

select
    c.*,
    case
        when c.tem_lista_nominal <> 'sim' and c.portaria_codigo <> '673_2024' then 'Não lista propostas'
        when k.portaria_codigo is null then 'Lista não carregada'
        else 'Lista carregada'
    end as situacao_lista,
    k.qt_propostas,
    k.uh_propostas
from cat c
left join contagem k on k.portaria_codigo = c.portaria_codigo
