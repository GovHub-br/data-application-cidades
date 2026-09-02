-- Confere cada célula publicada no boletim contra o que o gold produz hoje.
--
-- Substitui `scripts/conjuntura/comparar_gabarito_boletins.py`, que abria
-- conexão própria, rodava 32 SQLs de um YAML e escrevia relatório em Markdown.
-- Com os quadros materializados e uniformes, a conferência é um join — e roda
-- no mesmo `dbt build` que já roda, falhando junto com o resto.
--
-- ⚠️ Divergir NÃO é necessariamente defeito: BACEN, IBGE, CAGED, CBIC e
-- FipeZap revisam meses já publicados, então uma célula pode divergir por a
-- fonte ter mudado depois da edição. O que este teste garante é que nenhuma
-- divergência passe despercebida. Para saber o que o boletim viu na época,
-- use os snapshots (`conjuntura_snapshots.snap_boletim_*`).
--
-- Falha se a célula esperada não existir no gold — coordenada errada é
-- defeito de verdade, e silenciar isso esvaziaria o teste.

with esperado as (
    select * from {{ ref('boletim_gabarito') }}
),

obtido as (
    select modelo, edicao, linha, coluna, valor
    from {{ ref('gold_boletim_valores') }}
),

comparado as (
    select
        e.pagina, e.modelo, e.linha, e.coluna,
        e.esperado, o.valor as obtido, e.tolerancia,
        case
            -- a coordenada não existe: rótulo ou coluna mudou e o gabarito
            -- ficou apontando para o vazio. Isso é defeito e falha o teste.
            when o.modelo is null then 'COORDENADA_INVALIDA'
            -- a coordenada existe mas a célula está vazia: lacuna de fonte
            -- conhecida (ex.: SBPE novos x usados não alcança 2026). Reporta,
            -- não falha — o teste não é o lugar de brigar com fonte ausente.
            when o.valor is null then 'SEM_DADO'
            when abs(o.valor - e.esperado) <= e.tolerancia then 'OK'
            else 'DIVERGE'
        end as veredito
    from esperado e
    left join obtido o
      on  o.modelo = e.modelo
      and o.edicao = e.edicao
      and o.linha  = e.linha
      and o.coluna = e.coluna
)

-- Falha só em coordenada inválida. Divergência de valor NÃO falha: as fontes
-- revisam o passado, e um teste vermelho o tempo todo vira ruído que se
-- aprende a ignorar. As divergências ficam visíveis em `gold_boletim_valores`
-- contra o seed, e o que o boletim viu na época está nos snapshots.
select * from comparado where veredito = 'COORDENADA_INVALIDA'
