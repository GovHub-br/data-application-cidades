-- Aponta fontes cujo dado mais recente ficou velho demais para o boletim.
--
-- Por que este teste existe: várias fontes do conjuntura NÃO se atualizam
-- sozinhas. Os arquivos do GEAVO têm a data no nome e são apontados à mão; a
-- bronze só refaz quando alguém roda o dbt. Sem um teste, "o dado parou" só
-- aparece quando alguém estranha um número no dashboard — que é tarde.
--
-- O limite é por frequência da fonte, com folga para o atraso normal de
-- divulgação. Falhar aqui não quer dizer que o pipeline quebrou: quer dizer
-- que a fonte não traz dado novo há tempo demais e alguém precisa olhar.

with fontes as (
    select 'gld_sinapi'        as model, 'mensal'     as frequencia,
           max(data_referencia)          as mais_recente from {{ ref('gld_sinapi') }}
    union all
    select 'gld_incc_m', 'mensal', max(mes) from {{ ref('gld_incc_m') }}
    union all
    select 'gld_fipezap', 'mensal', max(data_referencia) from {{ ref('gld_fipezap') }}
    union all
    select 'gld_icst', 'mensal', max(data_referencia) from {{ ref('gld_icst') }}
    union all
    select 'gld_indice_imob', 'mensal', max(data_referencia) from {{ ref('gld_indice_imob') }}
    union all
    select 'gld_saldo_poupanca', 'mensal', max(data_referencia) from {{ ref('gld_saldo_poupanca') }}
    union all
    select 'gld_credito_pib', 'mensal', max(data) from {{ ref('gld_credito_pib') }}
    union all
    select 'gld_empregos_caged', 'mensal', max(make_date(ano, mes, 1)) from {{ ref('gld_empregos_caged') }}
    union all
    select 'gld_producao_fisica', 'mensal', max(data_referencia) from {{ ref('gld_producao_fisica') }}
    union all
    select 'gld_financiamentos_habitacionais', 'trimestral',
           max(make_date(ano, trimestre * 3, 1)) from {{ ref('gld_financiamentos_habitacionais') }}
    union all
    select 'gld_pib_construcao_civil', 'trimestral', max(data_referencia)
      from {{ ref('gld_pib_construcao_civil') }}
),

limites as (
    select *,
        -- A tolerância é contada a partir do INÍCIO do período de referência,
        -- então precisa cobrir o período inteiro MAIS o atraso de divulgação.
        --   mensal    : mês (30) + ~2 meses de atraso           -> 90
        --   trimestral: trimestre (90) + ~3 meses de atraso + folga -> 270
        -- 180 dias no trimestral era apertado demais e acusava o PIB da
        -- construção como parado sendo que ele estava no prazo: em ago/2026 o
        -- dado mais recente do IBGE é o 1T2026, e isso é o esperado.
        case frequencia when 'mensal' then 90 else 270 end as dias_tolerados,
        current_date - mais_recente as dias_parado
    from fontes
)

select model, frequencia, mais_recente, dias_parado, dias_tolerados
from limites
where mais_recente is null
   or dias_parado > dias_tolerados
