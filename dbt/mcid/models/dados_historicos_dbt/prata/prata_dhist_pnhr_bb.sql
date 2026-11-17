-- Silver da família `pnhr_bb` dos dados históricos (de-para em
-- macros/dados_historicos/de_para.sql), com a correção do único arquivo que mistura as
-- escalas do percentual de obra (2015-09, research R2): ali o percentual_auto não converte
-- (o máximo do arquivo é 100), então cada valor em (0, 1] é resolvido pela mesma proposta em
-- 2015-10, ficando a escala mais perto do vizinho; sem vizinho, nulo (o teste
-- dhist_prata_percentual_escala conta esses casos).
{% call duckdb_query() %}
with

    base as ({{ prata_dhist_sql("pnhr_bb") }}),

    vizinho as (
        select apf, any_value(percentual_execucao_fisica) as percentual_vizinho
        from base
        where competencia = date '2015-10-01'
        group by 1
    )

select
    base.* replace (
        case
            when
                base.competencia = date '2015-09-01'
                and base.percentual_execucao_fisica > 0
                and base.percentual_execucao_fisica <= 1
            then
                case
                    when
                        abs(base.percentual_execucao_fisica * 100 - vizinho.percentual_vizinho)
                        < abs(base.percentual_execucao_fisica - vizinho.percentual_vizinho)
                    then base.percentual_execucao_fisica * 100
                    when vizinho.percentual_vizinho is not null
                    then base.percentual_execucao_fisica
                end
            else base.percentual_execucao_fisica
        end as percentual_execucao_fisica
    )
from base
left join vizinho using (apf)
{% endcall %}
