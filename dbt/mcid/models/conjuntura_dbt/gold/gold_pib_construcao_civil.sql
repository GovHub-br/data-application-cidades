{{ config(materialized="table") }}

with
    pivotado as (
        select
            variavel_id,
            max(case when periodo = '202404' then valor_percentual end) as t_4t24,
            max(case when periodo = '202501' then valor_percentual end) as t_1t25,
            max(case when periodo = '202502' then valor_percentual end) as t_2t25,
            max(case when periodo = '202503' then valor_percentual end) as t_3t25,
            max(case when periodo = '202504' then valor_percentual end) as t_4t25,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_ibge_pib_construcao_civil") }}
        group by variavel_id
    )

select
    case
        variavel_id
        when 6564
        then 'Trim./Trim. Imediatamente Anterior'
        when 6563
        then 'Acumulada ao Longo do Ano'
        when 6562
        then 'Acum. Últimos 4 Trimestres'
    end as indicador,
    round(t_4t24, 1) as tri_2024_4,
    round(t_1t25, 1) as tri_2025_1,
    round(t_2t25, 1) as tri_2025_2,
    round(t_3t25, 1) as tri_2025_3,
    round(t_4t25, 1) as tri_2025_4,
    {{ add_metadata_timestamps("gold") }}
from pivotado
order by variavel_id desc
