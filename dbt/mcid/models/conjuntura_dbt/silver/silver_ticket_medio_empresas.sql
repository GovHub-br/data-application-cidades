{{ config(materialized="table") }}

select
    ano_ticket_medio as ano,
    trimestre_ticket_medio as trimestre,
    max(case when nome_empresa = 'MRV' then ticket_medio_lancamentos end) as mrv_ticket,
    max(
        case when nome_empresa = 'Direcional' then ticket_medio_lancamentos end
    ) as dir_ticket,
    max(case when nome_empresa = 'Tenda' then ticket_medio_lancamentos end) as ten_ticket,
    max(
        case when nome_empresa = 'MRV' then ticket_medio_lancamentos_var_tri_ant end
    ) as mrv_var_tri,
    max(
        case
            when nome_empresa = 'Direcional' then ticket_medio_lancamentos_var_tri_ant
        end
    ) as dir_var_tri,
    max(
        case when nome_empresa = 'Tenda' then ticket_medio_lancamentos_var_tri_ant end
    ) as ten_var_tri,
    max(
        case when nome_empresa = 'MRV' then ticket_medio_lancamentos_acum_4t20 end
    ) as mrv_acum,
    max(
        case when nome_empresa = 'Direcional' then ticket_medio_lancamentos_acum_4t20 end
    ) as dir_acum,
    max(
        case when nome_empresa = 'Tenda' then ticket_medio_lancamentos_acum_4t20 end
    ) as ten_acum,
    {{ add_metadata_timestamps("silver", has_ingest_date=false) }}
from {{ source("conjuntura_bronze", "bronze_ticket_medio_empresas") }}
group by ano_ticket_medio, trimestre_ticket_medio
