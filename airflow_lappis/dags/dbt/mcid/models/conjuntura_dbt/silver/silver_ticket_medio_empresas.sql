{{ config(materialized='table') }}

SELECT
    ano_ticket_medio                                AS ano,
    trimestre_ticket_medio                          AS trimestre,
    MAX(CASE WHEN nome_empresa = 'MRV'        THEN ticket_medio_lancamentos              END) AS mrv_ticket,
    MAX(CASE WHEN nome_empresa = 'Direcional'  THEN ticket_medio_lancamentos              END) AS dir_ticket,
    MAX(CASE WHEN nome_empresa = 'Tenda'       THEN ticket_medio_lancamentos              END) AS ten_ticket,
    MAX(CASE WHEN nome_empresa = 'MRV'        THEN ticket_medio_lancamentos_var_tri_ant  END) AS mrv_var_tri,
    MAX(CASE WHEN nome_empresa = 'Direcional'  THEN ticket_medio_lancamentos_var_tri_ant  END) AS dir_var_tri,
    MAX(CASE WHEN nome_empresa = 'Tenda'       THEN ticket_medio_lancamentos_var_tri_ant  END) AS ten_var_tri,
    MAX(CASE WHEN nome_empresa = 'MRV'        THEN ticket_medio_lancamentos_acum_4t20    END) AS mrv_acum,
    MAX(CASE WHEN nome_empresa = 'Direcional'  THEN ticket_medio_lancamentos_acum_4t20    END) AS dir_acum,
    MAX(CASE WHEN nome_empresa = 'Tenda'       THEN ticket_medio_lancamentos_acum_4t20    END) AS ten_acum
FROM {{ source('conjuntura_bronze', 'bronze_ticket_medio_empresas') }}
GROUP BY ano_ticket_medio, trimestre_ticket_medio