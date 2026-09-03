select *
from {{ ref("silver_mcmv_historico_serie_anual_ogu_fgts") }}
where dt_valid_to is not null
  and dt_valid_to <= dt_valid_from
