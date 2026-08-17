select *
from {{ ref("historico_mcmv_serie_temporal_snapshot") }}
where dt_valid_to is not null
  and dt_valid_to <= dt_valid_from
