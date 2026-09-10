-- DESABILITADO nesta branch: o modelo do piloto #118 (prata_dhist_serie_anual_ogu_fgts)
-- está enabled=false (change renomear-camadas-pt-historico-reloginho, D4). Reativar junto com o modelo.

{{ config(enabled=false) }}
select *
from {{ ref("prata_dhist_serie_anual_ogu_fgts") }}
where dt_valid_to is not null
  and dt_valid_to <= dt_valid_from
