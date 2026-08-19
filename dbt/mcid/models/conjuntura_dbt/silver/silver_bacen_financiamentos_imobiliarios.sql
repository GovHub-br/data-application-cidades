{{ config(materialized="table") }}

select tipo, data_referencia, valor, {{ add_metadata_timestamps("silver") }}
from {{ ref("bronze_bacen_financiamentos_imobiliarios") }}
