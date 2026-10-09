{{ config(materialized='table') }}

-- Prata do conjuntura: PIB da construção civil (IBGE/SIDRA).
--
-- Passthrough tipado. O achatamento do payload acontece **uma vez, na
-- ingestão** (conversor `ibge_v3`, plugins/ingestion), que itera variável →
-- resultados → séries → períodos; a tipagem é o macro `ibge_v3_tipado`. O padrão é uniforme para qualquer agregado,
-- então refazer isso em SQL aqui seria duplicar trabalho — foi o que o macro
-- `achatar_sidra` fazia, e por isso ele saiu (2026-08-30).

select
    periodo,
    -- date, não timestamp: representa um mês/trimestre de referência,
    -- não um instante. Sem o cast, `current_date - data_referencia`
    -- devolve interval e quebra o teste de frescor.
    data_referencia::date         as data_referencia,
    variavel_id,
    variavel_nome                as variavel,
    unidade,
    localidade_id,
    localidade_nome              as localidade,
    classificacao_id,
    classificacao,
    categoria_id,
    categoria,
    valor,
    dt_ingest
from ({{ ibge_v3_tipado(ref('bronze_ibge_pib_construcao_civil')) }}) as bronze
