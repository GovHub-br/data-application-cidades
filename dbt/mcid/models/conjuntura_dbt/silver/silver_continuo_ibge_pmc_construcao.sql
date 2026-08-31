{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PMC — vendas de materiais de construção (IBGE/SIDRA).
--
-- Passthrough tipado. O achatamento do payload do SIDRA acontece **uma vez,
-- na ingestão** (`ClienteIBGE.transformar_resposta`), que itera variável →
-- resultados → séries → períodos. O padrão é uniforme para qualquer agregado,
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
from {{ ref('bronze_continuo_ibge_pmc_construcao') }}
