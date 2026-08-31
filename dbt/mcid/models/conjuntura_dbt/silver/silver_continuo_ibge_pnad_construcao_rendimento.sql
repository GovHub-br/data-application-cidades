{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PNAD Contínua — rendimento médio real (IBGE/SIDRA).
--
-- Passthrough tipado — o achatamento acontece na ingestão
-- (`ClienteIBGE.transformar_resposta`).
--
-- Categorias: 47946 = Total, 47949 = Construção (classificação 888).
--
-- ⚠️ O nome do arquivo na staging segue a Variable `IBGE_CONFIGURACOES`
-- (`pnad_trabalho_construcao` / `pnad_rendimento_construcao`), e não o nome
-- antigo `pnad_construcao_*`. Os arquivos antigos ficaram órfãos: a config
-- foi renomeada, mas as chamadas passaram a falhar (separador de categoria
-- errado no client) e os novos nunca foram gerados. Corrigido em 2026-08-30.

select
    periodo,
    -- date, não timestamp: representa um mês/trimestre de referência,
    -- não um instante. Sem o cast, `current_date - data_referencia`
    -- devolve interval e quebra o teste de frescor.
    data_referencia::date         as data_referencia,
    variavel_id,
    variavel_nome     as variavel,
    unidade,
    localidade_id,
    localidade_nome   as localidade,
    classificacao_id,
    classificacao,
    categoria_id,
    categoria,
    valor,
    dt_ingest
from {{ ref('bronze_continuo_ibge_pnad_construcao_rendimento') }}
