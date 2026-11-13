{{ config(materialized="table", tags=["ts_lake"]) }}

-- Bronze: TODAS as remessas de acompanhamento do Trabalho Social da staging (pasta Trabalho Social,
-- inclusive Arquivados), empilhadas com o nome do arquivo. Cópia fiel, tudo texto.
--
-- Para que serve: a remessa de jul/2026 traz contratos sem portaria de TS (629 no FDS e quase todo
-- o Rural CAIXA). Remessas anteriores do mesmo contrato podem ter a portaria preenchida. A prata
-- prata_ts_portaria_remessa identifica linha, agente e competência pelo nome do arquivo.
--
-- O molde abaixo garante que as colunas usadas pela prata existam mesmo nas remessas de layout
-- antigo (as bases de jan/2025 usam contrato_registro_ao, percentual_obra etc.).
{% set padrao = caminho_fonte_lake('mcmv_ts_remessas_historico', 'trabalho_social_lake') %}
{% set colunas = [
    'recurso', 'recursos',
    'contrato', 'contratoregistroao', 'contrato_registro_ao', 'contratoregistro_ao', 'cimregistroaf',
    'nome_empreendimento', 'municipio', 'uf',
    'data_da_contratacao', 'data_contratacao_empreendimento',
    'portaria_adotada', 'portaria_ts_utilizada',
    'situacao_ts',
] %}

{{ bnz_ts_staging(padrao, colunas, todos_os_arquivos=true) }}
