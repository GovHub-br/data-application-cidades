{{ config(materialized='table') }}

-- Prata do conjuntura: PNAD Contínua — pessoas ocupadas (IBGE/SIDRA).
--
-- O achatamento acontece no leitor `ibge_agregados_json` da ingestão; a
-- tipagem, no macro `prata_ibge_agregado`.
--
-- Categorias: 47946 = Total, 47949 = Construção (classificação 888).
--
-- ⚠️ O nome do arquivo na staging segue a Variable `IBGE_CONFIGURACOES`
-- (`pnad_trabalho_construcao` / `pnad_rendimento_construcao`), e não o nome
-- antigo `pnad_construcao_*`. Os arquivos antigos ficaram órfãos: a config
-- foi renomeada, mas as chamadas passaram a falhar (separador de categoria
-- errado no client) e os novos nunca foram gerados. Corrigido em 2026-08-30.
{{ prata_ibge_agregado('bronze_ibge_pnad_construcao_ocupados') }}
