{{ config(materialized="table") }}

-- BRONZE — série histórica semanal de operações paralisadas do setor
-- público, família `operacoes_paralisadas_fgts_setorpublico` de
-- `staging/sftp/caixa.geavo/GEAVO/MC<aaaammdd>__MCidades_AO_2__operacoes_paralisadas_fgts_setorpublico.parquet`,
-- cópia fiel.
--
-- Diferente das outras 2 famílias GEAVO, esta é um RETRATO do momento (sem
-- coluna de competência) — só as operações paralisadas ATIVAS naquele
-- snapshot (124 linhas no snapshot mais recente). Sem filtro nem dedup
-- aqui; a prata (prata_pro_moradia_historico_paralisacao) restringe ao
-- universo Pró-Moradia e preserva uma linha por (contrato, semana de
-- observação) — não deduplica para o estado mais recente (D3 do design.md),
-- justamente para transformar este retrato em série histórica de verdade.
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql).
-- change enriquecer-pro-moradia-execucao-desembolso-historico.
{{ bronze_geavo_semanal('operacoes_paralisadas_fgts_setorpublico') }}
