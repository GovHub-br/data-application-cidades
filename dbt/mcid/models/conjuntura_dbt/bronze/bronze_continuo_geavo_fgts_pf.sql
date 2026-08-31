{{ config(materialized='table') }}

-- Bronze do conjuntura contínuo: FGTS-PF (Base_PF_FGTS, GEAVO/Caixa).
--
-- ⚠️ DÍVIDA TÉCNICA CONHECIDA — DEVE SER RESOLVIDA ANTES DA APROVAÇÃO DO PR.
--
-- Esta é a ÚNICA bronze do projeto que projeta colunas em vez de espelhar a
-- origem com `select *`. Isso **contraria o contrato da camada**: bronze é
-- espelho, e toda transformação — inclusive descartar coluna — pertence à
-- etapa bronze → silver, já dentro do banco. Projetar aqui é transformação
-- disfarçada.
--
-- Por que a exceção existe hoje (decisão do Lucas em 2026-08-30, para não
-- travar a entrega): com `select *` a tabela vai de 917 MB para ~3,4 GB,
-- e o histórico recente pesa — o Postgres já foi pressionado por essa
-- operação e chegou a cair esta semana por causa das materialized views.
-- São 10,8 milhões de linhas × 30 colunas.
--
-- O que precisa acontecer para regularizar:
--   1. confirmar a capacidade de disco do servidor do banco;
--   2. trocar por `select * from {{ fonte_lake('geavo_fgts_pf') }}`;
--   3. reconstruir e conferir o impacto.
-- Se o custo for inaceitável, a decisão precisa ser registrada como exceção
-- deliberada de arquitetura — não ficar como está, implícita.
--
-- Nota de sintaxe: o alias `r` e `r['coluna']` são exigência do pg_duckdb ao
-- projetar colunas de `read_parquet` (com `select *` não precisa). Sem isso o
-- model não constrói — foi assim que ele quebrou em 2026-08-30.

select
    r['dt_assinatura']  as dt_assinatura,
    r['faixa']          as faixa,
    r['tp_orcamento']   as tp_orcamento,
    r['tpimovel']       as tpimovel,
    r['vlr_emprestimo'] as vlr_emprestimo
from {{ fonte_lake('geavo_fgts_pf') }} as r
