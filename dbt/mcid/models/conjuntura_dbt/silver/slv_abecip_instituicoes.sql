{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: financiamentos SBPE por instituição
-- financeira e modalidade (ABECIP). Página 3, seção 6 do boletim
-- ("Novos Financiamentos Imobiliários por banco").
--
-- Origem: extração do relatório mensal da ABECIP feita por outro time
-- (pipeline de OCR — ver `execution_id`/`document_id`).
--
-- ⚠️ COBERTURA: cada extração cobre **uma competência**, não a série. Hoje
-- só existe 2026-06. O histórico vai se formar conforme novas competências
-- forem extraídas — não espere série longa aqui.
--
-- Validado em 2026-08-29: o acumulado do ano do TOTAL (277.086 UH /
-- R$ 93.738,1 mi) bate EXATO com a soma jan–jun/2026 do XLSX de unidades
-- (`slv_abecip_financiamentos`), que é uma extração
-- independente da mesma ABECIP.

select
    competencia_referencia                        as competencia,
    to_date(periodo_rotulo, 'YYYY-MM')            as data_referencia,
    left(periodo_rotulo, 4)::int                  as ano,
    right(periodo_rotulo, 2)::int                 as mes,
    modalidade                                    as modalidade,
    instituicao_financeira                        as instituicao,
    nullif(volume_mensal_milhoes, '')::numeric        as volume_mensal_milhoes,
    nullif(unidades_mensais, '')::numeric             as unidades_mensais,
    nullif(volume_acumulado_ano_milhoes, '')::numeric as volume_acumulado_ano_milhoes,
    nullif(unidades_acumuladas_ano, '')::numeric      as unidades_acumuladas_ano,
    exportado_em::timestamp                       as dt_ingest
from {{ ref('bnz_abecip_instituicoes') }}
