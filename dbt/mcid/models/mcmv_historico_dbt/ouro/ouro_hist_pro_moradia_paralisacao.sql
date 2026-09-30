{{ config(materialized="table") }}

-- OURO — histórico semanal de paralisação por contrato Pró-Moradia (change
-- criar-ouro-serie-historica-frentes-novas, design.md D5). Grão:
-- (codigo_contrato, dt_referencia).
--
-- Enriquecimento direto de prata_hist_pro_moradia_paralisacao com
-- contexto de contrato (uf, municipio, nome_empreendimento), sem agregação
-- nem dedup adicional — a prata já preserva 1 linha por semana observada
-- (cada snapshot em que o contrato apareceu como paralisado), e já resolveu
-- o defeito pontual de linhas idênticas no snapshot de 2026-06-19. O valor
-- desta tabela como "ouro" é a curadoria de contexto, não uma transformação
-- de grão.
--
-- Ausência de linha não é marcada como "resolução": um contrato que deixa de
-- aparecer na prata a partir de um dt_referencia simplesmente não tem mais
-- linha aqui dali em diante.
{% set paralisacao = ref('prata_hist_pro_moradia_paralisacao') %}
{% set contrato = ref('prata_hist_pro_moradia_contrato') %}

select
    p.frente_mcmv,
    p.cod_contrato as codigo_contrato,
    p.dt_referencia,
    c.uf,
    c.municipio,
    c.nome_empreendimento,
    p.dias_sem_evolucao,
    p.faixa_paralisacao,
    p.ultimo_motivo_paralisacao,
    p.situacao_atual,
    p.data_base
from {{ paralisacao }} as p
left join {{ contrato }} as c on p.cod_contrato = c.codigo_contrato
order by codigo_contrato, dt_referencia
