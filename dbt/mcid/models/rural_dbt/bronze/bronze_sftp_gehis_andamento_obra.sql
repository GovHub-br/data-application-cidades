{{ config(materialized="table") }}

-- Bronze: Andamento de obra informado pela CAIXA (GEHIS), retrato mensal, TODAS as remessas.
-- Fonte: SFTP — sftp/fabrica/GEFUS/(ANTERIORES/)CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M<AAAAMM...>
--
-- Diferente das outras bronzes, guarda a série inteira (julho/2021 em diante), não só o
-- último arquivo: o valor dela está justamente na trajetória da obra mês a mês (Normal,
-- Em atenção, Risco de paralisação, Paralisado, Retomada...). O arquivo traz APFs do Rural,
-- do FAR e do Entidades; quem filtra o programa é a prata/ouro, cruzando a APF.
-- Exploração (2026-10-06): 221 arquivos; no mais recente 822 APFs, 807 delas do Rural na SNH.
{% set padrao = "s3://data-lake-mcid/staging/**/*CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
