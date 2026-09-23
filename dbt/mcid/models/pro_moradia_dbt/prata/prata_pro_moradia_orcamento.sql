{{ config(materialized="table") }}

-- Prata: Orçamento do FGTS para o Pró-Moradia, por ano e região
-- Fonte: bronze_shpt_fgts_orcamento_final (prestação de contas anual do FGTS), filtrada para o
-- programa Pró-Moradia. É o "Planejamento" do ciclo: quanto o Conselho Curador reservou antes
-- de qualquer contrato.
--
-- Grão: ano de dotação × região. A região vem em duas grafias conforme o programa (`Nordeste`
-- e `NORDESTE`); `initcap` unifica. Se o mesmo ano chegar em mais de um arquivo, vale o de nome
-- mais recente.

with
    orcamento as (
        select
            {{ parse_int("ano_dotacao::text") }} as ano_orcamento,
            initcap(lower(trim({{ target.schema }}.corrigir_mojibake(agrupamento::text)))) as regiao,
            {{ parse_numeric("orcamento_original::text", "numeric(17, 2)") }} as orcamento_original,
            {{ parse_numeric("orcamento_final::text", "numeric(17, 2)") }} as orcamento_final,
            {{ parse_numeric("orcamento_alocado::text", "numeric(17, 2)") }} as orcamento_alocado,
            nullif(trim(arquivo_de_origem::text), '') as arquivo_de_origem,
            cast(filename as varchar) as arquivo_lake
        from {{ ref("bronze_shpt_fgts_orcamento_final") }}
        where upper({{ target.schema }}.corrigir_mojibake(programa::text)) like 'PR%MORADIA'
    )

select distinct on (ano_orcamento, regiao)
    ano_orcamento,
    regiao,
    orcamento_original,
    orcamento_final,
    orcamento_alocado,
    arquivo_de_origem
from orcamento
where ano_orcamento is not null
order by ano_orcamento, regiao, arquivo_lake desc
