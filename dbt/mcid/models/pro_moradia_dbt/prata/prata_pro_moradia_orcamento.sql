{{ config(materialized="table") }}

-- Prata: Orçamento do FGTS para o Pró-Moradia, por ano e região
-- Fonte: bronze_shpt_fgts_orcamento_final (prestação de contas anual do FGTS), filtrada para o
-- programa Pró-Moradia. É o "Planejamento" do ciclo: quanto o Conselho Curador reservou antes
-- de qualquer contrato.
--
-- Grão: ano de dotação × região. A região vem em duas grafias conforme o programa (`Nordeste`
-- e `NORDESTE`); `initcap` unifica. Se o mesmo ano chegar em mais de um arquivo, vale o de nome
-- mais recente.
--
-- A bronze empilha um arquivo por ano, e o layout da prestação de contas mudou entre os anos: o
-- 2024 chama a região de `agrupamento`, e ao menos um outro ano não (o teste de layout da bronze
-- avisou, e a primeira execução deixou região nula). Em vez de fixar um nome, a região sai da
-- primeira coluna não nula entre as que falam de região/agrupamento na bronze materializada.
-- Linha que ainda assim fica sem região — total geral do programa, por exemplo — é descartada:
-- somada às regiões, ela contaria o orçamento duas vezes.

{%- set colunas_regiao = [] -%}
{%- if execute -%}
    {%- for c in adapter.get_columns_in_relation(ref("bronze_shpt_fgts_orcamento_final")) -%}
        {%- if modules.re.search("agrup|regi", c.name | lower) -%}
            {%- do colunas_regiao.append(c.name) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}
{%- if not colunas_regiao -%}{%- do colunas_regiao.append("agrupamento") -%}{%- endif %}

with
    orcamento as (
        select
            {{ parse_int("ano_dotacao::text") }} as ano_orcamento,
            initcap(lower(nullif(trim({{ target.schema }}.corrigir_mojibake(coalesce(
                {%- for c in colunas_regiao %}nullif(trim("{{ c }}"::text), ''){{ ', ' if not loop.last }}{% endfor -%}
            ))), ''))) as regiao,
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
where ano_orcamento is not null and regiao is not null
order by ano_orcamento, regiao, arquivo_lake desc
