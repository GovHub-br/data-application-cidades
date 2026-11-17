-- Tudo que é APF se chama `apf` (ou `apf_vinculado`, o APF de outro contrato ligado a este).
-- Para cada coluna de texto da silver com outro nome, conta quantos dos seus valores
-- distintos (amostra das primeiras 200 mil linhas) são APFs conhecidos (relatório executivo,
-- INT040 e painel SNH). Devolve a coluna quando mais de 30% de ao menos 20 valores são APF.
-- SQL do Postgres, fora do envelope.
{%- set fontes = [
    "prata_dhist_relatorio_executivo_caixa",
    "prata_dhist_int040_far_caixa",
    "prata_dhist_snh_af_caixa",
    "prata_dhist_snh_af_bb",
] %}
{%- set nao_apf = ["apf", "apf_vinculado", "familia", "saida", "arquivo_raw"] %}
with

    apfs as (
        {% for t in fontes %}
            select distinct ltrim(apf, '0') as v
            from {{ ref(t) }}
            where apf is not null {{ "union" if not loop.last }}
        {% endfor %}
    ),

    contagem as (
        {% set partes = [] %}
        {% for f, dp in de_para_dhist().items() %}
            {% for c in dp["colunas"] if c.tipo == "texto" and c.nome not in nao_apf %}
                {% do partes.append(
                    "select '"
                    ~ f
                    ~ "' as familia, '"
                    ~ c.nome
                    ~ "' as coluna, count(*) as valores, count(*) filter (where v in (select v from apfs)) as apf"
                    ~ ' from (select distinct ltrim(trim("'
                    ~ c.nome
                    ~ "\"), '0') as v from (select \""
                    ~ c.nome
                    ~ '" from '
                    ~ ref("prata_dhist_" ~ f)
                    ~ ' limit 200000) as x where nullif(trim("'
                    ~ c.nome
                    ~ "\"), '') is not null) as s"
                ) %}
            {% endfor %}
        {% endfor %}
        {{ partes | join(" union all ") }}
    )

select *
from contagem
where valores >= 20 and apf > 0.3 * valores
