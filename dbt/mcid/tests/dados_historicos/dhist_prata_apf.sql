-- Validação 2 (silver): o APF, chave da cadeia do empreendimento, é numérico.
-- - Caixa: 8 dígitos (2 APFs do relatório executivo têm 7 em todas as épocas; nenhum APF de
-- 8 começa com 0, então não é zero perdido). Só falta no layout do relatório executivo
-- anterior a 2012-07, que não tinha a coluna.
-- - BB: 6 a 8 dígitos (a SNH BB tem 235 APFs de 6 e 853 de 7, medido em 16/11/2026).
-- - Famílias com `ponte_apf`: o APF vem do INT054 e é nulo fora da ponte (research R1).
-- - int054_far_bb: 3 dos 202 empreendimentos vêm sem APF na fonte (nu_apf vazio).
-- Só entram as famílias que têm a coluna `apf`.
{% call duckdb_query() %}
{%- set dp = de_para_dhist() %}
{%- set familias = [] %}
{%- for f in familias_prata_dhist() %}
    {%- if dp[f].get("ponte_apf") or "apf" in (dp[f]["colunas"] | map(attribute="nome") | list) %}
        {%- do familias.append(f) %}
    {%- endif %}
{%- endfor %}
{% for f in familias %}
    select '{{ f }}' as familia, saida, competencia, apf, count(*) as n
    from {{ rel_pgduckdb(ref("prata_dhist_" ~ f)) }}
    where
        (
            apf is not null
            and not regexp_matches(apf, '{{ "^\\d{6,8}$" if f.endswith("_bb") else "^\\d{7,8}$" }}')
        )
        {%- if not dp[f].get("ponte_apf") and f != "int054_far_bb" %}
            or (
                apf is null
                {%- if f == "relatorio_executivo_caixa" %}
                    and competencia >= date '2012-07-01'
                {% endif %}
            )
        {%- endif %}
    group by all {{ 'union all' if not loop.last }}
{% endfor %}
{% endcall %}
