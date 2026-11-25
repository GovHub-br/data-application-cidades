-- Frente do MCMV por chave (APF, código de empreendimento do BB, contrato PF), a partir das
-- famílias que têm a frente nos próprios dados (macros/dados_historicos/frente.sql). As
-- famílias de eventos e as tabelas do BB de 2013 herdam daqui. Uma linha por chave.
-- Quando a mesma chave aparece com mais de uma frente (0,1% dos APFs), vale a frente da
-- competência mais recente (e, no mesmo mês, a mais frequente); `n_frentes` e `frentes`
-- guardam o conflito para revisão.
{%- set partes = [] %}
{%- for f in familias_fonte_frente() %}
    {%- set dp = de_para_dhist()[f] %}
    {%- set nomes = dp["colunas"] | map(attribute="nome") | list %}
    {%- for tipo in ["apf", "codigo_empreendimento_bb", "codigo_contrato_pf"] %}
        {%- if tipo in nomes or (tipo == "apf" and dp.get("ponte_apf")) %}
            {%- do partes.append(
                "select '" ~ tipo ~ "' as tipo_chave, "
                ~ frente_chave(tipo, tipo)
                ~ " as chave, frente_mcmv, competencia, '"
                ~ f
                ~ "' as familia from "
                ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
                ~ " where frente_mcmv is not null"
            ) %}
        {%- endif %}
    {%- endfor %}
{%- endfor %}
{% call duckdb_query() %}
with

    todas as (
        {{ partes | join("\nunion all\n") }}
    ),

    por_frente as (
        select
            tipo_chave,
            chave,
            frente_mcmv,
            max(competencia) as competencia_mais_recente,
            count(*) as linhas,
            string_agg(distinct familia, ', ' order by familia) as familias
        from todas
        where chave is not null
        group by 1, 2, 3
    ),

    ordenadas as (
        select
            *,
            row_number() over (
                partition by tipo_chave, chave
                order by competencia_mais_recente desc nulls last, linhas desc, frente_mcmv
            ) as ordem,
            count(*) over (partition by tipo_chave, chave) as n_frentes,
            string_agg(frente_mcmv, ', ') over (partition by tipo_chave, chave) as frentes
        from por_frente
    )

select
    tipo_chave,
    chave,
    frente_mcmv,
    n_frentes,
    frentes,
    competencia_mais_recente,
    familias
from ordenadas
where ordem = 1
{% endcall %}
