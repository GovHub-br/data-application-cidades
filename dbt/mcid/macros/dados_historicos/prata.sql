{#
    Silver de uma família dos dados históricos: o de-para (de_para_dhist) aplicado sobre a
    bronze, com tipos e o contexto do log do cleansed.

    - Cada coluna da silver é o primeiro valor não vazio entre as origens (o cleansed usa
      texto vazio, não nulo), com o tipo aplicado por try_cast. O cleansed já escreveu os
      valores de um jeito só (ponto decimal, datas AAAA-MM-DD, sem ".0"), então o cast é
      direto; o teste dhist_prata_tipagem_sem_perda confere que nenhum valor se perdeu nele.
    - `ibge`: o código com 7 dígitos; o de 6 (sem o dígito verificador) é completado pelo
      seed ibge_municipio. O valor da fonte fica em `<nome>_fonte` (o "0" do painel SNH é
      código de ausente e fica nulo).
    - `ano_mes`: AAAAMM na fonte, vira o 1º dia do mês.
    - `valor_calculado`: valor que a fonte calculou (média, rateio) e guarda com mais de 4
      casas; vai como double para não arredondar.
    - `data_excel`: data que em algum arquivo veio como serial do Excel (5 dígitos, dias
      desde 1899-12-30), como o dt_aclt_prpt_eprd do PNHR.
    - `data` e `data_excel`: 1899-12-30, o dia zero do Excel, é data vazia na fonte e fica nula
      (as entregas do relatório de cidades de 2015-05 gravam 1899-12-30 00:04:09).
    - `latitude_br` e `longitude_br`: coordenada do Brasil, normalizada com o par da mesma
      linha (`par`) e a UF (`uf`, ou os dois primeiros dígitos do código IBGE em `ibge`). As
      fontes gravam sem sinal, ×100, ÷100 ou com latitude e longitude trocadas. No Brasil a
      longitude em módulo (28,8 a 74,2) é sempre maior que a latitude (0 a 34): o maior módulo
      é a longitude, que fica negativa, e a escala põe esse módulo entre 1 e 180. A latitude
      mantém o sinal da fonte; sem sinal, é negativa, menos em RR e AP. No AP sem sinal a
      latitude pode estar errada perto do Equador: `<nome>_sinal_incerto` marca. O que ainda
      cai fora do Brasil (0,0 é ausente) fica nulo; o valor da fonte segue na bronze.
    - `percentual_auto`: percentual cuja escala muda por arquivo (BB). Se todos os valores da
      `saida` estão em [0, 1], o arquivo é 0–1 e sai ×100; senão já é 0–100. O `percentual`
      da Caixa não muda de escala.
    - `ponte_apf` (opcional, na família): nome da coluna canônica com o código de
      empreendimento do BB; acrescenta `apf` pelo INT054 (prata_dhist_ponte_bb_apf), nulo
      fora dos códigos da ponte. A ponte tem chave única, então o join não multiplica linhas.
    - `situacao`: o texto da fonte e a situação canônica (`<nome>_canonica`), pelos seeds
      data_quality/dominio_status (compartilhado) e dados_historicos/dominio_status_extra.
    - `frente_mcmv` (FAR, Entidades, Rural, SUB50, Financiada) e `frente_mcmv_origem`, em
      toda família: do texto de produto/modalidade, dos códigos do BB, da frente única da
      família ou herdada pela chave (macros/dados_historicos/frente.sql).
    - Contexto pelo log, ligado pela `saida`: `competencia` (1º dia do mês) e `data_corte`.
    - O grão é o da fonte: uma linha da silver por linha do cleansed (`saida`,
      `_linha_origem`). Agregar é da gold.
#}
{% macro dhist_origem(origens) -%}
    coalesce(
        {#- o Postgres corta o nome da coluna da bronze em 63 caracteres (NAMEDATALEN) -#}
        {%- for o in origens %}
            nullif(trim(b."{{ o[:63] }}"), ''){{ ', ' if not loop.last }}
        {% endfor -%}
    )
{%- endmacro %}

{% macro dhist_tipo(expr, tipo) -%}
    {%- if tipo == "inteiro" -%} try_cast({{ expr }} as bigint)
    {%- elif tipo == "ano" -%} try_cast({{ expr }} as integer)
    {%- elif tipo == "valor" -%} try_cast({{ expr }} as decimal(20, 4))
    {%- elif tipo in ("percentual", "percentual_auto") -%} try_cast({{ expr }} as decimal(12, 6))
    {%- elif tipo in ("coordenada", "latitude_br", "longitude_br", "valor_calculado") -%}
        try_cast({{ expr }} as double)
    {%- elif tipo == "data_excel" -%}
        nullif(
            coalesce(
                try_cast(try_cast({{ expr }} as timestamp) as date),
                case
                    when regexp_full_match({{ expr }}, '\d{5}')
                    then date '1899-12-30' + cast({{ expr }} as integer)
                end
            ),
            date '1899-12-30'
        )
    {%- elif tipo == "data" -%}
        nullif(try_cast(try_cast({{ expr }} as timestamp) as date), date '1899-12-30')
    {%- elif tipo == "ano_mes" -%} try_cast(try_strptime({{ expr }}, '%Y%m') as date)
    {%- else -%} {{ expr }}
    {%- endif -%}
{%- endmacro %}

{% macro dhist_coordenada_br(c, colunas, parte) -%}
    {%- set par = colunas | selectattr("nome", "equalto", c.par) | first -%}
    {%- set lat_col, lon_col = (c, par) if c.tipo == "latitude_br" else (par, c) -%}
    {%- set la = dhist_tipo(dhist_origem(lat_col.origens), "coordenada") -%}
    {%- set lo = dhist_tipo(dhist_origem(lon_col.origens), "coordenada") -%}
    {%- if lat_col.uf is defined -%}
        {%- set uf = (
            "upper("
            ~ dhist_origem(
                (colunas | selectattr("nome", "equalto", lat_col.uf) | first).origens
            )
            ~ ")"
        ) -%}
    {%- else -%}
        {%- set uf = (
            "case substr("
            ~ dhist_origem(
                (colunas | selectattr("nome", "equalto", lat_col.ibge) | first).origens
            )
            ~ ", 1, 2) when '14' then 'RR' when '16' then 'AP' end"
        ) -%}
    {%- endif -%}
    {%- set lat_fonte = (
        "(case when abs("
        ~ la
        ~ ") > abs("
        ~ lo
        ~ ") then "
        ~ lo
        ~ " else "
        ~ la
        ~ " end)"
    ) -%}
    {%- set maior = "greatest(abs(" ~ la ~ "), abs(" ~ lo ~ "))" -%}
    {%- set escala = (
        "(case when "
        ~ maior
        ~ " > 180 then 0.01 when "
        ~ maior
        ~ " < 1 then 100 else 1 end)"
    ) -%}
    {%- set lat = (
        "(case when "
        ~ lat_fonte
        ~ " < 0 or coalesce("
        ~ uf
        ~ ", '') not in ('RR', 'AP') then -1 else 1 end * abs("
        ~ lat_fonte
        ~ ") * "
        ~ escala
        ~ ")"
    ) -%}
    {%- set lon = "(-" ~ maior ~ " * " ~ escala ~ ")" -%}
    {%- set valido = "(" ~ lat ~ " between -34 and 5.5 and " ~ lon ~ " between -74.2 and -28.8)" -%}
    {%- if parte == "incerto" -%} ({{ valido }} and {{ uf }} = 'AP' and {{ lat_fonte }} >= 0)
    {%- else -%} case when {{ valido }} then {{ lat if parte == "latitude" else lon }} end
    {%- endif -%}
{%- endmacro %}

{% macro prata_dhist_sql(familia) %}
    {%- set dp = de_para_dhist()[familia] -%}
    {%- set colunas = dp["colunas"] -%}
    {%- set ponte = dp.get("ponte_apf") -%}
    with

        b as (select * from {{ rel_pgduckdb(ref("bronze_dhist_" ~ familia)) }}),

        log as (
            select saida, competencia, data_corte
            from {{ rel_pgduckdb(ref("bronze_dhist_cleansed_log")) }}
            where status = 'tratado'
        )

    select
        b.familia,
        b.saida,
        b._linha_origem,
        b._source_file as arquivo_raw,
        try_cast(log.competencia || '-01' as date) as competencia,
        log.data_corte,
        {%- for c in colunas %}
            {%- set origem = dhist_origem(c.origens) %}
            {%- if c.tipo == "ibge" %}
                case
                    when length({{ origem }}) = 7
                    then {{ origem }}
                    when length({{ origem }}) = 6
                    then cast(ibge_{{ loop.index }}.cod_ibge as varchar)
                end as "{{ c.nome }}",
                {{ origem }} as "{{ c.nome }}_fonte"
            {%- elif c.tipo == "situacao" %}
                {{ origem }} as "{{ c.nome }}",
                status_{{ loop.index }}.situacao_canonica as "{{ c.nome }}_canonica"
            {%- elif c.tipo == "latitude_br" %}
                {{ dhist_coordenada_br(c, colunas, "latitude") }} as "{{ c.nome }}",
                {{ dhist_coordenada_br(c, colunas, "incerto") }} as "{{ c.nome }}_sinal_incerto"
            {%- elif c.tipo == "longitude_br" %}
                {{ dhist_coordenada_br(c, colunas, "longitude") }} as "{{ c.nome }}"
            {%- elif c.tipo == "percentual_auto" %}
                {%- set v = dhist_tipo(origem, "percentual") %}
                case
                    when max({{ v }}) over (partition by b.saida) <= 1
                    then {{ v }} * 100
                    else {{ v }}
                end as "{{ c.nome }}"
            {%- else %} {{ dhist_tipo(origem, c.tipo) }} as "{{ c.nome }}"
            {%- endif %}
            {{- ',' if not loop.last }}
        {%- endfor %}
        {%- if ponte %}, ponte.apf as apf {%- endif %},
        {{ frente_dhist_expr(familia) }}
    from b
    left join log on b.saida = log.saida
    {%- if ponte %}
            {%- set c_ponte = colunas | selectattr("nome", "equalto", ponte) | first %}
        left join
            {{ rel_pgduckdb(ref("prata_dhist_ponte_bb_apf")) }} as ponte
            on ponte.codigo_empreendimento_bb = ltrim({{ dhist_origem(c_ponte.origens) }}, '0')
    {%- endif %}
    {%- for c in colunas %}
        {%- if c.tipo == "ibge" %}
            left join
                {{ rel_pgduckdb(ref("ibge_municipio")) }} as ibge_{{ loop.index }}
                on substr(cast(ibge_{{ loop.index }}.cod_ibge as varchar), 1, 6)
                = {{ dhist_origem(c.origens) }}
        {%- elif c.tipo == "situacao" %}
            left join
                (
                    select distinct
                        strip_accents(lower(trim(valor_bruto))) as chave, situacao_canonica
                    from
                        (
                            select valor_bruto, situacao_canonica
                            from {{ rel_pgduckdb(ref("dominio_status")) }}
                            union all
                            select valor_bruto, situacao_canonica
                            from {{ rel_pgduckdb(ref("dominio_status_extra")) }}
                        )
                ) as status_{{ loop.index }}
                on status_{{ loop.index }}.chave
                = strip_accents(lower({{ dhist_origem(c.origens) }}))
        {%- endif %}
        {%- endfor %} {{ frente_dhist_join(familia, colunas) }}
{% endmacro %}

{#- A silver no target do banco: o SQL do DuckDB roda no pg_duckdb, dentro do envelope. -#}
{% macro prata_dhist(familia) %}
    {% call duckdb_query() %}
        {{ frente_dhist_heranca(familia, prata_dhist_sql(familia)) }}
    {% endcall %}
{% endmacro %}

{#- As famílias que já têm silver (as que estão no de-para). -#}
{% macro familias_prata_dhist() %} {{ return(de_para_dhist().keys() | list | sort) }} {% endmacro %}
