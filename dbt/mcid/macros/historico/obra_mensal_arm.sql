{#
    Projeção do contrato de obra_mensal a partir de UMA das 3 bronzes da família
    (change enriquecer-quantidades-uh-e-sinais-obra-historico, D4). Desde a
    change consolidar-schemas-historico-reloginho (D2) NÃO alimenta mais um
    modelo autônomo (o modelo de obra mensal autônomo foi removido) — serve os
    macros historico_obra_mensal_rows() / _vals() abaixo, que costuram a obra
    às 3 silvers de frente (braço de criação de linha + left join das 22 col).

    As 3 bronzes da família obra_mensal têm schemas DIVERGENTES (FAR:
    `dt_movimento` / `co_situacao_obra`; FDS/RURAL: `dh_movimento` /
    `co_situacao_operacao` + `co_andamento_operacao`; retomada e legalização com
    nomes distintos). Este macro projeta o contrato comum da silver a partir de
    QUALQUER uma das 3 relações, resolvendo o nome real por lista de aliases via
    coalesce_present[_parsed] — que introspecciona a bronze materializada e
    compila a coluna ausente como NULL do tipo certo (tolera drift de schema do
    parquet e a divergência entre frentes).

    Tipagem (task 5.2): pc_* -> parse_hist_double; qt_* -> parse_hist_bigint;
    dt_*/dh_* -> parse_hist_date; co_*/detalhe_* -> texto cru.
#}
{% macro silver_obra_mensal_arm(rel, frente_mcmv) %}
        select
            '{{ frente_mcmv }}'::text as frente_mcmv,
            nullif(trim(nu_apf::text), '')::text as apf,
            dt_referencia,
            {{ coalesce_present_parsed(
                rel, ['dt_movimento', 'dh_movimento'], 'parse_hist_date', 'date'
            ) }} as dt_movimento,
            -- curva de obra (sinal central desta silver)
            {{ coalesce_present_parsed(rel, ['pc_obra_prevista'], 'parse_hist_double', 'double') }}
            as pc_obra_prevista,
            {{ coalesce_present_parsed(rel, ['pc_obra_realizada'], 'parse_hist_double', 'double') }}
            as pc_obra_realizada,
            -- situação da obra/operação (código cru; domínio a decodificar em follow-up)
            nullif(trim({{ coalesce_present(rel, ['co_situacao_obra', 'co_situacao_operacao']) }}), '')::text
            as co_situacao_obra,
            nullif(trim({{ coalesce_present(rel, ['co_andamento_operacao']) }}), '')::text
            as co_andamento_operacao,
            {{ coalesce_present_parsed(rel, ['dt_alteracao_situacao'], 'parse_hist_date', 'date') }}
            as dt_alteracao_situacao,
            -- ciclo de paralisação / retomada. dt_paralisacao é 0% na fonte hoje
            -- (a coluna existe, vazia) — o sinal utilizável é co_situacao_obra / texto.
            {{ coalesce_present_parsed(rel, ['dt_paralisacao'], 'parse_hist_date', 'date') }}
            as dt_paralisacao,
            nullif(trim({{ coalesce_present(rel, ['co_classificacao_paralisados']) }}), '')::text
            as co_classificacao_paralisado,
            nullif(trim({{ coalesce_present(rel, ['detalhe_paralisacao_retomada', 'no_detalhe_paralisacao_retomada']) }}), '')::text
            as detalhe_paralisacao,
            {{ coalesce_present_parsed(
                rel, ['dt_previsao_conclusao_obra_retomada', 'dt_prev_conclusao_obra_retomada'], 'parse_hist_date', 'date'
            ) }} as dt_previsao_conclusao_obra_retomada,
            {{ coalesce_present_parsed(rel, ['dt_conclusao_obra_retomada'], 'parse_hist_date', 'date') }}
            as dt_conclusao_obra_retomada,
            -- decomposição de UH por estado
            {{ coalesce_present_parsed(rel, ['qt_uh_concluidas'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_concluidas,
            {{ coalesce_present_parsed(rel, ['qt_uh_alienada'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_alienadas,
            {{ coalesce_present_parsed(rel, ['qt_uh_sem_habitese'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_sem_habitese,
            {{ coalesce_present_parsed(rel, ['qt_uh_em_construcao_parcial'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_construcao_parcial,
            {{ coalesce_present_parsed(rel, ['qt_uh_ociosas_retomadas'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_ociosas_retomadas,
            {{ coalesce_present_parsed(
                rel, ['qt_unidades_habitacionais_invadidas', 'qt_uh_ocupacao_irregular'], 'parse_hist_bigint', 'bigint'
            ) }} as qt_unidades_habitacionais_invadidas,
            -- marcos
            {{ coalesce_present_parsed(rel, ['dt_conclusao_obra'], 'parse_hist_date', 'date') }}
            as dt_conclusao_obra,
            {{ coalesce_present_parsed(rel, ['dt_legalizacao', 'dt_legalizacao_reg'], 'parse_hist_date', 'date') }}
            as dt_legalizacao,
            {{ coalesce_present_parsed(
                rel, ['dt_previsao_entrega_do_empreendimento', 'dt_prev_entrega_emprend'], 'parse_hist_date', 'date'
            ) }} as dt_previsao_entrega,
            {{ coalesce_present_parsed(rel, ['dt_entrega_do_empreendimento'], 'parse_hist_date', 'date') }}
            as dt_entrega,
            -- só FAR
            {{ coalesce_present_parsed(rel, ['dt_acion_seguradora'], 'parse_hist_date', 'date') }}
            as dt_acion_seguradora,
            {{ coalesce_present_parsed(rel, ['dt_contrata_construtor_substituto'], 'parse_hist_date', 'date') }}
            as dt_contrata_construtor_substituto,
            {{ coalesce_present_parsed(rel, ['dt_repactuacao'], 'parse_hist_date', 'date') }}
            as dt_repactuacao,
            source_file,
            hash_linha,
            dt_ingest
        from {{ rel }}
        where nullif(trim(nu_apf::text), '') is not null
{% endmacro %}


{#
    ── Fusão obra_mensal → braço do union das silvers de frente ──
    Change: consolidar-schemas-historico-reloginho (D2 / C2).

    O modelo de obra mensal autônomo deixa de existir; a evolução
    mensal de obra vira MAIS UM BRAÇO do `union all by name` de cada
    prata_{far,fds,rural}_historico_empreendimento, ao lado dos braços
    INT0XX e SNH. A janela de obra_mensal (2025-12 → 2026-07) ultrapassa a das
    outras fontes: um left join descartaria 4 meses, um braço do union cria a
    linha. Nos meses só-de-obra (2026-04..07) `quantidade_uh`,
    `valor_contratado` e `valor_desembolsado` saem NULL — a cauda de estoque é
    declaradamente nula (C2), sem carry-forward.

    As 22 colunas novas do contrato comum (as de obra_mensal sem par):
#}
{% macro historico_obra_cols_novas() %}
    {{ return([
        'pc_obra_prevista', 'pc_obra_realizada', 'co_situacao_obra',
        'co_andamento_operacao', 'dt_alteracao_situacao', 'dt_paralisacao',
        'co_classificacao_paralisado', 'detalhe_paralisacao',
        'dt_previsao_conclusao_obra_retomada', 'dt_conclusao_obra_retomada',
        'qt_uh_concluidas', 'qt_uh_alienadas', 'qt_uh_sem_habitese',
        'qt_uh_construcao_parcial', 'qt_uh_ociosas_retomadas',
        'qt_unidades_habitacionais_invadidas', 'dt_legalizacao', 'dt_entrega',
        'dt_acion_seguradora', 'dt_contrata_construtor_substituto',
        'dt_repactuacao', 'id_obra_snapshot'
    ]) }}
{% endmacro %}

{#- Tipo DuckDB de cada uma das 22 (para NULLs tipados no braço `carregado`). -#}
{% macro historico_obra_cols_tipos() %}
    {{ return({
        'pc_obra_prevista': 'double', 'pc_obra_realizada': 'double',
        'co_situacao_obra': 'varchar', 'co_andamento_operacao': 'varchar',
        'dt_alteracao_situacao': 'date', 'dt_paralisacao': 'date',
        'co_classificacao_paralisado': 'varchar', 'detalhe_paralisacao': 'varchar',
        'dt_previsao_conclusao_obra_retomada': 'date', 'dt_conclusao_obra_retomada': 'date',
        'qt_uh_concluidas': 'bigint', 'qt_uh_alienadas': 'bigint',
        'qt_uh_sem_habitese': 'bigint', 'qt_uh_construcao_parcial': 'bigint',
        'qt_uh_ociosas_retomadas': 'bigint', 'qt_unidades_habitacionais_invadidas': 'bigint',
        'dt_legalizacao': 'date', 'dt_entrega': 'date', 'dt_acion_seguradora': 'date',
        'dt_contrata_construtor_substituto': 'date', 'dt_repactuacao': 'date',
        'id_obra_snapshot': 'varchar'
    }) }}
{% endmacro %}

{#-
    ── Estratégia (D2/C2) ──
    obra_mensal entra nas 3 silvers de DUAS formas complementares:

    1. CRIA LINHA nos meses só-de-obra (2026-04..07) — um braço mínimo no
       `union all by name` de `unioned` (só chave + identidade + proveniência;
       o `by name` completa o contrato com NULL). É a razão de subir o teto do
       eixo de 2026-03 para 2026-07. `historico_obra_mensal_rows()`.

    2. ADICIONA AS 22 COLUNAS por LEFT JOIN no grão exato (frente_mcmv, apf,
       dt_referencia), num CTE `enriquecido_obra` logo antes de `resolvido` —
       barato (build side ~12k linhas) e não engorda os CTEs de janela.
       `historico_obra_mensal_vals()`.

    Feito assim para não pagar 22 window aggregates sobre o intermediário de
    ~700k linhas do Rural (a máquina de build tem RAM apertada). O join cobre
    tanto as linhas só-de-obra quanto as de meses sobrepostos onde a linha
    SNH/SFTP venceu a dedup.
-#}

{#- Braço mínimo de criação de linha para `unioned` (union all by name). -#}
{% macro historico_obra_mensal_rows(rel, frente_mcmv, linha_mcmv) %}
        select
            'Minha Casa Minha Vida'::text as programa,
            o.frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            '{{ linha_mcmv }}'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            null::text as agente_financeiro,
            o.apf,
            o.apf as codigo_empreendimento,
            o.dt_referencia,
            o.dt_movimento,
            'obra_mensal'::text as fonte_serie,
            'sharepoint:MONIT_MOV_OBRA'::text as fonte_tabela,
            o.source_file,
            o.hash_linha,
            o.dt_ingest
        from (
{{ silver_obra_mensal_arm(rel, frente_mcmv) }}
        ) o
        where o.apf is not null
        qualify row_number() over (
            partition by o.frente_mcmv, o.apf, o.dt_referencia
            order by o.dt_movimento desc nulls last, o.source_file desc
        ) = 1
{% endmacro %}

{#- Fonte do left join: 1 linha por (frente_mcmv, apf, dt_referencia) com as
    22 colunas + id_obra_snapshot. Pequena. -#}
{% macro historico_obra_mensal_vals(rel, frente_mcmv) %}
        select
            o.frente_mcmv,
            o.apf,
            o.dt_referencia,
            o.pc_obra_prevista,
            o.pc_obra_realizada,
            o.co_situacao_obra,
            o.co_andamento_operacao,
            o.dt_alteracao_situacao,
            o.dt_paralisacao,
            o.co_classificacao_paralisado,
            o.detalhe_paralisacao,
            o.dt_previsao_conclusao_obra_retomada,
            o.dt_conclusao_obra_retomada,
            o.qt_uh_concluidas,
            o.qt_uh_alienadas,
            o.qt_uh_sem_habitese,
            o.qt_uh_construcao_parcial,
            o.qt_uh_ociosas_retomadas,
            o.qt_unidades_habitacionais_invadidas,
            o.dt_legalizacao,
            o.dt_entrega,
            o.dt_acion_seguradora,
            o.dt_contrata_construtor_substituto,
            o.dt_repactuacao,
            md5(concat_ws(
                '|', 'obra', o.frente_mcmv, coalesce(o.apf, ''), o.dt_referencia::text
            )) as id_obra_snapshot
        from (
{{ silver_obra_mensal_arm(rel, frente_mcmv) }}
        ) o
        where o.apf is not null
        qualify row_number() over (
            partition by o.frente_mcmv, o.apf, o.dt_referencia
            order by o.dt_movimento desc nulls last, o.source_file desc
        ) = 1
{% endmacro %}

{#- CTE `enriquecido_obra`: acrescenta as 22 por left join. Vai entre
    `enriquecido_dominio` (ou `enriquecido_id` no FDS) e `resolvido`. -#}
{% macro historico_obra_enriquecido(rel_bronze, frente_mcmv, cte_base) %}
    enriquecido_obra as (
        select
            b.*
            {%- for c in historico_obra_cols_novas() %}
            , ov.{{ c }} as {{ c }}_obra
            {%- endfor %}
        from {{ cte_base }} b
        left join (
{{ historico_obra_mensal_vals(rel_bronze, frente_mcmv) }}
        ) ov
            on ov.frente_mcmv = b.frente_mcmv
            and ov.apf = b.apf
            and ov.dt_referencia = b.dt_referencia
    ),
{% endmacro %}

{#- Projeção das 22 no CTE `resolvido` (lê de `enriquecido_obra`). Vírgula
    inicial; vai logo após `dt_assinatura_projeto`. -#}
{% macro historico_obra_cols_resolvido() %}
    {%- for c in historico_obra_cols_novas() %}
    , {{ c }}_obra as {{ c }}
    {%- endfor %}
{% endmacro %}

{#- Projeção das 22 no CTE `carregado` de historico_silver_tail() — NULL tipado
    (a linha carregada é carry-forward só da janela SNH, obra não propaga).
    Vírgula inicial; vai logo após `dt_assinatura_projeto`. -#}
{% macro historico_obra_cols_carregado() %}
    {%- set tipos = historico_obra_cols_tipos() %}
    {%- for c in historico_obra_cols_novas() %}
            , cast(null as {{ tipos[c] }}) as {{ c }}
    {%- endfor %}
{% endmacro %}

{#- Projeção das 22 no SELECT final de historico_silver_tail() (alias `p`).
    Vírgula inicial. -#}
{% macro historico_obra_cols_final() %}
    {%- for c in historico_obra_cols_novas() %}
    , p.{{ c }}
    {%- endfor %}
{% endmacro %}

