{#
    De onde vem a frente do MCMV (`frente_mcmv`) de cada família da silver histórica.

    Cada família declara as fontes, na ordem de precedência (vale a primeira preenchida):
    - `coluna`: o texto de produto, modalidade ou subprograma, traduzido pelo seed
      dados_historicos/dominio_frente (sem caixa e sem acento). O teste
      dhist_prata_frente_dominio falha se aparecer texto sem tradução.
    - `codigos`: colunas de código do BB, traduzidas pelo seed
      dados_historicos/dominio_frente_codigo, que restringe cada valor à sua coluna (o "1" da
      faixa não é o "1" do produto). Cada tradução foi medida contra a frente já conhecida do
      empreendimento, sem exceção (17/11/2026; a evidência está no seed).
    - `unica`: a família é de uma frente só. Conferida pela chave contra uma fonte que tem a
      frente (17/11/2026): INT054 e entrada auxiliar do FAR contra a modalidade da
      entrega_bb_pj, INT040 contra o relatório executivo e o SNH pelo APF. A entrada_bb é
      Financiada, não FAR (como o notebook 23 propunha): o público-alvo é todo Faixa II e III,
      e os códigos que também estão na entrega_bb_pj são 100% FGTS (a apoio à produção).
    - `chave`: herda pela chave (`apf`, `codigo_empreendimento_bb`, `codigo_contrato_pf`) a
      frente que as outras famílias dão a ela, em prata_dhist_frente_por_chave. Família que
      herda não alimenta essa tabela (senão haveria ciclo).

    Família fora daqui (ou linha sem fonte preenchida): `frente_mcmv` nula. São os relatórios
    agregados, que não descem ao empreendimento. `frente_mcmv_origem` diz de onde veio a
    frente de cada linha.
#}
{% macro frente_dhist() %}
    {{
        return(
            {
                "relatorio_executivo_caixa": {"coluna": "produto"},
                "base_pf_pj_caixa": {"coluna": "produto"},
                "bext": {"coluna": "produto"},
                "bext_agregado_caixa": {"coluna": "produto"},
                "contratacao_caixa_2010": {"coluna": "produto"},
                "contratacao_pf_caixa": {"coluna": "siaci_nome_do_produto_pf"},
                "contratacao_pf_total_caixa": {"coluna": "nome_do_produto_pf"},
                "demanda_far_caixa": {"coluna": "produto"},
                "desligados_caixa": {"coluna": "produto"},
                "empreendimentos_contratados_caixa": {"coluna": "produto"},
                "entrada_bb_aux_far": {"coluna": "subprograma"},
                "entrega_bb_pj": {"coluna": "modalidade", "codigos": ["codigo_produto"]},
                "pmcmv_parte2_caixa": {"coluna": "produto"},
                "pnhr_bb": {"coluna": "subprograma"},
                "propostas_recebidas_caixa": {"coluna": "produto"},
                "relatorio_cidades_caixa": {"coluna": "produto"},
                "relatorio_executivo_municipio_caixa": {"coluna": "produto"},
                "rural_caixa": {"coluna": "produto"},
                "snh_af_bb": {"coluna": "modalidade"},
                "snh_af_caixa": {"coluna": "modalidade"},
                "int040_far_caixa": {"unica": "FAR"},
                "int054_far_bb": {"unica": "FAR"},
                "entrada_bb": {"unica": "Financiada"},
                "propostas_far_caixa": {"unica": "FAR"},
                "previsao_entrega_far_caixa": {"unica": "FAR"},
                "entrega_bb_pf_pf": {"unica": "Financiada"},
                "tab_contratos_pf_fgts_bb": {"unica": "Financiada"},
                "tab_beneficiarios_fgts_bb": {"unica": "Financiada"},
                "entrega_bb_pj_pf": {"codigos": ["codigo_faixa"]},
                "tab_operacoes_bb": {
                    "codigos": ["codigo_subprograma", "codigo_linha_credito"],
                    "chave": ["apf", "codigo_empreendimento_bb"],
                },
                "datas_entregas_pj_caixa": {"chave": ["apf"]},
                "snh_entregas_af_caixa": {"chave": ["apf"]},
                "snh_entregas_af_bb": {"chave": ["apf"]},
                "relatorio_cidades_entregas_caixa": {"chave": ["codigo_contrato_pf"]},
                "tab_andamento_obras_bb": {"chave": ["apf", "codigo_empreendimento_bb"]},
                "tab_caracterizacoes_entornos_bb": {"chave": ["apf", "codigo_empreendimento_bb"]},
                "tab_contratos_pj_bb": {"chave": ["apf", "codigo_empreendimento_bb"]},
                "tab_unidades_concluidas_bb": {"chave": ["apf", "codigo_empreendimento_bb"]},
            }
        )
    }}
{% endmacro %}

{#- As famílias que alimentam prata_dhist_frente_por_chave: têm a frente nos próprios dados. -#}
{% macro familias_fonte_frente() %}
    {%- set fontes = [] -%}
    {%- for f, fr in frente_dhist().items() if not fr.get("chave") -%}
        {%- do fontes.append(f) -%}
    {%- endfor -%}
    {{ return(fontes) }}
{% endmacro %}

{#- A chave normalizada (o código do BB vem com e sem zero à esquerda). -#}
{% macro frente_chave(tipo, expr) -%}
    {%- if tipo == "codigo_empreendimento_bb" -%} nullif(ltrim(trim({{ expr }}::varchar), '0'), '')
    {%- else -%} nullif(trim({{ expr }}::varchar), '')
    {%- endif -%}
{%- endmacro %}

{#- As colunas de frente no select da silver (antes da herança pela chave). -#}
{% macro frente_dhist_expr(familia) -%}
    {%- set fr = frente_dhist().get(familia, {}) -%}
    {%- set fontes = [] -%}
    {%- set rotulos = [] -%}
    {%- if fr.get("coluna") -%}
        {%- do fontes.append("frente.frente_mcmv") -%}
        {%- do rotulos.append("'coluna:" ~ fr["coluna"] ~ "'") -%}
    {%- endif -%}
    {%- for cod in fr.get("codigos", []) -%}
        {%- do fontes.append("frente_cod_" ~ loop.index ~ ".frente_mcmv") -%}
        {%- do rotulos.append("'codigo:" ~ cod ~ "'") -%}
    {%- endfor -%}
    {%- if fr.get("unica") -%}
        {%- do fontes.append("'" ~ fr["unica"] ~ "'::varchar") -%}
        {%- do rotulos.append("'familia'") -%}
    {%- endif -%}
    {%- if fontes -%}
        coalesce({{ fontes | join(", ") }}) as frente_mcmv,
        case
            {%- for f in fontes %}
                when {{ f }} is not null then {{ rotulos[loop.index0] }}
            {% endfor %}
        end as frente_mcmv_origem
    {%- else -%} null::varchar as frente_mcmv, null::varchar as frente_mcmv_origem
    {%- endif -%}
{%- endmacro %}

{#- Os joins dos seeds de frente (texto e códigos). -#}
{% macro frente_dhist_join(familia, colunas) -%}
    {%- set fr = frente_dhist().get(familia, {}) -%}
    {%- if fr.get("coluna") -%}
        {%- set c = colunas | selectattr("nome", "equalto", fr["coluna"]) | first -%}
        {%- if not c -%}
            {{
                exceptions.raise_compiler_error(
                    "frente_dhist: " ~ familia ~ " não tem a coluna " ~ fr["coluna"]
                )
            }}
        {%- endif %}
        left join
            (
                select distinct strip_accents(lower(trim(valor_bruto))) as chave, frente_mcmv
                from {{ rel_pgduckdb(ref("dominio_frente")) }}
            ) as frente
            on frente.chave = strip_accents(lower({{ dhist_origem(c.origens) }}))
    {%- endif -%}
    {%- for cod in fr.get("codigos", []) -%}
            {%- set c = colunas | selectattr("nome", "equalto", cod) | first -%}
            {%- if not c -%}
                {{ exceptions.raise_compiler_error("frente_dhist: " ~ familia ~ " não tem " ~ cod) }}
            {%- endif %}
        left join
            {{ rel_pgduckdb(ref("dominio_frente_codigo")) }} as frente_cod_{{ loop.index }}
            on frente_cod_{{ loop.index }}.coluna = '{{ cod }}'
            and frente_cod_{{ loop.index }}.codigo = trim({{ dhist_origem(c.origens) }})
    {%- endfor -%}
{%- endmacro %}

{#- A herança pela chave, por fora do select da família (a chave pode ser coluna calculada,
    como o `apf` que vem da ponte do BB). -#}
{% macro frente_dhist_heranca(familia, sql_familia) -%}
    {%- set chaves = frente_dhist().get(familia, {}).get("chave", []) -%}
    {%- if not chaves -%} {{ sql_familia }}
    {%- else -%}
        with
            linhas as ({{ sql_familia }}),
            por_chave as (
                select tipo_chave, chave, frente_mcmv
                from {{ rel_pgduckdb(ref("prata_dhist_frente_por_chave")) }}
            )
        select
            linhas.* replace (
                coalesce(
                    linhas.frente_mcmv
                    {%- for k in chaves %}, chave_{{ loop.index }}.frente_mcmv
                    {% endfor %}
                ) as frente_mcmv,
                case
                    when linhas.frente_mcmv is not null
                    then linhas.frente_mcmv_origem
                    {%- for k in chaves %}
                        when chave_{{ loop.index }}.frente_mcmv is not null then 'chave:{{ k }}'
                    {%- endfor %}
                end as frente_mcmv_origem
            )
        from linhas
        {%- for k in chaves %}
            left join
                por_chave as chave_{{ loop.index }}
                on chave_{{ loop.index }}.tipo_chave = '{{ k }}'
                and chave_{{ loop.index }}.chave = {{ frente_chave(k, "linhas." ~ k) }}
        {%- endfor %}
    {%- endif -%}
{%- endmacro %}
