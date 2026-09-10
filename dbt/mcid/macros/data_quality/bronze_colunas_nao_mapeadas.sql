{#-
  Lista de colunas do BRONZE de empreendimento histórico que casam o padrão
  semântico do guardrail E são de fato consumidas por alguma silver/gold
  (diretamente ou via alias de macro `historico/`).

  Gerada a partir do SQL COMPILADO dos modelos — não dá para o teste varrer o
  filesystem em tempo de compilação (o `graph` do dbt não expõe `compiled_code`
  no contexto de teste). Regerar quando um mapeamento bronze->silver mudar:

      # da raiz do repo, com o eixo histórico já compilado
      # (dbt compile --select mcmv_historico_dbt), rodar helpers/
      # scripts/ ... ou o snippet em
      # models/mcmv_historico_dbt/docs/colunas-orfas-bronze-historico.md

  Se sair defasada o efeito é só um falso-positivo no `warn` (nunca falso-
  negativo perigoso: uma coluna consumida some da lista => aparece no warn =>
  alguém repara). Change: colunas-orfas-bronze-historico.
-#}
{% macro historico_bronze_aliases_consumidas() %}
    {{ return([
        'classificacao_dos_paralisados', 'co_situacao_obra', 'cod_pendencia_obra',
        'desc_situacao_contrato', 'detalhamento_da_situacao_do_empreendimento',
        'dt_assinatura', 'dt_assinatura_projeto', 'dt_contratacao', 'dt_contrato',
        'dt_efetiva_conclusao', 'dt_entrega', 'dt_inicio_obra', 'dt_legalizacao',
        'dt_primeira_entrega', 'dt_termino_obra', 'dt_ultima_entrega',
        'dt_ultima_liberacao', 'dt_ultima_liberacao_recurso',
        'no_entidade_organizadora', 'no_situacao_obra', 'nu_cnpj_entidade',
        'qt_uh_entregues', 'qt_uh_previsao_entrega', 'qt_unidade_financiadas',
        'qt_unidades', 'qt_unidades_concluidas', 'qt_unidades_entregues',
        'qt_unidades_habitacionais', 'qt_unidades_ociosas', 'qtde_uh_inicial',
        'qtde_unidades',
        'quantidade_de_uhs_contratadas_do_ano_de_referencia',
        'quantidade_de_uhs_contratadas_em_janeiro_do_ano_de_referencia',
        'quantidade_de_uhs_distratadas',
        'quantidade_de_uhs_distratadas_do_ano_de_referencia',
        'quantidade_de_uhs_distratadas_em_janeiro_do_ano_de_referencia',
        'quantidade_de_uhs_entregues_do_ano_de_referencia',
        'quantidade_de_uhs_entregues_em_janeiro_do_ano_de_referencia',
        'quantidade_de_uhs_vigentes_do_ano_de_referencia',
        'quantidade_de_uhs_vigentes_em_janeiro_do_ano_de_referencia',
        'situacao_do_empreendimento', 'situacao_empreendimento', 'situacao_gefus',
        'situacao_obra', 'situacao_obra_gefus', 'situacao_retomada',
        'uh_contratadas', 'uh_entregues', 'uh_vigentes', 'uhs_contratadas',
        'uhs_entregues', 'uhs_vigentes', 'unidades_habitacionais_a_serem_entregues',
        'valor_contratado', 'valor_desembolsado',
        'valor_desembolsado_do_ano_de_referencia',
        'vr_investimento', 'vr_investimento_pnhr', 'vr_liberado', 'total_liberado_far'
    ]) }}
{% endmacro %}


{#-
  Teste genérico (guardrail): lista as colunas de um bronze de empreendimento do
  eixo histórico que têm SINAL SEMÂNTICO de negócio mas não são consumidas por
  nenhuma silver nem registradas no seed `colunas_bronze_ignoradas`.

  `coalesce_present(bronze, [aliases])` que monta o contrato das silvers é uma
  ALLOWLIST — uma coluna do parquet fora da lista de aliases some sem `warn`.
  Este teste transforma esse "descarte silencioso" em item de manutenção.

  Para cada coluna do bronze:
    1. o nome casa um dos padrões semânticos
       `^(uh|qt|qtd|qtde|quantidade)_ | ^(vr|valor)_ | ^dt_ | situacao |
        pendencia | retomad | paralis | entidade | aporte`;
    2. o nome NÃO está em `historico_bronze_aliases_consumidas()` (colunas do
       bronze que alguma silver/gold consome — mantida à mão, ver acima);
    3. o nome NÃO está no seed `colunas_bronze_ignoradas` para este bronze.

  Severidade: SEMPRE `warn` — o objetivo é visibilidade de schema drift e de
  sinal descartado, nunca travar build. Change:
  colunas-orfas-bronze-historico (guardrail-mapeamento-bronze-historico).

  Uso no schema.yml (nível de model, nos 7 bronzes de empreendimento):

      - name: bronze_sftp_empreendimento_int040
        data_tests:
          - bronze_colunas_nao_mapeadas:
              config: { severity: warn }
-#}
{% macro test_bronze_colunas_nao_mapeadas(model) %}

{%- set padrao =
    '^(uh|qt|qtd|qtde|quantidade)_|^(vr|valor)_|^dt_|situacao|pendencia|retomad|paralis|entidade|aporte'
-%}
{%- set audit = [
    '_source_file', '_ingested_at', '_source_hash', 'iorigem', 'filename',
    'source_file', 'dt_referencia', 'dt_ingest', 'hash_linha', 'dt_movimento',
    'dh_movimento', 'idt_movimento', 'origem', 'de_interface', 'de_descricao',
    'de_pendencia_campo', 'nu_linha', 'co_validacao_mdr', 'source_table',
    'report_date', 'institution', 'profile', 'content_hash', 'agente_arquivo',
    'prioridade_reentrega', 'nu_contrato_emprendimento',
] -%}
{%- set consumidas = historico_bronze_aliases_consumidas() -%}

{#- sufixo curto do bronze: int040 / snh_bb / ... -#}
{%- set bronze_curto = model.identifier | lower
        | replace('bronze_sftp_empreendimento_', '') | replace('bronze_dhist_empreendimento_', '') -%}

{%- set orfas = [] -%}
{%- if execute -%}
    {%- for c in adapter.get_columns_in_relation(model) -%}
        {%- set nome = c.name | lower -%}
        {%- if nome not in audit
               and nome not in consumidas
               and modules.re.search(padrao, nome) -%}
            {%- do orfas.append(nome) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

with
    ignoradas as (
        select lower(trim(cast(coluna as varchar))) as coluna
        from {{ ref('colunas_bronze_ignoradas') }}
        where lower(trim(cast(bronze as varchar))) = '{{ bronze_curto }}'
    ),
    candidatas_orfas as (
        {%- if orfas | length > 0 %}
        {%- for nome in orfas %}
        select '{{ nome }}' as coluna{% if not loop.last %}
        union all{% endif %}
        {%- endfor %}
        {%- else %}
        select cast(null as varchar) as coluna where 1 = 0
        {%- endif %}
    )

select
    '{{ bronze_curto }}' as bronze,
    c.coluna
from candidatas_orfas c
left join ignoradas i using (coluna)
where i.coluna is null

{% endmacro %}
