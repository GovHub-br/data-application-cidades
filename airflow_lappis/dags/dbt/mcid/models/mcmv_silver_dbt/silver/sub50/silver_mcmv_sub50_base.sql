{{ config(materialized="table") }}

{% set apresentadas_relation = none %}
{% set selecionadas_relation = none %}

{% if execute %}
    {% set apresentadas_relation = adapter.get_relation(
        database=target.database,
        schema="__dados_brutos",
        identifier="novo_mcmv_fnhis_sub_50_propostas_apresentadas"
    ) %}
    {% set selecionadas_relation = adapter.get_relation(
        database=target.database,
        schema="__dados_brutos",
        identifier="novo_mcmv_fnhis_sub_50_propostas_selecionadas"
    ) %}
{% endif %}

{% if apresentadas_relation is not none or selecionadas_relation is not none %}

{% if apresentadas_relation is not none %}
with propostas_apresentadas as (
    select
        md5(concat_ws('|', 'sub50-apresentada', numero_da_proposta::text)) as id_silver_frente,
        'Minha Casa Minha Vida'::text as programa,
        'SUB50'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'FNHIS SUB50'::text as linha_mcmv,
        'proposta_fnhis_apresentada'::text as grao_registro,
        'raw'::text as fonte_camada,
        '__dados_brutos'::text as fonte_schema,
        'novo_mcmv_fnhis_sub_50_propostas_apresentadas'::text as fonte_tabela,
        'raw/novo_mcmv_fnhis_sub_50_propostas_apresentadas.csv'::text as fonte_minio_staging,
        null::text as apf,
        numero_da_proposta::text as contrato,
        numero_da_proposta::text as codigo_empreendimento,
        null::text as nome_empreendimento,
        cod_ibge_munic_beneficiado::text as codigo_ibge_municipio,
        municipio::text as municipio,
        null::text as uf,
        'Proponente'::text as responsavel_tipo,
        null::text as responsavel_id,
        proponente::text as responsavel_nome,
        null::text as agente_financeiro,
        null::integer as quantidade_empreendimentos,
        null::integer as quantidade_contratos,
        {{ parse_int('total_de_uh::text') }} as quantidade_uh,
        null::integer as quantidade_uh_entregues,
        null::numeric(15, 2) as valor_contratado,
        null::numeric(15, 2) as valor_desembolsado,
        null::numeric(10, 2) as percentual_execucao_fisica,
        null::numeric(10, 2) as percentual_execucao_financeira,
        coalesce(situacao_da_proposta, justificativa_nao_enquadramento)::text as status_operacional,
        {{ target.schema }}.parse_date_br(criado_em::text) as dt_referencia,
        null::date as dt_contratacao,
        null::date as dt_inicio_obra,
        null::date as dt_previsao_entrega,
        null::date as dt_entrega,
        {{ target.schema }}.parse_date_br(criado_em::text) as dt_ultima_atualizacao,
        'Fonte FNHIS/SUB50 de propostas apresentadas localizada no MinIO e materializada em __dados_brutos; grao de proposta, nao APF.'::text as observacao_silver,
        current_timestamp as dt_silver
    from {{ apresentadas_relation }}
    where nullif(trim(numero_da_proposta::text), '') is not null
)
{% endif %}

{% if selecionadas_relation is not none %}
{% if apresentadas_relation is not none %},{% else %}with{% endif %}
propostas_selecionadas as (
    select
        md5(concat_ws('|', 'sub50-selecionada', num_proposta::text)) as id_silver_frente,
        'Minha Casa Minha Vida'::text as programa,
        'SUB50'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'FNHIS SUB50'::text as linha_mcmv,
        'proposta_fnhis_selecionada'::text as grao_registro,
        'raw'::text as fonte_camada,
        '__dados_brutos'::text as fonte_schema,
        'novo_mcmv_fnhis_sub_50_propostas_selecionadas'::text as fonte_tabela,
        'raw/novo_mcmv_fnhis_sub_50_propostas_selecionadas.csv'::text as fonte_minio_staging,
        null::text as apf,
        num_proposta::text as contrato,
        num_proposta::text as codigo_empreendimento,
        null::text as nome_empreendimento,
        null::text as codigo_ibge_municipio,
        municipio::text as municipio,
        uf::text as uf,
        'Proponente'::text as responsavel_tipo,
        cnpj::text as responsavel_id,
        nome_proponente::text as responsavel_nome,
        null::text as agente_financeiro,
        null::integer as quantidade_empreendimentos,
        null::integer as quantidade_contratos,
        null::integer as quantidade_uh,
        null::integer as quantidade_uh_entregues,
        {{ parse_financial_value('coalesce(valor_de_repasse::text, vl_repasse_proposta::text, valor_empenhado_acumulado::text)') }} as valor_contratado,
        {{ parse_financial_value('coalesce(valor_empenhado_acumulado::text, vl_empenhado_pre_convenio::text)') }} as valor_desembolsado,
        null::numeric(10, 2) as percentual_execucao_fisica,
        null::numeric(10, 2) as percentual_execucao_financeira,
        coalesce(sit_contratacao, situacao_proposta, situacao_instrumento, modalidade)::text as status_operacional,
        {{ target.schema }}.parse_date_br(data_consulta::text) as dt_referencia,
        {{ target.schema }}.parse_date_br(data_assinatura::text) as dt_contratacao,
        null::date as dt_inicio_obra,
        null::date as dt_previsao_entrega,
        null::date as dt_entrega,
        coalesce({{ target.schema }}.parse_date_br(data_assinatura::text), {{ target.schema }}.parse_date_br(data_consulta::text)) as dt_ultima_atualizacao,
        'Fonte FNHIS/SUB50 de propostas selecionadas localizada no MinIO e materializada em __dados_brutos; grao de proposta selecionada.'::text as observacao_silver,
        current_timestamp as dt_silver
    from {{ selecionadas_relation }}
    where nullif(trim(num_proposta::text), '') is not null
)
{% endif %}

{% if apresentadas_relation is not none %}
select * from propostas_apresentadas
{% endif %}
{% if apresentadas_relation is not none and selecionadas_relation is not none %}
union all
{% endif %}
{% if selecionadas_relation is not none %}
select * from propostas_selecionadas
{% endif %}

{% else %}

{{ mcmv_silver_empty_contract(
    "SUB50",
    "Subsidiada",
    "FNHIS SUB50",
    "Fontes FNHIS/SUB50 esperadas em __dados_brutos: novo_mcmv_fnhis_sub_50_propostas_apresentadas e novo_mcmv_fnhis_sub_50_propostas_selecionadas."
) }}

{% endif %}
