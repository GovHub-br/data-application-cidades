{{ config(materialized="table") }}

-- Base silver da frente Rural para silver_mcmv_frentes_base.
-- Consome a silver normalizada empreendimento_rural_dbt (change
-- migracao-bronze-minio-mcmv, task 5.3) — antes fazia parsing de separador pipe
-- das INT057/INT065 no schema Postgres sftp.

select
    md5(concat_ws('|', 'rural', apf)) as id_silver_frente,
    'Minha Casa Minha Vida'::text as programa,
    'Rural'::text as frente_mcmv,
    'Subsidiada'::text as grupo_linha,
    'PNHR Rural'::text as linha_mcmv,
    'empreendimento_apf'::text as grao_registro,
    'silver'::text as fonte_camada,
    'empreendimento_rural'::text as fonte_schema,
    'silver_rural_empreendimento'::text as fonte_tabela,
    'raw.novo_mcmv_rural_* + int065/int057 + SNH'::text as fonte_minio_staging,
    apf::text as apf,
    apf::text as contrato,
    apf::text as codigo_empreendimento,
    fase_empreendimento::text as fase_empreendimento,
    empreendimento_nome::text as nome_empreendimento,
    cod_ibge::text as codigo_ibge_municipio,
    municipio::text as municipio,
    uf::text as uf,
    'Entidade Organizadora'::text as responsavel_tipo,
    eo_cnpj::text as responsavel_id,
    eo_nome::text as responsavel_nome,
    agente_financeiro::text as agente_financeiro,
    1::integer as quantidade_empreendimentos,
    1::integer as quantidade_contratos,
    quantidade_uh::integer as quantidade_uh,
    qt_uh_alienadas::integer as quantidade_uh_entregues,
    valor_contratado::numeric(15, 2) as valor_contratado,
    valor_desembolsado::numeric(15, 2) as valor_desembolsado,
    percentual_execucao_fisica::numeric(10, 2) as percentual_execucao_fisica,
    percentual_execucao_financeira::numeric(10, 2) as percentual_execucao_financeira,
    situacao_gefus::text as status_operacional,
    null::date as dt_referencia,
    dt_contratacao::date as dt_contratacao,
    dt_inicio_obra::date as dt_inicio_obra,
    dt_previsao_entrega::date as dt_previsao_entrega,
    dt_entrega::date as dt_entrega,
    coalesce(dt_entrega, dt_conclusao_obra, dt_previsao_entrega, dt_ultima_liberacao, dt_contratacao)::date as dt_ultima_atualizacao,
    'Rural vem da silver empreendimento_rural.silver_rural_empreendimento.'::text as observacao_silver,
    current_timestamp as dt_silver
from {{ ref("silver_rural_empreendimento") }}
where apf is not null
