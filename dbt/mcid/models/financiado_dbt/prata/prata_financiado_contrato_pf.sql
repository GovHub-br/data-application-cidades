{{ config(materialized="table") }}

-- Prata: Contratos PF do Canal FGTS — CCI e CCA na mesma tabela.
-- Fonte: bronze_geavo_cci_analitico + bronze_geavo_cca_analitico (produto Conjuntura)
-- Grão: uma linha por `numerodocontrato`.
--
-- Lê as bronze do Conjuntura em vez de criar as suas: os dois modelos já são espelho
-- `select *` do MESMO parquet que este domínio precisa, e o prefixo `bronze_geavo_`
-- é reservado àquele produto em `governance/dominios.yml`. Duplicar o espelho sob
-- outro nome dobraria o disco e criaria duas versões da mesma remessa.
--
-- A diferença que importa entre as duas metades: o CCA traz `operacao`, que é o
-- contrato PJ do empreendimento; o CCI não traz. Contrato de CCI, portanto, não tem
-- como ser amarrado ao lado PJ por esta tabela, e é por isso que `cod_operacao` nasce
-- nula em toda a metade CCI: é ausência na origem, não falta de tratamento.
with
    cca as (
        select
            trim(numerodocontrato) as numero_contrato,
            'CCA' as origem,
            nullif(trim(operacao), '') as cod_operacao,
            nullif(trim(linha_codigo), '') as cod_linha,
            nullif(trim(linhadecredito), '') as linha_credito,
            nullif(trim(municipio_codigo), '') as cod_municipio_caixa,
            nullif(trim(faixaderenda_codigo), '') as cod_faixa_renda,
            nullif(trim(caracteristica), '') as cod_caracteristica,
            nullif(trim(modalidade), '') as modalidade,
            nullif(trim(nome_programa), '') as programa,
            nullif(trim(pf_pj), '') as tipo_pessoa,
            {{ parse_numeric("vlrdofinanciamento") }} as vr_financiamento,
            {{ parse_numeric("vlrdodescontofgts") }} as vr_desconto_fgts,
            {{ parse_numeric("vlrdodescontoogu") }} as vr_desconto_ogu,
            {{ parse_numeric("vlrdecompra") }} as vr_compra,
            {{ parse_numeric("vlrcontrapartidaparceria") }} as vr_contrapartida_parceria,
            {{ parse_data_canal_fgts("datadacontratacao") }} as dt_contratacao,
            {{ parse_data_canal_fgts("dtterminoobra") }} as dt_termino_obra,
            {{ parse_data_canal_fgts("data_entrega_para_pf") }} as dt_entrega_pf,
            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em
        from {{ ref("bronze_geavo_cca_analitico") }}
    ),

    cci as (
        select
            trim(numerodocontrato) as numero_contrato,
            'CCI' as origem,
            cast(null as text) as cod_operacao,
            nullif(trim(linha_codigo), '') as cod_linha,
            nullif(trim(linhadecredito), '') as linha_credito,
            nullif(trim(municipio_codigo), '') as cod_municipio_caixa,
            nullif(trim(faixaderenda_codigo), '') as cod_faixa_renda,
            nullif(trim(caracteristica), '') as cod_caracteristica,
            nullif(trim(modalidade), '') as modalidade,
            nullif(trim(nome_programa), '') as programa,
            cast(null as text) as tipo_pessoa,
            {{ parse_numeric("vlrdofinanciamento") }} as vr_financiamento,
            {{ parse_numeric("vlrdodescontofgts") }} as vr_desconto_fgts,
            {{ parse_numeric("vlrdodescontoogu") }} as vr_desconto_ogu,
            {{ parse_numeric("vlrdecompra") }} as vr_compra,
            {{ parse_numeric("vlrcontrapartidaparceria") }} as vr_contrapartida_parceria,
            {{ parse_data_canal_fgts("datadacontratacao") }} as dt_contratacao,
            {{ parse_data_canal_fgts("dtterminoobra") }} as dt_termino_obra,
            {{ parse_data_canal_fgts("data_entrega_para_pf") }} as dt_entrega_pf,
            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em
        from {{ ref("bronze_geavo_cci_analitico") }}
    ),

    unificado as (
        select *
        from cca
        union all
        select *
        from cci
    ),

    municipio as (
        select
            trim(codigo) as cod,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(uf), '') as uf,
            nullif(trim(ibge_codigo), '') as cod_ibge
        from {{ ref("bronze_sftp_canalfgts_municipios") }}
    )

select
    u.*,

    -- `003` é a marca de parceria e é onde a contrapartida aparece. Fora dela o valor
    -- vem zerado, porque a origem só registra contrapartida quando ela vira dinheiro
    -- na operação.
    u.cod_caracteristica = '003' as ic_parceria,
    coalesce(u.vr_contrapartida_parceria, 0) > 0 as ic_tem_contrapartida_financeira,

    m.municipio,
    m.uf,
    m.cod_ibge
from unificado as u
left join municipio as m on u.cod_municipio_caixa = m.cod
