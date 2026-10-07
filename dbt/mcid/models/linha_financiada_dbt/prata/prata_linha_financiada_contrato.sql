{{ config(materialized='table') }}

-- Grão: uma linha por contrato e carteira de origem. Classe Média e MCMV
-- Cidades são indicadores sobre a Linha Financiada, não linhas excludentes.

with cidades as (
    select *
    from (
        select
            trim(numero_do_contrato::text) as contrato,
            trim(operacao::text) as operacao,
            trim(municipio_codigo::text) as codigo_municipio,
            trim(municipio_nome::text) as municipio,
            trim(uf_sigla::text) as uf,
            trim(faixa_programa::text) as faixa_programa,
            {{ linha_financiada_data_iso('data_da_contratacao') }} as data_contratacao,
            trim(tomador_codigo::text) as agente_financeiro,
            trim(programa::text) as programa,
            trim(modalidade::text) as modalidade,
            trim(tipo_imovel::text) as tipo_imovel,
            trim(classificacao_imovel::text) as classificacao_imovel,
            trim(caracteristica::text) as caracteristica_imovel,
            nullif(replace(vlr_do_financiamento_bruto::text, ',', '.'), '')::numeric as valor_financiamento,
            nullif(replace(vlr_do_desconto::text, ',', '.'), '')::numeric as valor_desconto_fgts,
            nullif(replace(vlr_do_desconto_ogu::text, ',', '.'), '')::numeric as valor_desconto_ogu,
            nullif(replace(vlr_de_garantia_do_imovel::text, ',', '.'), '')::numeric as valor_garantia,
            nullif(replace(vlr_de_compra::text, ',', '.'), '')::numeric as valor_compra,
            nullif(replace(vlr_contrapartida_parceria::text, ',', '.'), '')::numeric as valor_aporte_cidades,
            nullif(replace(vlr_da_renda::text, ',', '.'), '')::numeric as renda_familiar,
            nullif(replace(tx_jrs_inicial::text, ',', '.'), '')::numeric as taxa_juros_inicial,
            nullif(trim(prazo_em_meses::text), '')::integer as prazo_meses,
            trim(genero::text) as genero,
            trim(criado_em::text) as data_remessa_raw,
            row_number() over (
                partition by trim(numero_do_contrato::text)
                order by criado_em desc nulls last
            ) as rn
        from {{ ref('bronze_sharepoint_novo_mcmv_cidades_emendas') }}
        where nullif(trim(numero_do_contrato::text), '') is not null
    ) x
    where rn = 1
),

cci_cca as (
    select
        carteira,
        trim(numerodocontrato::text) as contrato,
        -- A operação da carteira CCA é o código do empreendimento no canal
        -- AO1. CCI não disponibiliza essa chave nesta remessa.
        nullif(trim(operacao::text), '') as codigo_empreendimento,
        trim(anomescontratacao::text) as ano_mes,
        case
            when length(trim(anomescontratacao::text)) = 6
            then make_date(left(trim(anomescontratacao::text), 4)::int,
                           right(trim(anomescontratacao::text), 2)::int, 1)
        end as data_contratacao,
        trim(municipio_codigo::text) as codigo_municipio,
        null::text as municipio,
        null::text as uf,
        trim(agentefinanceiro_codigo::text) as agente_financeiro,
        trim(compatibilidade_faixa_novo_mcmv::text) as faixa_codigo,
        trim(nome_programa::text) as programa,
        trim(modalidade::text) as modalidade,
        trim(classificacaoimovel::text) as classificacao_imovel,
        trim(caracteristica::text) as caracteristica_imovel,
        nullif(replace(vlrdofinanciamento::text, ',', '.'), '')::numeric as valor_financiamento,
        nullif(replace(vlrdodescontofgts::text, ',', '.'), '')::numeric as valor_desconto_fgts,
        nullif(replace(vlrdodescontoogu::text, ',', '.'), '')::numeric as valor_desconto_ogu,
        nullif(replace(vlrdegarantiadoimovel::text, ',', '.'), '')::numeric as valor_garantia,
        nullif(replace(vlrdecompra::text, ',', '.'), '')::numeric as valor_compra,
        nullif(replace(vlrcontrapartidaparceria::text, ',', '.'), '')::numeric as valor_contrapartida_informada,
        nullif(replace(vlrdarenda::text, ',', '.'), '')::numeric as renda_familiar,
        nullif(replace(txjrsinicial::text, ',', '.'), '')::numeric as taxa_juros_inicial,
        nullif(trim(prazo::text), '')::integer as prazo_meses,
        trim(datadaremessa::text) as data_remessa_raw,
        trim(tipo::text) as tipo_imovel,
        trim(genero::text) as genero,
        trim(pmcmv::text) as indicador_mcmv_origem
    from (
        select
            'CCI'::text as carteira,
            numerodocontrato, null::text as operacao, anomescontratacao, municipio_codigo,
            agentefinanceiro_codigo, compatibilidade_faixa_novo_mcmv,
            nome_programa, modalidade, classificacaoimovel, caracteristica,
            vlrdofinanciamento, vlrdodescontofgts, vlrdodescontoogu,
            vlrdegarantiadoimovel, vlrdecompra, vlrcontrapartidaparceria,
            vlrdarenda, txjrsinicial, prazo, datadaremessa, tipo, genero, pmcmv
        from {{ ref('bronze_geavo_cci_analitico') }}
        union all
        select
            'CCA'::text as carteira,
            numerodocontrato, operacao, anomescontratacao, municipio_codigo,
            agentefinanceiro_codigo, compatibilidade_faixa_novo_mcmv,
            nome_programa, modalidade, classificacaoimovel, caracteristica,
            vlrdofinanciamento, vlrdodescontofgts, vlrdodescontoogu,
            vlrdegarantiadoimovel, vlrdecompra, vlrcontrapartidaparceria,
            vlrdarenda, txjrsinicial, prazo, datadaremessa, tipo, genero, pmcmv
        from {{ ref('bronze_geavo_cca_analitico') }}
    ) u
    where nullif(trim(numerodocontrato::text), '') is not null
),

fgts as (
    select
        md5(concat_ws('|', 'FGTS', b.carteira, b.contrato)) as id_contrato_linha_financiada,
        b.contrato,
        b.codigo_empreendimento,
        b.carteira,
        'FGTS'::text as fonte_recurso,
        case
            when c.contrato is not null and left(coalesce(b.faixa_codigo, c.faixa_programa), 1) = '4'
                then 'Classe Média + MCMV Cidades'
            when c.contrato is not null then 'MCMV Cidades'
            when left(b.faixa_codigo, 1) = '4' then 'Classe Média'
            else 'Financiada geral'
        end as segmento_linha_financiada,
        left(b.faixa_codigo, 1) = '4' as ic_classe_media,
        c.contrato is not null as ic_mcmv_cidades,
        false as ic_pro_moradia,
        true as ic_fgts,
        false as ic_fundo_social,
        coalesce(b.data_contratacao,
                 case when length(b.ano_mes) = 6
                      then make_date(left(b.ano_mes, 4)::int, right(b.ano_mes, 2)::int, 1) end) as data_contratacao,
        b.codigo_municipio,
        coalesce(b.municipio, c.municipio) as municipio,
        coalesce(b.uf, c.uf) as uf,
        b.agente_financeiro,
        b.faixa_codigo,
        b.programa,
        b.modalidade,
        b.tipo_imovel,
        b.classificacao_imovel,
        b.caracteristica_imovel,
        b.valor_financiamento,
        b.valor_desconto_fgts,
        b.valor_desconto_ogu,
        b.valor_garantia,
        b.valor_compra,
        coalesce(c.valor_aporte_cidades, b.valor_contrapartida_informada) as valor_contrapartida_informada,
        c.valor_aporte_cidades,
        b.renda_familiar,
        b.taxa_juros_inicial,
        b.prazo_meses,
        b.genero,
        b.data_remessa_raw,
        case when c.contrato is not null then 'MCMV_CIDADES' else 'GEAVO_CCI_CCA' end as fonte_classificacao,
        current_timestamp as dt_silver
    from cci_cca b
    left join cidades c on c.contrato = b.contrato
),

fundo_social as (
    select
        md5(concat_ws('|', 'FUNDO_SOCIAL', trim(nu_contrato::text), trim(dt_evento::text))) as id_contrato_linha_financiada,
        trim(nu_contrato::text) as contrato,
        trim(nu_codigo_atu::text) as codigo_empreendimento,
        'Fundo Social'::text as carteira,
        'Fundo Social'::text as fonte_recurso,
        case when c.contrato is not null then 'Fundo Social + MCMV Cidades' else 'Fundo Social' end as segmento_linha_financiada,
        false as ic_classe_media,
        c.contrato is not null as ic_mcmv_cidades,
        false as ic_pro_moradia,
        false as ic_fgts,
        true as ic_fundo_social,
        {{ linha_financiada_data_formato(
            'f.dt_evento', '%d/%m/%Y', 'DD/MM/YYYY', '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        ) }} as data_contratacao,
        trim(f.co_municipio_ibge::text) as codigo_municipio,
        trim(f.no_municipio_imovel::text) as municipio,
        trim(f.sg_uf_imovel::text) as uf,
        null::text as agente_financeiro,
        trim(f.faixa_renda::text) as faixa_codigo,
        'MCMV Faixa 3 Fundo Social'::text as programa,
        trim(f.modalidade::text) as modalidade,
        trim(f.tipo_imovel::text) as tipo_imovel,
        trim(f.co_classificacao_imovel::text) as classificacao_imovel,
        null::text as caracteristica_imovel,
        nullif(replace(f.vr_evento::text, ',', '.'), '')::numeric as valor_financiamento,
        nullif(replace(f.vr_desconto_resolucao_460::text, ',', '.'), '')::numeric as valor_desconto_fgts,
        null::numeric as valor_desconto_ogu,
        nullif(replace(f.vr_garantia::text, ',', '.'), '')::numeric as valor_garantia,
        nullif(replace(f.vr_investimento::text, ',', '.'), '')::numeric as valor_compra,
        c.valor_aporte_cidades as valor_contrapartida_informada,
        c.valor_aporte_cidades,
        nullif(replace(f.vr_renda_familiar_comprovada::text, ',', '.'), '')::numeric as renda_familiar,
        nullif(replace(f.pc_taxa_juros_nominal_inicial::text, ',', '.'), '')::numeric as taxa_juros_inicial,
        nullif(trim(f.pz_financiamento::text), '')::integer as prazo_meses,
        trim(f.sg_sexo::text) as genero,
        trim(f.dt_remessa::text) as data_remessa_raw,
        'GEFUS_FUNDO_SOCIAL'::text as fonte_classificacao,
        current_timestamp as dt_silver
    from {{ ref('bronze_gefus_fundo_social') }} f
    left join cidades c on c.contrato = trim(f.nu_contrato::text)
    where nullif(trim(f.nu_contrato::text), '') is not null
),

pro_moradia as (
    select
        md5(concat_ws('|', 'PRO_MORADIA', trim(c.cod_contrato::text))) as id_contrato_linha_financiada,
        trim(c.cod_contrato::text) as contrato,
        trim(c.cod_empreendimento::text) as codigo_empreendimento,
        'Operações FGTS'::text as carteira,
        'FGTS'::text as fonte_recurso,
        'Pró-Moradia'::text as segmento_linha_financiada,
        false as ic_classe_media,
        false as ic_mcmv_cidades,
        true as ic_pro_moradia,
        true as ic_fgts,
        false as ic_fundo_social,
        {{ linha_financiada_data_formato(
            'c.dte_assinatura', '%m/%d/%y %H:%M:%S', 'MM/DD/YY HH24:MI:SS',
            '^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
        ) }} as data_contratacao,
        null::text as codigo_municipio,
        null::text as municipio,
        trim(c.uf::text) as uf,
        trim(c.cod_af::text) as agente_financeiro,
        null::text as faixa_codigo,
        trim(l.linha::text) as programa,
        trim(c.cod_modalidade::text) as modalidade,
        null::text as tipo_imovel,
        trim(c.cod_clasificacao::text) as classificacao_imovel,
        trim(c.cod_objetivo::text) as caracteristica_imovel,
        nullif(replace(c.vlr_contratado::text, ',', '.'), '')::numeric as valor_financiamento,
        null::numeric as valor_desconto_fgts,
        null::numeric as valor_desconto_ogu,
        nullif(replace(c.vlr_investimento::text, ',', '.'), '')::numeric as valor_garantia,
        null::numeric as valor_compra,
        null::numeric as valor_contrapartida_informada,
        null::numeric as valor_aporte_cidades,
        null::numeric as renda_familiar,
        null::numeric as taxa_juros_inicial,
        null::integer as prazo_meses,
        null::text as genero,
        trim(c.criado_em::text) as data_remessa_raw,
        'FGTS_AO1_LINHA_26'::text as fonte_classificacao,
        current_timestamp as dt_silver
    from {{ ref('bronze_sharepoint_fgts_canal_tab_ao_1_contratos_fgts') }} c
    join {{ ref('bronze_sharepoint_fgts_canal_tdom_ao_1_linha') }} l
      on trim(l.codigo::text) = trim(c.cod_linha::text)
    where trim(c.cod_linha::text) = '26'
),

cidades_exclusivo as (
    select
        md5(concat_ws('|', 'MCMV_CIDADES', c.contrato)) as id_contrato_linha_financiada,
        c.contrato,
        c.operacao as codigo_empreendimento,
        'MCMV Cidades'::text as carteira,
        'FGTS'::text as fonte_recurso,
        'MCMV Cidades'::text as segmento_linha_financiada,
        left(c.faixa_programa, 1) = '4' as ic_classe_media,
        true as ic_mcmv_cidades,
        false as ic_pro_moradia,
        true as ic_fgts,
        false as ic_fundo_social,
        c.data_contratacao,
        c.codigo_municipio,
        c.municipio,
        c.uf,
        c.agente_financeiro,
        c.faixa_programa as faixa_codigo,
        c.programa,
        c.modalidade,
        c.tipo_imovel,
        c.classificacao_imovel,
        c.caracteristica_imovel,
        c.valor_financiamento,
        c.valor_desconto_fgts,
        c.valor_desconto_ogu,
        c.valor_garantia,
        c.valor_compra,
        c.valor_aporte_cidades as valor_contrapartida_informada,
        c.valor_aporte_cidades,
        c.renda_familiar,
        c.taxa_juros_inicial,
        c.prazo_meses,
        c.genero,
        c.data_remessa_raw,
        'MCMV_CIDADES'::text as fonte_classificacao,
        current_timestamp as dt_silver
    from cidades c
    where not exists (select 1 from cci_cca f where f.contrato = c.contrato)
      and not exists (
          select 1 from {{ ref('bronze_gefus_fundo_social') }} s
          where trim(s.nu_contrato::text) = c.contrato
      )
)

select * from fgts
union all
select * from fundo_social
union all
select * from pro_moradia
union all
select * from cidades_exclusivo
