{{ config(materialized="table") }}

-- Prata: um contrato de família beneficiária do FAR por linha, juntando o Novo MCMV
-- (MONT_CONT_PF_FAR) e a carteira legada (INT039 CAIXA, INT021 BB) num layout comum.
-- Não carrega identificador pessoal: CPF, NIS, nome e endereço ficam na bronze.
-- Inadimplente = contrato ativo com prestação vencida há mais de 90 dias, a mesma
-- regra do IC_CONTRATO_INADIMP do layout 015.
with
    dominio as (select campo, codigo, rotulo from {{ ref("dominio_far_perguntas") }}),

    demanda_empreendimento as (
        select apf, lpad(co_tipo_demanda::text, 3, '0') as co_tipo_demanda
        from {{ ref("prata_far_cadastro_pj") }}
    ),

    novo as (
        select
            'Novo MCMV' as geracao,
            'CAIXA' as agente_financeiro,
            trim(m.nu_apf) as apf,
            trim(m.nu_contrato) as nu_contrato,
            {{ var('schema_udfs') }}.parse_date_br(m.dt_assinatura_contrato) as dt_assinatura,
            nullif(trim(m.sg_uf), '') as uf,
            nullif(trim(m.co_municipio_ibge), '') as cod_ibge,
            nullif(upper(trim(m.no_municipio)), '') as municipio,
            case trim(m.no_genero) when 'F' then 'Feminino' when 'M' then 'Masculino' else 'Não informado' end as sexo_responsavel,
            coalesce(ec.rotulo, 'Não informado') as estado_civil,
            nullif(trim(m.nu_cpf_benef_coob), '') is not null as ic_coobrigado,
            case trim(m.mulher_titular) when 'S' then true when 'N' then false end as ic_mulher_responsavel,
            case when trim(m.ic_benef_idoso) in ('S', '1') then true when trim(m.ic_benef_idoso) in ('N', '0') then false end as ic_responsavel_idoso,
            case when trim(m.ic_memb_fam_idoso) in ('S', '1') then true when trim(m.ic_memb_fam_idoso) in ('N', '0') then false end as ic_familia_com_idoso,
            case when trim(m.ic_benef_pcd) in ('S', '1') then true when trim(m.ic_benef_pcd) in ('N', '0') then false end as ic_responsavel_pcd,
            case when trim(m.ic_memb_fam_pcd) in ('S', '1') then true when trim(m.ic_memb_fam_pcd) in ('N', '0') then false end as ic_familia_com_pcd,
            case when trim(m.ic_benef_sit_rua) in ('S', '1') then true when trim(m.ic_benef_sit_rua) in ('N', '0') then false end as ic_situacao_rua,
            {{ parse_int("m.qt_memb_familia") }} as qt_membros_familia,
            {{ parse_numeric("m.vr_renda_comprovada") }} as renda_familiar,
            coalesce(dem.rotulo, 'Não informada') as modalidade,
            coalesce(ti.rotulo, 'Não informado') as tipo_instrumento,
            coalesce(sc.rotulo, 'Não informada') as situacao_contrato,
            lpad(trim(m.co_situacao_contrato), 3, '0') = '001' as ic_ativo,
            {{ parse_int("m.qt_dias_atraso") }} as dias_atraso,
            {{ parse_int("m.qt_prestacoes_atrasadas") }} as prestacoes_atraso,
            {{ parse_numeric("m.vr_prestacao_atrasada") }} as valor_em_atraso,
            trim(m.ic_contrato_inadimp) = 'S' as ic_inadimplente,
            trim(m.ic_imovel_retomado) = 'S' as ic_retomado,
            trim(m.ic_acao_judicial) = 'S' as ic_acao_judicial,
            trim(m.ic_sinistro_mip) = 'S' as ic_sinistro_mip,
            trim(m.ic_sinistro_dfi) = 'S' as ic_sinistro_dfi,
            {{ parse_numeric("m.vr_imovel") }} as valor_imovel,
            {{ parse_numeric("m.vr_subsidio_far") }} as valor_subsidio,
            {{ parse_numeric("m.vr_prestacao") }} as valor_prestacao,
            nullif(trim(m.nu_registro_contrato), '') is null as ic_sem_registro,
            {{ var('schema_udfs') }}.parse_date_br(left(m.dh_movimento, 10)) as dt_retrato,
            m.filename as arquivo_de_origem
        from {{ ref("bronze_sftp_mont_cont_pf_far") }} m
        left join dominio ec on ec.campo = 'estado_civil' and ec.codigo = trim(m.no_estado_civil)
        left join dominio ti on ti.campo = 'tipo_instrumento' and ti.codigo = lpad(trim(m.co_tipo_instrum_contratual), 3, '0')
        left join dominio sc on sc.campo = 'situacao_contrato' and sc.codigo = lpad(trim(m.co_situacao_contrato), 3, '0')
        left join demanda_empreendimento de on de.apf = trim(m.nu_apf)
        left join dominio dem on dem.campo = 'tipo_demanda' and dem.codigo = de.co_tipo_demanda
    ),

    -- INT039 e INT021 têm o mesmo layout; diferem só em nomes de coluna pontuais.
    legado_bruto as (
        select
            'CAIXA' as agente_financeiro, nu_apf, nu_contrato, dt_assinatura, sg_uf_imovel,
            co_municipio_ibge_completo as cod_ibge, no_municipio_imovel, co_sexo, de_estado_civil,
            qt_coobrigados, vr_renda_familiar_comprovada, dsc_tipo_contrato, dsc_situacao_ctr,
            qt_dias_atraso, qt_prestacoes_atraso, vr_encargo_total_atraso, vr_garantia_inicial,
            vr_subsidio_na_concesssao, vr_prestacao, sem_registro, dt_movimento, filename
        from {{ ref("bronze_sftp_int039_far_caixa_pf") }}
        union all
        select
            'Banco do Brasil', nu_apf, nu_contrato, dt_assinatura, sg_uf_imovel,
            cod_municipio_ibge, no_municipio_imovel, co_sexo, de_estado_civil,
            qt_coobrigados, vr_renda_familiar_comprovada, dsc_tipo_contrato, dsc_situacao_ctr,
            qt_dias_atraso, null, vr_encargo_total_atraso, vr_garantia_inicial,
            vr_subsidio_na_concesssao, vr_parcela_mensal_beneficiario, sem_registro, dt_movimento, filename
        from {{ ref("bronze_sftp_int021_far_bb_pf") }}
    ),

    legado as (
        select
            'Legado (2009–2023)' as geracao,
            agente_financeiro,
            trim(nu_apf) as apf,
            trim(nu_contrato) as nu_contrato,
            {{ var('schema_udfs') }}.parse_date_br(dt_assinatura) as dt_assinatura,
            nullif(trim(sg_uf_imovel), '') as uf,
            nullif(trim(cod_ibge), '') as cod_ibge,
            nullif(upper(trim(no_municipio_imovel)), '') as municipio,
            case trim(co_sexo) when '2' then 'Feminino' when '1' then 'Masculino' else 'Não informado' end as sexo_responsavel,
            case
                when de_estado_civil ilike 'solteiro%' or de_estado_civil ilike 'menor%' then 'Solteiro(a)'
                when de_estado_civil ilike 'casado%' then 'Casado(a)'
                when de_estado_civil ilike 'divorciado%' or de_estado_civil ilike 'separado%' then 'Divorciado(a) ou separado(a)'
                when de_estado_civil ilike 'vi_vo%' then 'Viúvo(a)'
                when de_estado_civil ilike 'uni%' then 'União estável'
                else 'Não informado'
            end as estado_civil,
            case when nullif(trim(qt_coobrigados), '') is null then null else {{ parse_int("qt_coobrigados") }} > 0 end as ic_coobrigado,
            null::boolean as ic_mulher_responsavel,
            null::boolean as ic_responsavel_idoso,
            null::boolean as ic_familia_com_idoso,
            null::boolean as ic_responsavel_pcd,
            null::boolean as ic_familia_com_pcd,
            null::boolean as ic_situacao_rua,
            null::int as qt_membros_familia,
            {{ parse_numeric("vr_renda_familiar_comprovada") }} as renda_familiar,
            case trim(dsc_tipo_contrato)
                when 'SUBSIDIO_MENSAL_NA_PRESTACAO' then 'Subsídio mensal na prestação'
                when 'SUBSIDIO_INTEGRAL_NA_CONCESSAO' then 'Subsídio integral na concessão'
                when 'FAR_FGTS_SUBRROGADO_AO_FAR' then 'FAR/FGTS sub-rogado ao FAR'
                when 'FAR_FGTS' then 'FAR/FGTS'
                when 'FAR' then 'FAR'
                when 'PAC_CALAMIDADES' then 'Calamidade (PAC)'
                when 'CALAMIDADES' then 'Calamidade'
                else 'Não informada'
            end as modalidade,
            'Não informado' as tipo_instrumento,
            case trim(dsc_situacao_ctr)
                when 'ATIVO' then 'Ativo'
                when 'LIQUIDADO' then 'Liquidado'
                when 'LIQUIDACAO_SINISTRO' then 'Liquidado por sinistro (MIP/DFI)'
                when 'DISTRATO/LIQUIDACAO_SINISTRO' then 'Distratado'
                when 'DISTRATO' then 'Distratado'
                when 'SUBROGACAO' then 'Sub-rogado'
                else 'Não informada'
            end as situacao_contrato,
            trim(dsc_situacao_ctr) = 'ATIVO' as ic_ativo,
            {{ parse_int("qt_dias_atraso") }} as dias_atraso,
            {{ parse_int("qt_prestacoes_atraso") }} as prestacoes_atraso,
            {{ parse_numeric("vr_encargo_total_atraso") }} as valor_em_atraso,
            trim(dsc_situacao_ctr) = 'ATIVO' and coalesce({{ parse_int("qt_dias_atraso") }}, 0) > 90 as ic_inadimplente,
            null::boolean as ic_retomado,
            null::boolean as ic_acao_judicial,
            null::boolean as ic_sinistro_mip,
            null::boolean as ic_sinistro_dfi,
            {{ parse_numeric("vr_garantia_inicial") }} as valor_imovel,
            {{ parse_numeric("vr_subsidio_na_concesssao") }} as valor_subsidio,
            {{ parse_numeric("vr_prestacao") }} as valor_prestacao,
            case trim(sem_registro) when 'SIM' then true when 'NAO' then false end as ic_sem_registro,
            {{ var('schema_udfs') }}.parse_date_br(dt_movimento) as dt_retrato,
            filename as arquivo_de_origem
        from legado_bruto
    ),

    unido as (
        select * from novo
        union all
        select * from legado
    )

select
    *,
    extract(year from dt_assinatura)::int as ano_assinatura,
    case
        when renda_familiar is null then 'Não informada'
        when renda_familiar = 0 then 'Sem renda comprovada'
        when renda_familiar <= 1000 then 'Até R$ 1.000'
        when renda_familiar <= 2000 then 'R$ 1.000 a R$ 2.000'
        when renda_familiar <= 2850 then 'R$ 2.000 a R$ 2.850'
        else 'Acima de R$ 2.850'
    end as faixa_renda
from unido
