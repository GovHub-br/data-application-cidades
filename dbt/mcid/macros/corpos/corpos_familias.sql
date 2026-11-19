{#
    Corpos das pratas de famílias (dado pessoal minimizado).

    As bronzes INT058, INT064 e INT042 trazem nome, CPF, nascimento, endereço, CEP e PIS dos
    beneficiários. A prata NÃO carrega nenhum desses campos: ela guarda o contrato individual
    (a chave), a localização do imóvel em nível de município, os valores, a situação e três
    atributos de perfil já agregados (sexo, estado civil e faixa etária). A faixa etária sai da
    data de nascimento na própria prata, e a data em si não é persistida.
#}

{% macro familia_data(coluna) %}
    case
        when {{ coluna }} is null or trim({{ coluna }}) = '' then null
        when {{ coluna }} ~ '^\d{4}-\d{2}-\d{2}' then left({{ coluna }}, 10)::date
        else {{ var('schema_udfs') }}.parse_date_br({{ coluna }})
    end
{% endmacro %}

{% macro familia_texto(coluna) %}
    nullif(trim({{ var('schema_udfs') }}.corrigir_mojibake({{ coluna }}::text)), '')
{% endmacro %}

{% macro familia_faixa_etaria(dt_nascimento, dt_referencia) %}
    case
        when {{ dt_nascimento }} is null or {{ dt_referencia }} is null then null
        when date_part('year', age({{ dt_referencia }}, {{ dt_nascimento }})) < 0 then null
        when date_part('year', age({{ dt_referencia }}, {{ dt_nascimento }})) < 18 then 'Até 17 anos'
        when date_part('year', age({{ dt_referencia }}, {{ dt_nascimento }})) < 30 then '18 a 29 anos'
        when date_part('year', age({{ dt_referencia }}, {{ dt_nascimento }})) < 45 then '30 a 44 anos'
        when date_part('year', age({{ dt_referencia }}, {{ dt_nascimento }})) < 60 then '45 a 59 anos'
        else '60 anos ou mais'
    end
{% endmacro %}


{% macro corpo_prata_rural_pnhr_familia() %}
-- Silver: Famílias beneficiárias do PNHR (rural legado), Banco do Brasil e CAIXA.
-- Fonte: bronze_sftp_int058_pnhr_bb_familias (BB) e bronze_sftp_int064_pnhr_caixa_familias (CAIXA).
-- Grão: agente financeiro × contrato individual, com o último movimento (dt_movimento).
--
-- Os dois layouts diferem: a INT064 traz nu_apf, imóvel e renda familiar; a INT058 não traz.
-- Onde só um agente informa, a coluna fica nula no outro. Sem nome, CPF, nascimento nem
-- endereço: ver o cabeçalho de macros/corpos/corpos_familias.sql.

with
    bb as (
        select
            'BB' as agente_financeiro,
            {{ familia_texto('co_agente_financeiro') }} as agente_financeiro_codigo,
            {{ var('schema_udfs') }}.normalize_apf(nu_contrato_empreendimento::text) as apf,
            {{ familia_texto('nu_contrato_empreendimento') }} as nu_contrato_empreendimento,
            {{ familia_texto('nu_contrato_individual') }} as nu_contrato_individual,
            {{ familia_data('dt_movimento') }} as dt_movimento,
            {{ familia_data('dt_contrato') }} as dt_contrato,
            {{ familia_data('dt_efetiva_conclusao') }} as dt_efetiva_conclusao,
            null::text as co_tipo_contratacao,
            {{ familia_texto('co_natureza') }} as co_natureza,
            {{ familia_texto('co_grupo_renda') }} as co_grupo_renda,
            {{ familia_texto('no_tipo_beneficiario') }} as tipo_beneficiario,
            {{ familia_texto('co_sexo_beneficiario') }} as sexo,
            {{ familia_texto('no_estado_civil') }} as estado_civil,
            {{ familia_faixa_etaria(familia_data('dt_nascimento'), familia_data('dt_movimento')) }} as faixa_etaria,
            {{ familia_texto('co_municipio_ibge') }} as cod_ibge,
            {{ familia_texto('no_municipio') }} as municipio,
            {{ familia_texto('sg_uf') }} as uf,
            {{ parse_financial_value('vr_investimento') }} as vr_investimento,
            {{ parse_financial_value('vr_operacao') }} as vr_operacao,
            {{ parse_financial_value('vr_edificacao') }} as vr_edificacao,
            {{ parse_financial_value('vr_atec') }} as vr_atec,
            {{ parse_financial_value('vr_ts') }} as vr_trabalho_social,
            {{ parse_financial_value('vr_custo_originacao') }} as vr_custo_originacao,
            {{ parse_financial_value('vr_taxa_administracao') }} as vr_taxa_administracao,
            {{ parse_financial_value('vr_diferencial_juros') }} as vr_diferencial_juros,
            {{ parse_financial_value('vr_taxa_risco_credito') }} as vr_taxa_risco_credito,
            {{ parse_financial_value('vr_cisterna') }} as vr_cisterna,
            {{ parse_financial_value('vr_efluentes') }} as vr_efluentes,
            {{ parse_financial_value('vr_atec_ts_cisterna') }} as vr_atec_ts_cisterna,
            {{ parse_financial_value('vr_tx_adm_cisterna') }} as vr_taxa_administracao_cisterna,
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_financial_value('vr_subsidio') }} as vr_subsidio,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            null::numeric(15, 2) as vr_imovel,
            null::numeric(15, 2) as vr_renda_familiar,
            {{ parse_financial_value('vr_inadimplente') }} as vr_inadimplente,
            {{ parse_financial_value('vr_parcela_mensal_beneficiario') }} as vr_parcela_mensal,
            upper(trim(ic_cisterna::text)) = 'S' as ic_cisterna,
            {{ parse_int('qt_dias_atraso') }} as qt_dias_atraso,
            regexp_replace(filename::text, '^.*/', '') as arquivo_de_origem
        from {{ ref("bronze_sftp_int058_pnhr_bb_familias") }}
        where nullif(trim(nu_contrato_individual::text), '') is not null
    ),

    caixa as (
        select
            'CAIXA' as agente_financeiro,
            {{ familia_texto('co_agente_financeiro') }} as agente_financeiro_codigo,
            {{ var('schema_udfs') }}.normalize_apf(nu_apf::text) as apf,
            {{ familia_texto('nu_contrato_emprendimento') }} as nu_contrato_empreendimento,
            {{ familia_texto('nu_contrato_individual') }} as nu_contrato_individual,
            {{ familia_data('dt_movimento') }} as dt_movimento,
            {{ familia_data('dt_contrato') }} as dt_contrato,
            {{ familia_data('dt_efetiva_conclusao') }} as dt_efetiva_conclusao,
            {{ familia_texto('co_tipo_contratacao') }} as co_tipo_contratacao,
            {{ familia_texto('co_natureza') }} as co_natureza,
            {{ familia_texto('co_grupo_renda') }} as co_grupo_renda,
            {{ familia_texto('no_tipo_beneficiario') }} as tipo_beneficiario,
            {{ familia_texto('co_sexo_beneficiario') }} as sexo,
            {{ familia_texto('no_estado_civil') }} as estado_civil,
            {{ familia_faixa_etaria(familia_data('dt_nascimento'), familia_data('dt_movimento')) }} as faixa_etaria,
            {{ familia_texto('co_municipio_ibge') }} as cod_ibge,
            {{ familia_texto('no_municipio') }} as municipio,
            {{ familia_texto('sg_uf') }} as uf,
            {{ parse_financial_value('vr_investimento_pnhr') }} as vr_investimento,
            {{ parse_financial_value('vr_operacao') }} as vr_operacao,
            {{ parse_financial_value('vr_edificcacao') }} as vr_edificacao,
            {{ parse_financial_value('vr_atec') }} as vr_atec,
            {{ parse_financial_value('vr_ts') }} as vr_trabalho_social,
            {{ parse_financial_value('vr_custo_originacao') }} as vr_custo_originacao,
            {{ parse_financial_value('vr_taxa_administracao') }} as vr_taxa_administracao,
            {{ parse_financial_value('vr_diferencial_juros') }} as vr_diferencial_juros,
            {{ parse_financial_value('vr_taxa_risco_credito') }} as vr_taxa_risco_credito,
            {{ parse_financial_value('vr_cisterna') }} as vr_cisterna,
            {{ parse_financial_value('vr_efluentes') }} as vr_efluentes,
            {{ parse_financial_value('vr_atec_ts_cisterna') }} as vr_atec_ts_cisterna,
            {{ parse_financial_value('vr_tx_adm_cisterna') }} as vr_taxa_administracao_cisterna,
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_financial_value('vr_subsidio_fgts') }} as vr_subsidio,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            {{ parse_financial_value('vr_imovel') }} as vr_imovel,
            {{ parse_financial_value('vr_renda_familiar') }} as vr_renda_familiar,
            {{ parse_financial_value('vr_inadimplente') }} as vr_inadimplente,
            {{ parse_financial_value('vr_parcela_mensal_beneficiario') }} as vr_parcela_mensal,
            upper(trim(ic_cisterna::text)) = 'S' as ic_cisterna,
            {{ parse_int('qt_dias_atraso') }} as qt_dias_atraso,
            regexp_replace(filename::text, '^.*/', '') as arquivo_de_origem
        from {{ ref("bronze_sftp_int064_pnhr_caixa_familias") }}
        where nullif(trim(nu_contrato_individual::text), '') is not null
    ),

    empilhado as (
        select * from bb
        union all
        select * from caixa
    )

select distinct on (agente_financeiro, nu_contrato_individual) *
from empilhado
order by agente_financeiro, nu_contrato_individual, dt_movimento desc nulls last
{% endmacro %}


{% macro corpo_prata_fds_familia_legado() %}
-- Silver: Famílias do MCMV Entidades (FDS) no legado, contratos da CAIXA.
-- Fonte: bronze_sftp_int042_fds_caixa_familias. Grão: contrato (nu_contrato), com o último
-- movimento (dt_movimento).
--
-- Sem nome do mutuário, CPF, PIS, nascimento, endereço nem CEP: ver o cabeçalho de
-- macros/corpos/corpos_familias.sql. O município e a UF são do imóvel.

with
    bruto as (
        select
            {{ familia_texto('co_agente_financeiro') }} as agente_financeiro_codigo,
            {{ var('schema_udfs') }}.normalize_apf(nu_apf::text) as apf,
            {{ var('schema_udfs') }}.normalize_apf(nuapf_vinculacao::text) as apf_vinculacao,
            {{ familia_texto('nu_contrato') }} as nu_contrato,
            {{ familia_data('dt_movimento') }} as dt_movimento,
            {{ familia_data('dt_assinatura') }} as dt_assinatura,
            {{ familia_data('dt_termino_contrato') }} as dt_termino_contrato,
            {{ familia_texto('co_produto_siico') }} as co_produto_siico,
            {{ familia_texto('dsc_tipo_contrato') }} as tipo_contrato,
            {{ familia_texto('dsc_situacao_ctr') }} as situacao_contrato,
            {{ familia_texto('fase') }} as fase,
            {{ familia_texto('co_sexo') }} as sexo,
            {{ familia_texto('de_estado_civil') }} as estado_civil,
            upper(trim(mulher_chefe_familia::text)) in ('S', 'SIM', '1', 'TRUE') as ic_mulher_chefe_familia,
            {{ parse_int('qt_coobrigados') }} as qt_coobrigados,
            {{ familia_faixa_etaria(familia_data('dt_nascimento'), familia_data('dt_movimento')) }} as faixa_etaria,
            {{ familia_texto('co_municipio_ibge_completo') }} as cod_ibge,
            {{ familia_texto('no_municipio_imovel') }} as municipio,
            {{ familia_texto('sg_uf_imovel') }} as uf,
            {{ parse_financial_value('vr_renda_familiar_comprovada') }} as vr_renda_familiar,
            {{ parse_financial_value('vr_garantia_inicial') }} as vr_garantia_inicial,
            {{ parse_financial_value('vr_subsidio_na_concesssao') }} as vr_subsidio_concessao,
            {{ parse_financial_value('vr_subsidio_concedido') }} as vr_subsidio_concedido,
            {{ parse_financial_value('vr_saldo_devedor_atual') }} as vr_saldo_devedor,
            {{ parse_financial_value('vr_prestacao') }} as vr_prestacao,
            {{ parse_financial_value('vr_encargo_total_atraso') }} as vr_encargo_atraso,
            {{ parse_int('qt_dias_atraso') }} as qt_dias_atraso,
            {{ parse_int('qt_prestacoes_atraso') }} as qt_prestacoes_atraso,
            {{ parse_int('pz_amortizacao') }} as pz_amortizacao,
            {{ parse_int('pz_decorrido') }} as pz_decorrido,
            {{ parse_int('pz_remanescente') }} as pz_remanescente,
            regexp_replace(filename::text, '^.*/', '') as arquivo_de_origem
        from {{ ref("bronze_sftp_int042_fds_caixa_familias") }}
        where nullif(trim(nu_contrato::text), '') is not null
    )

select distinct on (nu_contrato) *
from bruto
order by nu_contrato, dt_movimento desc nulls last
{% endmacro %}
