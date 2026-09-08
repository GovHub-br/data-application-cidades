{{ config(materialized="table", schema="mcmv_historico", alias="dim_empreendimento_historico") }}

-- DIM — atributos ESTÁVEIS do empreendimento histórico (change
-- colunas-orfas-bronze-historico). 1 linha por (frente_mcmv,
-- codigo_empreendimento) — a MESMA chave de gold_snapshot_empreendimento_atual
-- (= id_empreendimento no FDS, = apf nas demais frentes).
--
-- Atributos que NÃO variam mês a mês e por isso saíram (ou não entraram) do
-- contrato mensal `&contrato_empreendimento`: entidade organizadora, tipologia,
-- regime de construção, coordenadas, endereço, vinculação de APF, portaria de
-- seleção. Resolvidos do ÚLTIMO snapshot de cada fonte por APF; para o FDS
-- multi-APF, prevalece o APF com a observação mais recente na silver.
--
-- Coordenadas: só o SNH tem decimal (`latitude_do_imovel`); os
-- `gps_*_grau/minuto/segundo` do INT059 são 0% reais (verificado 2026-09-08).
--
-- Destino conforme o target (D2). As bronzes/silvers precisam existir no compile
-- (coalesce_present introspecciona a relação).

{% set int040 = ref('bronze_mcmv_historico_empreendimento_int040') %}
{% set int054 = ref('bronze_mcmv_historico_empreendimento_int054') %}
{% set int057 = ref('bronze_mcmv_historico_empreendimento_int057') %}
{% set int059 = ref('bronze_mcmv_historico_empreendimento_int059') %}
{% set int065 = ref('bronze_mcmv_historico_empreendimento_int065') %}
{% set snh_familias = familias_snh_empreendimento() %}

with

    -- chave alinhada ao gold_snapshot: (frente, codigo_empreendimento, apf) +
    -- o mês mais recente em que o APF apareceu na silver.
    chave_apf as (
        {% for s in ['far', 'fds', 'rural'] %}
        select
            frente_mcmv,
            codigo_empreendimento,
            apf,
            max(id_empreendimento) as id_empreendimento,
            max(dt_referencia) as chave_dt
        from {{ ref('silver_mcmv_historico_empreendimento_' ~ s) }}
        where apf is not null
        group by 1, 2, 3
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    -- atributos por linha de bronze, todas as fontes.
    attrs_raw as (
        {{ dim_empreendimento_arm(int040) }}
        union all by name
        {{ dim_empreendimento_arm(int054) }}
        union all by name
        {{ dim_empreendimento_arm(int057) }}
        union all by name
        {{ dim_empreendimento_arm(int059) }}
        union all by name
        {{ dim_empreendimento_arm(int065) }}
        {% for f in snh_familias %}
        union all by name
        {{ dim_empreendimento_arm(ref(f.modelo)) }}
        {% endfor %}
    ),

    -- 1 linha por apf: último snapshot; coalesce por campo do snapshot mais
    -- recente que preencheu (atributo estável, mas a fonte recente é a melhor).
    attrs_apf as (
        select
            apf,
            max(snap_date) as attr_snap_date,
            max(no_entidade_organizadora) filter (where no_entidade_organizadora is not null) as no_entidade_organizadora,
            max(nu_cnpj_entidade) filter (where nu_cnpj_entidade is not null) as nu_cnpj_entidade,
            max(dsc_tipologia) filter (where dsc_tipologia is not null) as dsc_tipologia,
            max(tipo_de_unidade_do_empreendimento) filter (where tipo_de_unidade_do_empreendimento is not null) as tipo_de_unidade_do_empreendimento,
            max(regime_construcao) filter (where regime_construcao is not null) as regime_construcao,
            max(cod_regime_execucao) filter (where cod_regime_execucao is not null) as cod_regime_execucao,
            max(modalidade_requalificacao) filter (where modalidade_requalificacao is not null) as modalidade_requalificacao,
            -- coordenada: a do snapshot mais recente que teve valor.
            arg_max(latitude, snap_date) filter (where latitude is not null) as latitude,
            arg_max(longitude, snap_date) filter (where longitude is not null) as longitude,
            max(bairro) filter (where bairro is not null) as bairro,
            max(cep) filter (where cep is not null) as cep,
            max(logradouro) filter (where logradouro is not null) as logradouro,
            max(nu_apf_vinculacao) filter (where nu_apf_vinculacao is not null) as nu_apf_vinculacao,
            max(portaria_selecao) filter (where portaria_selecao is not null) as portaria_selecao
        from attrs_raw
        group by 1
    ),

    juntado as (
        select
            k.frente_mcmv,
            k.codigo_empreendimento,
            k.apf,
            k.id_empreendimento,
            k.chave_dt,
            a.attr_snap_date,
            a.no_entidade_organizadora,
            a.nu_cnpj_entidade,
            a.dsc_tipologia,
            a.tipo_de_unidade_do_empreendimento,
            a.regime_construcao,
            a.cod_regime_execucao,
            a.modalidade_requalificacao,
            a.latitude,
            a.longitude,
            a.bairro,
            a.cep,
            a.logradouro,
            a.nu_apf_vinculacao,
            a.portaria_selecao,
            row_number() over (
                partition by k.frente_mcmv, k.codigo_empreendimento
                order by k.chave_dt desc, a.attr_snap_date desc nulls last, k.apf
            ) as rn
        from chave_apf k
        left join attrs_apf a on k.apf = a.apf
    )

select
    frente_mcmv,
    codigo_empreendimento,
    apf,
    id_empreendimento,
    no_entidade_organizadora,
    nu_cnpj_entidade,
    dsc_tipologia,
    tipo_de_unidade_do_empreendimento,
    regime_construcao,
    cod_regime_execucao,
    modalidade_requalificacao,
    latitude,
    longitude,
    bairro,
    cep,
    logradouro,
    nu_apf_vinculacao,
    portaria_selecao,
    current_timestamp as dt_dim
from juntado
where rn = 1
