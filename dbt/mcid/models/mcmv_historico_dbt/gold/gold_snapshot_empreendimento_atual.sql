{{ config(materialized="table") }}

-- Snapshot corrente (estado atual) derivado da silver historica por frente.
-- Mantem 1 linha por (frente, codigo_empreendimento) com o ultimo mes. No FDS um
-- empreendimento multi-fase tem 2-3 APFs (Projeto/Obra/Desligamento) e colapsa
-- para 1 pela chave estavel codigo_empreendimento (= id_empreendimento no FDS,
-- = apf nas demais frentes; change id-empreendimento-eixo-historico),
-- prevalecendo a fase mais avancada (Desligamento > Obra > Projeto) e nela o
-- dt_referencia mais recente. FAR/Rural: fase nula -> so dt_referencia (inalterado).
-- Consolidado apenas (filtravel por frente_mcmv) — nao ha versao por frente.
--
-- Uniao direta das 3 silvers por frente (FAR/FDS/Rural) — antes lia do helper
-- silver_mcmv_historico_empreendimento, aposentado na convencao 2026-09-04
-- (cada frente materializa como silver_historico_empreendimento no schema da
-- propria frente; nao ha mais um schema unico onde um union all resolveria
-- sozinho).
--
-- ATRIBUTOS ESTAVEIS (change consolidar-schemas-historico-reloginho, D3): as 14
-- colunas que eram a tabela separada dim_empreendimento_historico (chave e
-- cardinalidade IDENTICAS — 17.545/17.545 no semi join por (frente_mcmv,
-- codigo_empreendimento)) entram aqui por left join, ao fim do contrato. A
-- logica de resolucao (ultimo snapshot de cada fonte por atributo) e a mesma:
-- dim_empreendimento_arm sobre as bronzes INT0XX + SNH, dedup para 1 linha/apf
-- e depois 1 linha/(frente, codigo_empreendimento). `dt_dim` NAO migra (a linha
-- ja expoe dt_silver / o gold ja e um retrato corrente).
--   GOTCHA: no_entidade_organizadora / nu_cnpj_entidade so existem em Rural
--   (INT057/INT065 ~86%); INT059/FDS nao traz a coluna. Os gps_* do INT059 sao
--   0% reais — a coordenada decimal vem do latitude_do_imovel/longitude do SNH.
{% set int040 = ref('bronze_mcmv_historico_empreendimento_int040') %}
{% set int054 = ref('bronze_mcmv_historico_empreendimento_int054') %}
{% set int057 = ref('bronze_mcmv_historico_empreendimento_int057') %}
{% set int059 = ref('bronze_mcmv_historico_empreendimento_int059') %}
{% set int065 = ref('bronze_mcmv_historico_empreendimento_int065') %}
{% set snh_familias = familias_snh_empreendimento() %}
with
    consolidado as (
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_far') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_fds') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_rural') }}
    ),

    ultimo as (
        select
            *,
            row_number() over (
                partition by frente_mcmv, codigo_empreendimento
                order by
                    case fase_empreendimento
                        when 'Desligamento' then 0
                        when 'Obra' then 1
                        when 'Projeto' then 2
                        else 3
                    end,
                    dt_referencia desc
            ) as rn
        from consolidado
    ),

    -- ── atributos estaveis (ex-dim_empreendimento_historico) ──
    dim_chave as (
        select
            frente_mcmv,
            codigo_empreendimento,
            apf,
            max(id_empreendimento) as id_empreendimento,
            max(dt_referencia) as chave_dt
        from consolidado
        where apf is not null
        group by 1, 2, 3
    ),

    dim_attrs_raw as (
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

    dim_attrs_apf as (
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
            arg_max(latitude, snap_date) filter (where latitude is not null) as latitude,
            arg_max(longitude, snap_date) filter (where longitude is not null) as longitude,
            max(bairro) filter (where bairro is not null) as bairro,
            max(cep) filter (where cep is not null) as cep,
            max(logradouro) filter (where logradouro is not null) as logradouro,
            max(nu_apf_vinculacao) filter (where nu_apf_vinculacao is not null) as nu_apf_vinculacao,
            max(portaria_selecao) filter (where portaria_selecao is not null) as portaria_selecao
        from dim_attrs_raw
        group by 1
    ),

    dim_juntado as (
        select
            k.frente_mcmv,
            k.codigo_empreendimento,
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
        from dim_chave k
        left join dim_attrs_apf a on k.apf = a.apf
    )

select
    id_historico_snapshot,
    programa,
    u.frente_mcmv,
    grupo_linha,
    linha_mcmv,
    'empreendimento'::text as grao_registro,
    agente_financeiro,
    apf,
    u.codigo_empreendimento,
    id_empreendimento,
    fase_empreendimento,
    nome_empreendimento,
    codigo_ibge_municipio,
    municipio,
    uf,
    responsavel_id,
    responsavel_nome,
    quantidade_uh,
    quantidade_uh_entregues,
    -- change destravar-datas-obra-entrega-silver-historico: conclusao fisica de
    -- obra (estagio anterior a entrega da UH) e a previsao de entrega do SNH.
    quantidade_uh_concluidas,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    dt_inicio_obra,
    -- dt_entrega -> dt_entrega_uh + dt_conclusao_obra (BREAKING; change
    -- enriquecer-datas-acompanhamento-historico). dt_entrega_uh_fonte diz se o
    -- valor veio do feed mensal (sftp[:INTxxx]) ou da espinha (snh:entrega_evento).
    dt_entrega_uh,
    dt_conclusao_obra,
    dt_entrega_uh_fonte,
    dt_previsao_entrega,
    qt_uh_previsao_entrega,
    dt_referencia,
    dt_movimento,
    fonte_tabela,
    source_file,
    situacao_canonica,
    regiao_sigla,
    regiao_nome,
    -- quantidades de UH e sinais de obra + execução financeira
    -- (change enriquecer-quantidades-uh-e-sinais-obra-historico). Aditivas ao
    -- fim do contrato; NULL onde a fonte do último snapshot não reporta.
    quantidade_uh_distratadas,
    quantidade_uh_vigentes,
    quantidade_uh_ociosas,
    quantidade_uh_inicial,
    cod_pendencia_obra,
    percentual_execucao_financeira,
    percentual_execucao_financeira_fonte,
    gap_fisico_financeiro_pp,
    -- fonte_valor = 'carregado' quando o último snapshot do empreendimento é
    -- carry-forward do SNH intermitente (dt_snapshot_efetivo = mês real). D6.
    fonte_valor,
    dt_snapshot_efetivo,
    -- marcadores de LOCF de coluna (change dedup-fonte-silver-historico, D3):
    -- true quando valor_contratado/valor_desembolsado ou responsavel_* do último
    -- snapshot vieram de forward-fill (tipicamente do SFTP antes da virada de
    -- feed) e não de observação SNH. Herdados da silver.
    valor_contratado_preenchido,
    responsavel_preenchido,
    -- sinais de retomada/paralisação e marcos de data promovidos direto da fonte
    -- (change colunas-orfas-bronze-historico, Blocos A/C). Estado do último
    -- snapshot; NULL onde a frente não tem a coluna de origem.
    sinal_retomada,
    motivo_paralisacao,
    desc_situacao_contrato,
    dt_ultima_liberacao,
    dt_primeira_entrega,
    dt_silver,
    -- atributos estaveis (ex-dim_empreendimento_historico; change
    -- consolidar-schemas-historico-reloginho, D3). Aditivas ao fim; NULL onde a
    -- frente nao tem a coluna de origem.
    dj.no_entidade_organizadora,
    dj.nu_cnpj_entidade,
    dj.dsc_tipologia,
    dj.tipo_de_unidade_do_empreendimento,
    dj.regime_construcao,
    dj.cod_regime_execucao,
    dj.modalidade_requalificacao,
    dj.latitude,
    dj.longitude,
    dj.bairro,
    dj.cep,
    dj.logradouro,
    dj.nu_apf_vinculacao,
    dj.portaria_selecao
from ultimo u
left join dim_juntado dj
    on u.frente_mcmv = dj.frente_mcmv
    and u.codigo_empreendimento = dj.codigo_empreendimento
    and dj.rn = 1
where u.rn = 1
