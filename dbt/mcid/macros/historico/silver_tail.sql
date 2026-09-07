{#
    Cauda comum das 3 silvers históricas por frente (FAR / FDS / Rural), change
    enriquecer-quantidades-uh-e-sinais-obra-historico.

    Cada silver, depois de resolver o contrato semântico comum, termina com uma
    CTE chamada `resolvido` (1 linha por frente_mcmv × apf × dt_referencia, já
    filtrada por rn = 1 e pela quarentena) que projeta explicitamente as 49
    colunas do contrato — as 42 antigas + as 5 de quantidade de UH + as 2 de
    execução financeira (`percentual_execucao_financeira`, `_fonte`). NÃO projeta
    `dt_silver` (fica aqui).

    Este macro acrescenta:
      - carry-forward do snapshot SNH intermitente (D6): quando um
        (frente_mcmv, apf) tem observação SNH no mês M-k e no mês M+j mas não em
        meses intermediários, arrasta a última observação conhecida, marcando
        `fonte_valor = 'carregado'` e `dt_snapshot_efetivo` = mês real. Para
        após `var('carry_forward_max_meses', 3)` meses consecutivos. NÃO
        interpola. Só considera a cadência mensal SNH (o SFTP mensal é denso).
      - `gap_fisico_financeiro_pp` = `percentual_execucao_financeira −
        percentual_execucao_fisica` (NULL se qualquer lado for NULL; negativo
        preservado).
      - `fonte_valor` / `dt_snapshot_efetivo` nas linhas reais.
      - `dt_silver`.
#}
{% macro historico_silver_tail() %}
{%- set max_meses = var('carry_forward_max_meses', 3) -%}
,

    resolvido_flag as (
        select
            *,
            'observado'::text as fonte_valor,
            dt_referencia as dt_snapshot_efetivo
        from resolvido
    ),

    -- calendário de meses observados na silver inteira
    meses as (
        select distinct date_trunc('month', dt_referencia)::date as mes
        from resolvido_flag
    ),

    -- janela [primeiro, último] mês com observação SNH real de cada (frente, apf)
    apf_janela_snh as (
        select
            frente_mcmv,
            apf,
            date_trunc('month', min(dt_referencia))::date as primeiro,
            date_trunc('month', max(dt_referencia))::date as ultimo
        from resolvido_flag
        where fonte_serie = 'snh'
        group by 1, 2
    ),

    -- todos os meses dentro da janela SNH de cada (frente, apf)
    grid as (
        select j.frente_mcmv, j.apf, m.mes
        from apf_janela_snh j
        join meses m on m.mes > j.primeiro and m.mes <= j.ultimo
    ),

    -- meses da janela SNH que NÃO têm linha nenhuma na silver (nem SNH nem SFTP)
    faltantes as (
        select g.frente_mcmv, g.apf, g.mes
        from grid g
        left join resolvido_flag r
            on r.frente_mcmv = g.frente_mcmv
            and r.apf = g.apf
            and date_trunc('month', r.dt_referencia)::date = g.mes
        where r.apf is null
    ),

    -- para cada mês faltante, a última observação SNH anterior
    carregado_cand as (
        select
            f.mes as mes_carregado,
            r.*,
            row_number() over (
                partition by f.frente_mcmv, f.apf, f.mes
                order by r.dt_referencia desc
            ) as rn_cf
        from faltantes f
        join resolvido_flag r
            on r.frente_mcmv = f.frente_mcmv
            and r.apf = f.apf
            and r.fonte_serie = 'snh'
            and date_trunc('month', r.dt_referencia)::date < f.mes
    ),

    carregado as (
        select
            md5(
                concat_ws(
                    '|', 'empreendimento', frente_mcmv, coalesce(apf, ''), mes_carregado::text
                )
            ) as id_historico_snapshot,
            id_negocio_historico,
            programa,
            frente_mcmv,
            grupo_linha,
            linha_mcmv,
            grao_registro,
            agente_financeiro,
            apf,
            codigo_empreendimento,
            nome_empreendimento,
            codigo_ibge_municipio,
            municipio,
            uf,
            responsavel_id,
            responsavel_nome,
            quantidade_uh,
            quantidade_uh_entregues,
            valor_contratado,
            valor_desembolsado,
            percentual_execucao_fisica,
            status_operacional,
            dt_contratacao,
            dt_inicio_obra,
            dt_entrega_uh,
            dt_conclusao_obra,
            quantidade_uh_concluidas,
            dt_previsao_entrega,
            qt_uh_previsao_entrega,
            dt_entrega_uh_fonte,
            mes_carregado as dt_referencia,
            -- linha carregada não tem movimento próprio no mês -- o mês real da
            -- observação fica em dt_snapshot_efetivo. NULL mantém o teste
            -- assert_empreendimentos_dt_movimento_consistente coerente.
            null::date as dt_movimento,
            fonte_serie,
            fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest,
            situacao_canonica,
            regiao_sigla,
            regiao_nome,
            id_empreendimento,
            fase_empreendimento,
            quantidade_uh_distratadas,
            quantidade_uh_vigentes,
            quantidade_uh_ociosas,
            quantidade_uh_inicial,
            cod_pendencia_obra,
            percentual_execucao_financeira,
            percentual_execucao_financeira_fonte,
            'carregado'::text as fonte_valor,
            dt_referencia as dt_snapshot_efetivo
        from carregado_cand
        where
            rn_cf = 1
            and date_diff(
                'month', date_trunc('month', dt_referencia)::date, mes_carregado
            ) <= {{ max_meses }}
    ),

    final as (
        select * from resolvido_flag
        union all
        select * from carregado
    )

select
    id_historico_snapshot,
    id_negocio_historico,
    programa,
    frente_mcmv,
    grupo_linha,
    linha_mcmv,
    grao_registro,
    agente_financeiro,
    apf,
    codigo_empreendimento,
    nome_empreendimento,
    codigo_ibge_municipio,
    municipio,
    uf,
    responsavel_id,
    responsavel_nome,
    quantidade_uh,
    quantidade_uh_entregues,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    dt_inicio_obra,
    dt_entrega_uh,
    dt_conclusao_obra,
    quantidade_uh_concluidas,
    dt_previsao_entrega,
    qt_uh_previsao_entrega,
    dt_entrega_uh_fonte,
    dt_referencia,
    dt_movimento,
    fonte_serie,
    fonte_tabela,
    source_file,
    hash_linha,
    dt_ingest,
    situacao_canonica,
    regiao_sigla,
    regiao_nome,
    id_empreendimento,
    fase_empreendimento,
    -- quantidades de UH e sinais de obra (change enriquecer-quantidades-uh-e-sinais-obra-historico)
    quantidade_uh_distratadas,
    quantidade_uh_vigentes,
    quantidade_uh_ociosas,
    quantidade_uh_inicial,
    cod_pendencia_obra,
    percentual_execucao_financeira,
    percentual_execucao_financeira_fonte,
    -- gap físico-financeiro em pontos percentuais; negativo (financeiro à frente
    -- do físico) é sinal de risco e NÃO é truncado a zero.
    (percentual_execucao_financeira - percentual_execucao_fisica)
    as gap_fisico_financeiro_pp,
    fonte_valor,
    dt_snapshot_efetivo,
    current_timestamp as dt_silver
from final
{% endmacro %}
