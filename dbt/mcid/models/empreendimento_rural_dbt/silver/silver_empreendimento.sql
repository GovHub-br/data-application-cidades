{{ config(materialized="table") }}

-- Silver: Empreendimento Rural — Visão unificada
-- Consolida os dados cadastrais, contratuais, andamento físico-financeiro de Caixa e BB.

with
    snh as (
        select * from {{ ref("silver_prioritarios_snh") }}
    ),

    caixa as (
        select * from {{ ref("silver_prioritarios_caixa") }}
    ),

    bb as (
        select * from {{ ref("silver_prioritarios_bb") }}
    ),

    cad_pj as (
        select * from {{ ref("silver_cadastro_pj") }}
    ),

    pnhr_caixa as (
        select * from {{ ref("silver_pnhr_caixa") }}
    ),

    pnhr_bb as (
        select * from {{ ref("silver_pnhr_bb") }}
    ),

    -- A obra mensal é a medição física mais granular que existe, e estava de fora daqui:
    -- a gold do gráfico lia direto dela, o que deixou a ficha e o gráfico com números
    -- diferentes para "execução física". Entra como candidata como as outras.
    -- `distinct on` é defensivo: hoje é uma linha por APF, e se a origem passar a mandar
    -- histórico no mesmo arquivo, a mais recente vence em vez de duplicar o empreendimento.
    obra as (
        select distinct on (apf) *
        from {{ ref("silver_obra_mensal") }}
        order by apf, dt_movimento desc nulls last
    )

select
    s.apf,

    -- Agente Financeiro e Modalidade
    s.agente_financeiro,
    s.modalidade,
    coalesce(s.ic_novo_mcmv, (cpj.apf is not null), false) as ic_novo_mcmv,

    -- Nomes e Entidades
    upper(coalesce(
        cpj.empreendimento_nome,
        s.empreendimento_nome,
        cx.empreendimento_nome,
        b.empreendimento_nome,
        pcx.empreendimento_nome,
        pbb.empreendimento_nome
    )) as empreendimento_nome,

    upper(coalesce(
        cpj.eo_nome,
        pcx.eo_nome,
        pbb.eo_nome
    )) as entidade_organizadora_nome,

    coalesce(
        cpj.eo_cnpj,
        pcx.eo_cnpj,
        pbb.eo_cnpj
    ) as entidade_organizadora_cnpj,

    -- Só o SNH traz construtora; o coalesce herdado repetia o mesmo campo duas vezes.
    upper(s.construtora_nome) as construtora_nome,

    s.construtora_cnpj,

    -- Localização
    s.municipio,
    s.uf,
    s.estado_nome,
    s.regiao,
    s.cod_ibge,

    -- Medições: vale quem mediu por ÚLTIMO, não quem aparece primeiro na lista de fontes.
    -- O snapshot do SNH tem a melhor cobertura, mas é export avulso e pode estar meses
    -- atrás dos feeds mensais da CAIXA e do MONIT de obra.
    --
    -- Cada medição vem com a fonte e a data que a produziram, para o número ser auditável
    -- no dashboard. Ausência é NULL, nunca 0: "não sei" e "zero" são afirmações
    -- diferentes, e a segunda é falsa.

    -- UHs
    {% set uh_contratadas = [
        ('s.uh_contratadas', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.uh_contratadas', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.uh_contratadas', 'b.dt_movimento', 'prioritarios_bb'),
        ('pcx.qt_unidades', 'pcx.dt_movimento', 'pnhr_caixa'),
        ('pbb.qt_unidades', 'pbb.dt_movimento', 'pnhr_bb'),
        ('cpj.qt_uh_contratadas', 'null', 'cadastro_pj'),
    ] %}
    {{ valor_mais_recente(uh_contratadas, tipo='int') }} as quantidade_uh_contratadas,

    {% set uh_entregues = [
        ('s.uh_entregues', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.uh_entregues', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.uh_entregues', 'b.dt_movimento', 'prioritarios_bb'),
        ('o.qt_uh_concluidas', 'o.dt_movimento', 'obra_mensal'),
        ('pcx.qt_unidades_entregues', 'pcx.dt_movimento', 'pnhr_caixa'),
        ('pbb.qt_unidades_entregues', 'pbb.dt_movimento', 'pnhr_bb'),
    ] %}
    {{ valor_mais_recente(uh_entregues, tipo='int') }} as quantidade_uh_entregues,

    {% set uh_vigentes = [
        ('s.uh_vigentes', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.uh_vigentes', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.uh_vigentes', 'b.dt_movimento', 'prioritarios_bb'),
    ] %}
    {{ valor_mais_recente(uh_vigentes, tipo='int') }} as quantidade_uh_vigentes,

    {% set uh_distratadas = [
        ('s.uh_distratadas', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.uh_distratadas', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.uh_distratadas', 'b.dt_movimento', 'prioritarios_bb'),
    ] %}
    {{ valor_mais_recente(uh_distratadas, tipo='int') }} as quantidade_uh_distratadas,

    -- Valores contratuais
    {% set contratado = [
        ('s.valor_contratado', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.valor_contratado', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.valor_contratado', 'b.dt_movimento', 'prioritarios_bb'),
        ('pcx.vr_investimento', 'pcx.dt_movimento', 'pnhr_caixa'),
        ('pbb.vr_investimento', 'pbb.dt_movimento', 'pnhr_bb'),
        ('cpj.vr_investimento_total', 'null', 'cadastro_pj'),
    ] %}
    {{ valor_mais_recente(contratado) }} as valor_contratado,
    {{ valor_mais_recente(contratado, retornar='fonte') }} as fonte_valor_contratado,

    {% set aporte = [
        ('s.valor_aporte_adicional', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.valor_aporte_adicional', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.valor_aporte_adicional', 'b.dt_movimento', 'prioritarios_bb'),
        ('cpj.vr_aporte', 'null', 'cadastro_pj'),
    ] %}
    {{ valor_mais_recente(aporte) }} as valor_aporte_adicional,

    -- Desembolso: a divergência mais cara. No 63665048 eram R$ 293.314 de diferença
    -- entre o snapshot do SNH e o feed da CAIXA do mês.
    {% set desembolsado = [
        ('s.valor_desembolsado', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.valor_desembolsado', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.valor_desembolsado', 'b.dt_movimento', 'prioritarios_bb'),
        ('pcx.vr_liberado', 'pcx.dt_movimento', 'pnhr_caixa'),
        ('pbb.vr_liberado', 'pbb.dt_movimento', 'pnhr_bb'),
        ('cpj.vr_liberado', 'cpj.dt_ultima_liberacao', 'cadastro_pj'),
    ] %}
    {{ valor_mais_recente(desembolsado) }} as valor_desembolsado,
    {{ valor_mais_recente(desembolsado, retornar='fonte') }} as fonte_valor_desembolsado,
    {{ valor_mais_recente(desembolsado, retornar='data') }} as dt_referencia_valor_desembolsado,

    -- Execução física (%)
    {% set fisica = [
        ('s.percentual_execucao_fisica', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.percentual_execucao_fisica', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.percentual_execucao_fisica', 'b.dt_movimento', 'prioritarios_bb'),
        ('o.percentual_obra_realizada', 'o.dt_movimento', 'obra_mensal'),
        ('pcx.percentual_execucao_fisica', 'pcx.dt_movimento', 'pnhr_caixa'),
        ('pbb.percentual_execucao_fisica', 'pbb.dt_movimento', 'pnhr_bb'),
        ('cpj.percentual_obra_realizada', 'null', 'cadastro_pj'),
    ] %}
    {{ valor_mais_recente(fisica) }} as percentual_execucao_fisica,
    {{ valor_mais_recente(fisica, retornar='fonte') }} as fonte_execucao_fisica,
    {{ valor_mais_recente(fisica, retornar='data') }} as dt_referencia_execucao_fisica,

    -- Situação
    {% set situacao = [
        ('s.situacao', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.situacao', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.situacao', 'b.dt_movimento', 'prioritarios_bb'),
        ('pcx.situacao_obra', 'pcx.dt_movimento', 'pnhr_caixa'),
        ('pbb.situacao_obra', 'pbb.dt_movimento', 'pnhr_bb'),
    ] %}
    {{ valor_mais_recente(situacao, tipo='text') }} as situacao_empreendimento,
    {{ valor_mais_recente(situacao, tipo='text', retornar='fonte') }} as fonte_situacao,

    {{ valor_mais_recente([
        ('s.situacao_detalhamento', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.situacao_detalhamento', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.situacao_detalhamento', 'b.dt_movimento', 'prioritarios_bb'),
    ], tipo='text') }} as detalhamento_situacao,

    -- Coordenadas: cadastrais, não medições — a ordem de fonte fixa serve.
    coalesce(s.latitude, cx.latitude, b.latitude) as latitude,
    coalesce(s.longitude, cx.longitude, b.longitude) as longitude,

    -- Datas contratuais: fatos, não medições. Mantêm o coalesce por prioridade de fonte.
    coalesce(
        s.dt_contratacao,
        cx.dt_contratacao,
        b.dt_contratacao,
        cpj.dt_contratacao,
        pcx.dt_contrato,
        pbb.dt_contrato
    ) as dt_contratacao,

    -- A previsão de entrega é uma projeção e MUDA: vale a mais recente, como as medições.
    -- O MONIT de obra entra aqui, e é a única fonte que a preenche em boa parte da
    -- carteira (nos prioritários ela é 99,8% nula).
    {{ valor_mais_recente([
        ('s.dt_previsao_entrega', 's.dt_referencia', 'prioritarios_snh'),
        ('cx.dt_previsao_entrega', 'cx.dt_movimento', 'prioritarios_caixa'),
        ('b.dt_previsao_entrega', 'b.dt_movimento', 'prioritarios_bb'),
        ('o.dt_previsao_entrega', 'o.dt_movimento', 'obra_mensal'),
    ], tipo='date') }} as dt_previsao_entrega,

    coalesce(
        s.dt_termino,
        o.dt_conclusao_obra,
        cpj.dt_conclusao,
        pcx.dt_conclusao_obra,
        pbb.dt_conclusao_obra
    ) as dt_conclusao_obra,

    -- Data da medição mais recente que entrou nesta linha, qualquer que seja o campo.
    -- É a "idade" do empreendimento na visão consolidada, e serve para o dashboard avisar
    -- quando um número é velho em vez de apresentá-lo como se fosse de hoje.
    greatest(
        s.dt_referencia, cx.dt_movimento, b.dt_movimento,
        o.dt_movimento, pcx.dt_movimento, pbb.dt_movimento
    ) as dt_referencia_consolidada

from snh s
left join caixa cx on s.apf = cx.apf and s.agente_financeiro = 'CAIXA'
left join bb b on s.apf = b.apf and s.agente_financeiro = 'BB'
left join cad_pj cpj on s.apf = cpj.apf
left join obra o on s.apf = o.apf
left join pnhr_caixa pcx on s.apf = pcx.apf and s.agente_financeiro = 'CAIXA'
left join pnhr_bb pbb on s.apf = pbb.apf and s.agente_financeiro = 'BB'
