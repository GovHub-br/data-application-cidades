{#
    Corpos SQL de modelos do produto rural que outros modelos da MESMA camada precisam reaproveitar.

    Regra das camadas (medallion estrito): bronze lê a fonte; prata lê só bronze; ouro lê só prata.
    Quando uma prata precisava do resultado de outra prata (ou uma ouro de outra ouro), em vez de
    ler a tabela ela chama o corpo da outra como subconsulta. O SQL é o mesmo, então o dado é o
    mesmo; muda só de onde ele é lido: a linhagem passa a ir da camada anterior direto ao modelo.

    Cada modelo listado aqui é uma casca fina (config + chamada do corpo) em models/.
#}

{% macro corpo_prata_rural_cadastro_pj() %}
-- Silver: Cadastro PJ Rural
-- Fonte: bronze.bronze_shpt_monit_cad_pj_rural_mensal (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados cadastrais limpos e tipados do empreendimento contratado

with
    cad_pj_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf_com_dv) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nu_contrato_empreend)), '') as nu_contrato_empreend,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_empreendimento)), '') as empreendimento_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(co_agente_finan)), '') as agente_financeiro_codigo,

            -- Entidade Organizadora (EO)
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_nome_eo)), '') as eo_nome,
            nullif(regexp_replace(trim(co_cnpj_eo), '[^0-9]', '', 'g'), '') as eo_cnpj,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(sg_uf)), '') as uf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nu_ibge_empreend)), '') as cod_ibge,

            -- Tipologia e Quantidades
            {{ parse_int('nu_modalidade') }} as modalidade_codigo,
            {{ parse_int('qt_uh_selecionadas') }} as qt_uh_selecionadas,
            {{ parse_int('qt_uh_contratadas') }} as qt_uh_contratadas,
            {{ parse_int('qt_uh_concluidas') }} as qt_uh_concluidas,

            -- Prazos
            {{ parse_int('pz_construcao') }} as prazo_construcao,

            -- Valores
            {{ parse_financial_value('vr_investimento_total') }} as vr_investimento_total,
            {{ parse_financial_value('vr_obra') }} as vr_obra,
            {{ parse_financial_value('vr_atec') }} as vr_atec,
            {{ parse_financial_value('vr_ts') }} as vr_trabalho_social,
            {{ parse_financial_value('vr_custo_indireto') }} as vr_custo_indireto,
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            {{ parse_financial_value('vr_aporte') }} as vr_aporte,
            {{ parse_financial_value('vr_suplementacao') }} as vr_suplementacao,

            -- Cisternas e Efluentes
            {{ parse_int('qt_cisterna') }} as qt_cisterna,
            {{ parse_financial_value('vr_cisterna') }} as vr_cisterna,
            {{ parse_int('qt_efluente') }} as qt_efluente,
            {{ parse_financial_value('vr_efluente') }} as vr_efluente,

            -- Execução Física
            {{ parse_numeric('pc_obra_realizada', 'numeric(6,2)') }} as percentual_obra_realizada,

            -- Situação da Obra
            {{ parse_int('co_situacao_obra') }} as co_situacao_obra,

            -- Datas-chave
            {{ target.schema }}.parse_date_br(dt_contratacao) as dt_contratacao,
            {{ target.schema }}.parse_date_br(dt_retomada) as dt_retomada,
            {{ target.schema }}.parse_date_br(dt_ult_liberacao) as dt_ultima_liberacao,
            {{ target.schema }}.parse_date_br(dt_conclusao) as dt_conclusao,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_monit_cad_pj_rural_mensal") }}
    )

select *
from cad_pj_raw
{% endmacro %}

{% macro corpo_prata_rural_empreendimento() %}
-- Silver: Empreendimento Rural — Visão unificada
-- Consolida os dados cadastrais, contratuais, andamento físico-financeiro de Caixa e BB.

with
    snh as (
        select * from ({{ corpo_prata_rural_prioritarios_snh() }}) as prata_rural_prioritarios_snh
    ),

    caixa as (
        select * from ({{ corpo_prata_rural_prioritarios_caixa() }}) as prata_rural_prioritarios_caixa
    ),

    bb as (
        select * from ({{ corpo_prata_rural_prioritarios_bb() }}) as prata_rural_prioritarios_bb
    ),

    cad_pj as (
        select * from ({{ corpo_prata_rural_cadastro_pj() }}) as prata_rural_cadastro_pj
    ),

    pnhr_caixa as (
        select * from ({{ corpo_prata_rural_pnhr_caixa() }}) as prata_rural_pnhr_caixa
    ),

    pnhr_bb as (
        select * from ({{ corpo_prata_rural_pnhr_bb() }}) as prata_rural_pnhr_bb
    ),

    -- A obra mensal é a medição física mais granular que existe, e estava de fora daqui:
    -- a gold do gráfico lia direto dela, o que deixou a ficha e o gráfico com números
    -- diferentes para "execução física". Entra como candidata como as outras.
    -- `distinct on` é defensivo: hoje é uma linha por APF, e se a origem passar a mandar
    -- histórico no mesmo arquivo, a mais recente vence em vez de duplicar o empreendimento.
    obra as (
        select distinct on (apf) *
        from ({{ corpo_prata_rural_obra_mensal() }}) as prata_rural_obra_mensal
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
{% endmacro %}

{% macro corpo_prata_rural_entidade_recontratacao() %}
-- Prata: Para cada operação do Rural, o que a MESMA entidade organizadora ainda tinha em aberto
-- quando ela foi contratada. Responde "a entidade concluiu o anterior antes de concorrer ao
-- próximo?".
-- Grão: operação (APF) com EO identificada.
--
-- Operação anterior "pendente" na data D = contratada antes de D, não distratada, e sem obra
-- concluída até D (dt_conclusao_obra > D, ou nula com status ainda aberto). Anteriores que hoje
-- estão concluídas mas sem data de conclusão não entram como pendentes: são contadas à parte em
-- `qt_anteriores_sem_data_conclusao`, para mostrar a incerteza em vez de escondê-la.
-- A data de contratação é usada no lugar da data em que a EO concorreu, que não existe no dado.

with
    op as (
        select * from ({{ corpo_prata_rural_operacao_eo() }}) as prata_rural_operacao_eo
        where eo_cnpj is not null and dt_contratacao is not null
    ),

    pares as (
        select
            n.apf,
            count(a.apf) as qt_anteriores,
            count(a.apf) filter (where
                a.status_operacao <> 'Distratada'
                and (a.dt_conclusao_obra > n.dt_contratacao
                     or (a.dt_conclusao_obra is null and a.status_operacao in ('Em obra', 'Paralisada', 'Não iniciada')))
            ) as qt_anteriores_pendentes,
            coalesce(sum(a.quantidade_uh_contratadas - coalesce(a.quantidade_uh_entregues, 0)) filter (where
                a.status_operacao <> 'Distratada'
                and (a.dt_conclusao_obra > n.dt_contratacao
                     or (a.dt_conclusao_obra is null and a.status_operacao in ('Em obra', 'Paralisada', 'Não iniciada')))
            ), 0) as uh_anteriores_pendentes,
            count(a.apf) filter (where a.status_operacao = 'Paralisada') as qt_anteriores_hoje_paralisadas,
            count(a.apf) filter (where a.dt_conclusao_obra is null and a.status_operacao = 'Concluída') as qt_anteriores_sem_data_conclusao,
            min(a.dt_contratacao) filter (where
                a.status_operacao <> 'Distratada'
                and (a.dt_conclusao_obra > n.dt_contratacao
                     or (a.dt_conclusao_obra is null and a.status_operacao in ('Em obra', 'Paralisada', 'Não iniciada')))
            ) as dt_anterior_pendente_mais_antiga
        from op n
        left join op a on a.eo_cnpj = n.eo_cnpj and a.dt_contratacao < n.dt_contratacao
        group by n.apf
    )

select
    o.apf,
    o.eo_cnpj,
    o.eo_nome,
    o.fonte_eo,
    o.ic_novo_mcmv,
    o.empreendimento_nome,
    o.municipio,
    o.uf,
    o.dt_contratacao,
    extract(year from o.dt_contratacao)::int as ano_contratacao,
    o.quantidade_uh_contratadas,
    o.status_operacao,
    p.qt_anteriores,
    p.qt_anteriores_pendentes,
    p.uh_anteriores_pendentes,
    p.qt_anteriores_hoje_paralisadas,
    p.qt_anteriores_sem_data_conclusao,
    p.dt_anterior_pendente_mais_antiga,
    p.qt_anteriores_pendentes > 0 as ic_contratou_com_pendencia,
    case
        when p.qt_anteriores = 0 then 'Primeira operação da EO'
        when p.qt_anteriores_pendentes = 0 then 'Anteriores concluídas'
        when p.qt_anteriores_hoje_paralisadas > 0 then 'Contratou com anterior pendente (hoje paralisada)'
        else 'Contratou com anterior pendente'
    end as situacao_recontratacao
from op o
join pares p on p.apf = o.apf
{% endmacro %}

{% macro corpo_prata_rural_evolucao_financeira() %}
-- Prata: Evolução Financeira Rural — Série temporal de desembolsos consolidados por APF × mês.
-- Une o financeiro do Novo MCMV Rural com as liberações históricas do PNHR.

with
    financeiro_novo as (
        select
            apf,
            date_trunc('month', dt_liberacao_recurso) as mes,

            -- Convenção de sinal do MONIT: ic_credito='0' é débito, a LIBERAÇÃO real,
            -- e vem com valor negativo; ic_credito='1' é crédito, DEVOLUÇÃO, e vem
            -- positivo. O sinal é invertido aqui para liberação somar e devolução
            -- subtrair — somar vr_movimento cru daria o acumulado ao contrário.
            case
                when ic_credito = '1' then -abs(vr_movimento)
                else abs(vr_movimento)
            end as vr_liberado,
            vr_desembolso_obra as vr_pago_obra,
            vr_desembolso_trabalho_social as vr_pago_ts,
            vr_desembolso_atec as vr_pago_atec,
            vr_desembolso_cisternas_efluentes as vr_pago_cisternas_efluentes,
            vr_desembolso_custos_indiretos as vr_pago_custos_indiretos
        from ({{ corpo_prata_rural_financeiro_mensal() }}) as prata_rural_financeiro_mensal
        where dt_liberacao_recurso is not null
    ),

    -- Liberações históricas do PNHR: já tipadas e filtradas na silver liberacoes_pnhr.
    -- O INT055 não decompõe o valor por componente, então só alimenta o total liberado.
    financeiro_pnhr as (
        select
            apf,
            date_trunc('month', dt_liberacao) as mes,
            vr_liberado,
            -- NULL, e não 0.00: o INT055 não decompõe o valor por componente. Zero
            -- afirmaria "não houve desembolso de obra"; o arquivo simplesmente não informa.
            null::numeric as vr_pago_obra,
            null::numeric as vr_pago_ts,
            null::numeric as vr_pago_atec,
            null::numeric as vr_pago_cisternas_efluentes,
            null::numeric as vr_pago_custos_indiretos
        from ({{ corpo_prata_rural_pnhr_liberacoes() }}) as prata_rural_pnhr_liberacoes
    ),

    union_financeiro as (
        select * from financeiro_novo
        union all
        select * from financeiro_pnhr
    ),

    mensal_agrupado as (
        select
            apf,
            mes,
            count(*) as qt_liberacoes,
            sum(vr_liberado) as vr_liberado_mes,
            sum(vr_pago_obra) as vr_pago_obra_mes,
            sum(vr_pago_ts) as vr_pago_ts_mes,
            sum(vr_pago_atec) as vr_pago_atec_mes,
            sum(vr_pago_cisternas_efluentes) as vr_pago_cisternas_efluentes_mes,
            sum(vr_pago_custos_indiretos) as vr_pago_custos_indiretos_mes
        from union_financeiro
        group by apf, mes
    ),

    empreendimento as (
        select * from ({{ corpo_prata_rural_empreendimento() }}) as prata_rural_empreendimento
    ),

    evolucao as (
        select
            e.apf,
            m.mes,
            m.qt_liberacoes,

            -- Valores mensais
            -- Sem coalesce para 0: componente nulo significa "a fonte não decompõe"
            -- (caso do INT055), e somar zeros aí produziria um total de obra falso.
            m.vr_liberado_mes,
            m.vr_pago_obra_mes,
            m.vr_pago_ts_mes,
            m.vr_pago_atec_mes,
            m.vr_pago_cisternas_efluentes_mes,
            m.vr_pago_custos_indiretos_mes,

            -- Acumulado progressivo por APF
            sum(m.vr_liberado_mes) over (
                partition by e.apf order by m.mes
            ) as vr_acumulado,

            -- Metadados do contrato
            e.valor_contratado,
            e.municipio,
            e.uf

        from mensal_agrupado m
        inner join empreendimento e on m.apf = e.apf
    )

select
    apf,
    mes,
    qt_liberacoes,

    -- Valores do mês
    vr_liberado_mes,
    vr_pago_obra_mes,
    vr_pago_ts_mes,
    vr_pago_atec_mes,
    vr_pago_cisternas_efluentes_mes,
    vr_pago_custos_indiretos_mes,

    -- Acumulado e percentual
    vr_acumulado,
    -- Sem `else 0.00`: contrato sem valor conhecido não permite calcular percentual, e
    -- publicar 0% seria afirmar que nada foi executado.
    case
        when coalesce(valor_contratado, 0.00) > 0
        then round((vr_acumulado / valor_contratado) * 100, 2)
    end as pct_executado_financeiro,

    valor_contratado,
    municipio,
    uf

from evolucao
{% endmacro %}

{% macro corpo_prata_rural_ficha_empreendimento() %}
-- Prata: Ficha do Empreendimento (Rural)
-- Consome silver/empreendimento e injeta regras de negócio finais como status, prazos e ritmo físico-financeiro.

with
    base_silver as (
        select * from ({{ corpo_prata_rural_empreendimento() }}) as prata_rural_empreendimento
    ),

    -- Acumulado da SÉRIE de liberações, para ser confrontado com o valor_desembolsado,
    -- que é ESTOQUE. A série é um PISO, não a verdade: o INT055 só traz o que CAIXA e BB
    -- reportaram por aquela integração, então contrato antigo tem história anterior ao
    -- arquivo. O estoque é o número da ficha; a série fica ao lado, com a diferença.
    serie as (
        select
            apf,
            max(vr_acumulado) as vr_acumulado_liberacoes,
            count(*) as qt_meses_com_liberacao
        from ({{ corpo_prata_rural_evolucao_financeira() }}) as prata_rural_evolucao_financeira
        group by apf
    ),

    -- Normaliza a situação (a origem manda a mesma em duas grafias) e calcula a execução
    -- financeira uma vez só, para a coluna e o CASE do ritmo não poderem divergir.
    calculado as (
        select
            b.*,
            se.vr_acumulado_liberacoes,
            se.qt_meses_com_liberacao,
            upper(trim(situacao_empreendimento)) as situacao_normalizada,
            case
                when coalesce(valor_contratado, 0) > 0
                then round((valor_desembolsado / valor_contratado) * 100, 2)
            end as pct_execucao_financeira
        from base_silver b
        left join serie se on b.apf = se.apf
    )

select
    -- Identificadores
    apf,
    agente_financeiro,
    case when ic_novo_mcmv then 'NOVO MCMV RURAL' else 'PNHR (HISTÓRICO)' end as programa,

    -- Nomes
    upper(empreendimento_nome) as nome_empreendimento,
    upper(entidade_organizadora_nome) as nome_entidade_organizadora,
    entidade_organizadora_cnpj,
    upper(construtora_nome) as nome_construtora,
    construtora_cnpj,

    -- Escopo e Tipologia
    quantidade_uh_contratadas as quantidade_uh,
    quantidade_uh_entregues,
    quantidade_uh_vigentes,
    floor(quantidade_uh_contratadas * 3.3)::int as pessoas_atendidas,
    case when ic_novo_mcmv then 'Novo MCMV Rural' else 'PNHR (Rural)' end as tipologia,

    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,
    concat(
        apf,
        ' - ',
        municipio, '/', uf,
        ' - ',
        upper(empreendimento_nome)
    ) as apf_municipio_empreendimento,

    -- Coordenadas
    latitude,
    longitude,

    -- Status do Projeto
    situacao_empreendimento,
    case
        when situacao_normalizada in ('CONCLUÍDO E ENTREGUE', 'CONCLUIDA', 'CONCLUÍDA', 'ENTREGUE')
            then 'Concluído'
        when percentual_execucao_fisica is null then 'Sem Informação'
        when percentual_execucao_fisica >= 100 then 'Concluído'
        when percentual_execucao_fisica = 0 then 'Não Iniciado'
        else 'Em Andamento'
    end as status_execucao_simplificado,

    -- Valores Contratuais
    valor_contratado,
    valor_aporte_adicional,
    case
        when coalesce(quantidade_uh_contratadas, 0) > 0
        then round((valor_contratado / quantidade_uh_contratadas), 2)
    end as valor_por_uh,
    dt_contratacao,

    -- Evolução e Prazos
    percentual_execucao_fisica,
    dt_previsao_entrega,
    dt_conclusao_obra,

    -- Regra de Negócio: Atraso na Entrega
    --
    -- 'Sem Previsão' não é um detalhe: dt_previsao_entrega é nula em 99,8% da carteira, e o
    -- `else 'Dentro do Prazo'` anterior classificava toda essa massa como em dia. A coluna
    -- afirmava pontualidade sobre empreendimentos cujo prazo ninguém informou.
    case
        when situacao_normalizada in ('CONCLUÍDO E ENTREGUE', 'CONCLUIDA', 'CONCLUÍDA', 'ENTREGUE')
            then 'Entregue'
        when dt_previsao_entrega is null then 'Sem Previsão'
        when dt_previsao_entrega < current_date then 'Em Atraso'
        else 'Dentro do Prazo'
    end as status_prazo,

    -- Evolução Financeira: ESTOQUE (valor_desembolsado, dos prioritários) e FLUXO
    -- (vr_acumulado_liberacoes, da série). Medem a mesma grandeza por caminhos diferentes
    -- e podem não fechar; quando não fecham, é a série que está incompleta. As duas ficam
    -- visíveis, com a diferença, para a série não contradizer o card.
    valor_desembolsado,
    pct_execucao_financeira as percentual_execucao_financeira,
    vr_acumulado_liberacoes,
    qt_meses_com_liberacao,
    case
        when coalesce(valor_contratado, 0) > 0 and vr_acumulado_liberacoes is not null
        then round((vr_acumulado_liberacoes / valor_contratado) * 100, 2)
    end as percentual_acumulado_liberacoes,
    case
        when valor_desembolsado is not null and vr_acumulado_liberacoes is not null
        then round(valor_desembolsado - vr_acumulado_liberacoes, 2)
    end as diferenca_estoque_menos_serie,

    -- Desembolso acima do contratado é possível de verdade (aporte, suplementação), mas
    -- também é a assinatura de erro de sinal ou de liberação duplicada na origem. Marcar
    -- em vez de limitar: um `least(pct, 100)` esconderia o caso que precisa ser olhado.
    case
        when valor_desembolsado is null or coalesce(valor_contratado, 0) <= 0
            then 'Sem Informação'
        when valor_desembolsado > (valor_contratado + coalesce(valor_aporte_adicional, 0))
            then 'Desembolso Acima do Contratado'
        else 'Dentro do Contratado'
    end as consistencia_desembolso,

    -- Regra de Negócio: Ritmo Físico-Financeiro (margem de 5%)
    -- Comparar um lado conhecido com um lado desconhecido não produz ritmo nenhum.
    case
        when pct_execucao_financeira is null or percentual_execucao_fisica is null
            then 'Sem Informação'
        when pct_execucao_financeira > percentual_execucao_fisica + 5 then 'Desembolso Adiantado'
        when pct_execucao_financeira < percentual_execucao_fisica - 5 then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro,

    -- Procedência: de onde saiu cada medição e de quando ela é. Sem isto, um número de dez
    -- meses atrás aparece no dashboard com a mesma autoridade de um de ontem.
    fonte_execucao_fisica,
    dt_referencia_execucao_fisica,
    fonte_valor_desembolsado,
    dt_referencia_valor_desembolsado,
    fonte_situacao,
    dt_referencia_consolidada,
    (current_date - dt_referencia_consolidada) as dias_desde_ultima_medicao

from calculado
{% endmacro %}

{% macro corpo_prata_rural_financeiro_mensal() %}
-- Silver: Série histórica de desembolsos financeiros (Rural)
-- Fonte: bronze.bronze_shpt_monit_mov_financ_rural_mensal (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: liberações financeiras limpas e tipadas por APF
--
-- Chave APF (schema drift corrigido em 03/11/2026): o MONIT_MOV_FINANC traz `nu_apf` SEM o dígito
-- verificador (6 dígitos, ex. 630767), enquanto o MONIT_MOV_OBRA e o Cad PJ trazem a APF com DV (8
-- dígitos, ex. 63076714). O lpad de antes gerava 00630767, que não casa com nenhuma outra base. Aqui a
-- APF é resolvida pelos 6 primeiros dígitos contra as APFs com DV conhecidas nas bronzes do Novo Rural
-- (obra mensal e Cad PJ), só quando há UM candidato. Sem candidato único, fica a forma antiga e
-- `metodo_chave_apf` avisa. Conferência de 03/11/2026: 1.557 de 1.582 APFs resolvidas; o pc_evolucao do
-- financeiro tem correlação 0,98 com o % de obra realizado da APF resolvida.

with
    apf_com_dv as (
        select {{ target.schema }}.normalize_apf(nu_apf::text) as apf
        from {{ ref("bronze_shpt_monit_mov_obra_rural_mensal") }}
        where length(regexp_replace(coalesce(nu_apf::text, ''), '[^0-9]', '', 'g')) >= 8
        union
        select {{ target.schema }}.normalize_apf(nu_apf_com_dv::text)
        from {{ ref("bronze_shpt_monit_cad_pj_rural_mensal") }}
        where length(regexp_replace(coalesce(nu_apf_com_dv::text, ''), '[^0-9]', '', 'g')) >= 8
    ),

    apf_sem_dv as (
        select left(apf, 6) as apf_6, min(apf) as apf, count(distinct apf) as qt_candidatos
        from apf_com_dv
        where apf is not null
        group by left(apf, 6)
    ),

    financeiro_mensal_raw as (
        select
            -- Identificadores
            nullif(trim({{ target.schema }}.corrigir_mojibake(id)), '') as id,
            nullif(regexp_replace(coalesce(nu_apf::text, ''), '[^0-9]', '', 'g'), '') as nu_apf_origem,
            {{ parse_int('co_tipo_registro') }} as co_tipo_registro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nu_identificador)), '') as nu_identificador,

            -- Execução e Crédito
            {{ parse_numeric('pc_evolucao', 'numeric(6,2)') }} as percentual_evolucao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(ic_credito)), '') as ic_credito,

            -- Valores
            {{ parse_financial_value('vr_movimento') }} as vr_movimento,
            {{ parse_financial_value('vr_desembolso_obra') }} as vr_desembolso_obra,
            {{ parse_financial_value('vr_desembolso_ts') }} as vr_desembolso_trabalho_social,
            {{ parse_financial_value('vr_desembolso_atec') }} as vr_desembolso_atec,
            {{ parse_financial_value('vr_desembolso_cisternas_efluentes') }} as vr_desembolso_cisternas_efluentes,
            {{ parse_financial_value('vr_desembolso_custos_indiretos') }} as vr_desembolso_custos_indiretos,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_remessa) as dt_remessa,
            {{ target.schema }}.parse_date_br(dt_liberacao_recurso) as dt_liberacao_recurso,
            {{ target.schema }}.parse_date_br(dh_gravacao) as dh_gravacao,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_monit_mov_financ_rural_mensal") }}
    )

select
    f.id,
    case
        when length(f.nu_apf_origem) >= 8 then {{ target.schema }}.normalize_apf(f.nu_apf_origem)
        when r.qt_candidatos = 1 then r.apf
        else {{ target.schema }}.normalize_apf(f.nu_apf_origem)
    end as apf,
    f.co_tipo_registro,
    f.nu_identificador,
    f.percentual_evolucao,
    f.ic_credito,
    f.vr_movimento,
    f.vr_desembolso_obra,
    f.vr_desembolso_trabalho_social,
    f.vr_desembolso_atec,
    f.vr_desembolso_cisternas_efluentes,
    f.vr_desembolso_custos_indiretos,
    f.dt_movimento,
    f.dt_remessa,
    f.dt_liberacao_recurso,
    f.dh_gravacao,
    f.arquivo_de_origem,
    f.criado_em,
    f.nu_apf_origem,
    case
        when f.nu_apf_origem is null then 'Sem APF na origem'
        when length(f.nu_apf_origem) >= 8 then 'APF com DV na origem'
        when r.qt_candidatos = 1 then 'APF sem DV resolvida pelos 6 primeiros dígitos'
        when r.qt_candidatos > 1 then 'APF sem DV ambígua (mais de um candidato)'
        else 'APF sem DV não encontrada em obra/Cad PJ'
    end as metodo_chave_apf
from financeiro_mensal_raw f
left join apf_sem_dv r on r.apf_6 = lpad(f.nu_apf_origem, 6, '0') and length(f.nu_apf_origem) < 8
{% endmacro %}

{% macro corpo_prata_rural_obra_mensal() %}
-- Silver: Evolução física mensal da obra (Rural)
-- Fonte: bronze.bronze_shpt_monit_mov_obra_rural_mensal (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados de andamento da obra limpos e tipados por APF

with
    obra_mensal_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf) as apf,

            -- Situação e Andamento
            {{ parse_int('co_situacao_operacao') }} as co_situacao_operacao,
            {{ parse_int('co_andamento_operacao') }} as co_andamento_operacao,

            -- Evolução física (%)
            {{ parse_numeric('pc_obra_prevista', 'numeric(6,2)') }} as percentual_obra_prevista,
            {{ parse_numeric('pc_obra_realizada', 'numeric(6,2)') }} as percentual_obra_realizada,

            -- UHs Concluídas/Alienadas/Sem Habite-se
            {{ parse_int('qt_uh_concluidas') }} as qt_uh_concluidas,
            {{ parse_int('qt_uh_concluidas_adaptadas') }} as qt_uh_concluidas_adaptadas,
            {{ parse_int('qt_uh_alienada') }} as qt_uh_alienadas,
            {{ parse_int('nu_qt_uh_a_alienar') }} as qt_uh_a_alienar,
            {{ parse_int('qt_uh_sem_habitese') }} as qt_uh_sem_habitese,
            {{ parse_int('qt_uh_em_construcao_parcial') }} as qt_uh_em_construcao_parcial,
            {{ parse_int('qt_uh_ociosas_retomadas') }} as qt_uh_ociosas_retomadas,

            -- Valores
            {{ parse_financial_value('vr_total_uh_alienadas') }} as vr_total_uh_alienadas,
            {{ parse_financial_value('vr_total_uh_a_alienar') }} as vr_total_uh_a_alienar,
            {{ parse_financial_value('vr_total_uh_sem_habitese') }} as vr_total_uh_sem_habitese,
            {{ parse_financial_value('vr_total_uh_em_construcao') }} as vr_total_uh_em_construcao,
            {{ parse_financial_value('vr_total_uh_ociosas_retomadas') }} as vr_total_uh_ociosas_retomadas,

            -- Indicadores
            case when trim(ic_invadido) = 'S' then true else false end as ic_invadido,
            case when trim(ic_titularidade) = 'S' then true else false end as ic_titularidade,

            -- Paralisados e Motivos
            {{ parse_int('co_classificacao_paralisados') }} as co_classificacao_paralisados,
            {{ parse_int('co_classificacao_nao_retomada') }} as co_classificacao_nao_retomada,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_detalhe_paralisacao_retomada)), '') as detalhe_paralisacao_retomada,
            {{ parse_int('co_motivo_desimobilizacao') }} as co_motivo_desimobilizacao,
            {{ parse_int('co_motivo_distrato_empreendimento') }} as co_motivo_distrato,

            -- Datas
            {{ target.schema }}.parse_date_br(dh_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_alteracao_situacao) as dt_alteracao_situacao,
            {{ target.schema }}.parse_date_br(dt_alteracao_andamento) as dt_alteracao_andamento,
            {{ target.schema }}.parse_date_br(dt_invasao) as dt_invasao,
            {{ target.schema }}.parse_date_br(dt_previsao_conclusao_obra_retomada) as dt_previsao_conclusao_obra_retomada,
            {{ target.schema }}.parse_date_br(dt_conclusao_obra_retomada) as dt_conclusao_obra_retomada,
            {{ target.schema }}.parse_date_br(dt_primeira_assinatura_pf) as dt_primeira_assinatura_pf,
            {{ target.schema }}.parse_date_br(dt_ultima_assinatura_pf) as dt_ultima_assinatura_pf,
            {{ target.schema }}.parse_date_br(dt_paralisacao) as dt_paralisacao,
            {{ target.schema }}.parse_date_br(dt_conclusao_obra) as dt_conclusao_obra,
            {{ target.schema }}.parse_date_br(dt_previsao_entrega_do_empreendimento) as dt_previsao_entrega,
            {{ target.schema }}.parse_date_br(dt_entrega_do_empreendimento) as dt_entrega_empreendimento,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_monit_mov_obra_rural_mensal") }}
    )

select *
from obra_mensal_raw
{% endmacro %}

{% macro corpo_prata_rural_operacao_eo() %}
-- Prata: Operação do Rural com a ENTIDADE ORGANIZADORA resolvida e a situação simplificada.
-- Fonte: prata_rural_empreendimento (EO do Cad PJ / INT065 / INT057) e, só onde ela não traz EO,
-- o CNPJ "construtora/entidade" do arquivo AF CAIXA (bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa).
-- Grão: APF.
--
-- Por que o fallback: no Novo Rural a SNH não informa EO em nenhuma das ~1.260 operações e o
-- Cad PJ só cobre 127. O AF CAIXA traz um CNPJ de "construtora/entidade" — no Rural a EO é quem
-- contrata a obra, mas isso NÃO foi confirmado com a CAIXA; por isso `fonte_eo` diz de onde veio
-- e a ouro mostra a cobertura por fonte.

with
    emp as (select * from ({{ corpo_prata_rural_empreendimento() }}) as prata_rural_empreendimento),

    -- CNPJ normalizado (14 dígitos; "0"/só zeros vira nulo — ver macro cnpj_normalizado).
    -- Antes o CNPJ ia como veio: 5.981 operações tinham perdido o zero à esquerda e 55
    -- traziam "0", que juntava entidades diferentes numa "EO" de CNPJ 0.
    af_caixa as (
        select distinct on (apf)
            {{ target.schema }}.normalize_apf(apf::text) as apf,
            {{ cnpj_normalizado("cnpj_da_construtora_entidade") }} as cnpj,
            {{ cnpj_digitos_origem("cnpj_da_construtora_entidade") }} as cnpj_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(razao_social_da_construtora_entidade::text)), '') as nome
        from {{ ref("bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa") }}
        where upper(trim(modalidade::text)) = 'RURAL'
          and {{ cnpj_digitos_origem("cnpj_da_construtora_entidade") }} is not null
        order by apf, ({{ cnpj_normalizado("cnpj_da_construtora_entidade") }} is null)
    ),

    resolvida as (
        select
            e.*,
            {{ cnpj_normalizado("e.entidade_organizadora_cnpj") }} as cnpj_cadastro,
            {{ cnpj_digitos_origem("e.entidade_organizadora_cnpj") }} as cnpj_cadastro_origem,
            a.cnpj as cnpj_af,
            a.cnpj_origem as cnpj_af_origem,
            a.nome as nome_af
        from emp e
        left join af_caixa a on a.apf = e.apf
    )

select
    e.apf,
    e.agente_financeiro,
    e.ic_novo_mcmv,
    e.empreendimento_nome,
    e.municipio,
    e.uf,
    e.regiao,
    e.cod_ibge,
    coalesce(e.cnpj_cadastro, e.cnpj_af) as eo_cnpj,
    case
        when e.cnpj_cadastro is not null then e.entidade_organizadora_nome
        when e.cnpj_af is not null then e.nome_af
        else coalesce(e.entidade_organizadora_nome, e.nome_af)
    end as eo_nome,
    case
        when e.cnpj_cadastro is not null then 'cadastro EO (Cad PJ / INT065 / INT057)'
        when e.cnpj_af is not null then 'AF CAIXA construtora/entidade (a confirmar)'
        when coalesce(e.cnpj_cadastro_origem, e.cnpj_af_origem) is not null then 'CNPJ inválido na origem'
        else 'sem EO'
    end as fonte_eo,
    -- CNPJ como chegou (dígitos, sem normalizar), da mesma fonte usada acima
    case
        when e.cnpj_cadastro is not null then e.cnpj_cadastro_origem
        when e.cnpj_af is not null then e.cnpj_af_origem
        else coalesce(e.cnpj_cadastro_origem, e.cnpj_af_origem)
    end as eo_cnpj_origem,
    e.situacao_empreendimento,
    case
        when e.situacao_empreendimento ~* 'distrat|cancel' then 'Distratada'
        when e.situacao_empreendimento ~* 'paralis' then 'Paralisada'
        when e.situacao_empreendimento ~* 'conclu|entreg' then 'Concluída'
        when e.situacao_empreendimento ~* 'n.o inic|projeto' then 'Não iniciada'
        else 'Em obra'
    end as status_operacao,
    e.dt_contratacao,
    e.dt_conclusao_obra,
    e.quantidade_uh_contratadas,
    e.quantidade_uh_entregues,
    e.quantidade_uh_distratadas,
    e.percentual_execucao_fisica,
    e.valor_contratado,
    e.valor_desembolsado
from resolvida e
{% endmacro %}

{% macro corpo_prata_rural_pnhr_bb() %}
-- Silver: Integração BB PJ (PNHR)
-- Fonte: bronze.bronze_sftp_int057_pnhr_bb_empreendimentos (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados cadastrais e financeiros históricos do BB PJ limpos e tipados

with
    int_bb_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(nu_contrato_empreendimento) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nu_contrato_empreendimento)), '') as nu_contrato_empreendimento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_empreendimento)), '') as empreendimento_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(co_agente_financeiro)), '') as agente_financeiro_codigo,

            -- Entidade Organizadora (EO)
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_entidade_organizadora)), '') as eo_nome,
            nullif(regexp_replace(trim(nu_cnpj_entidade), '[^0-9]', '', 'g'), '') as eo_cnpj,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(sg_uf)), '') as uf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(co_municipio_ibge)), '') as cod_ibge,

            -- Quantidades
            {{ parse_int('qt_unidades') }} as qt_unidades,
            {{ parse_int('qt_unidades_concluidas') }} as qt_unidades_concluidas,
            {{ parse_int('qt_unidades_entregues') }} as qt_unidades_entregues,

            -- Valores
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
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,

            -- Prazos e Execução
            {{ parse_int('pz_obra') }} as prazo_obra,
            {{ parse_numeric('pc_execucao_fisica_obra', 'numeric(6, 2)') }} as percentual_execucao_fisica,
            {{ parse_numeric('pc_execucao_financeira_obra', 'numeric(6, 2)') }} as percentual_execucao_financeira,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_situacao_obra)), '') as situacao_obra,

            -- Datas
            case
                when dt_contrato is null or trim(dt_contrato) = '' then null
                when dt_contrato ~ '^\d{4}-\d{2}-\d{2}' then dt_contrato::date
                else {{ target.schema }}.parse_date_br(dt_contrato)
            end as dt_contrato,
            case
                when dt_ultima_liberacao is null or trim(dt_ultima_liberacao) = '' then null
                when dt_ultima_liberacao ~ '^\d{4}-\d{2}-\d{2}' then dt_ultima_liberacao::date
                else {{ target.schema }}.parse_date_br(dt_ultima_liberacao)
            end as dt_ultima_liberacao,
            case
                when dt_efetiva_conclusao is null or trim(dt_efetiva_conclusao) = '' then null
                when dt_efetiva_conclusao ~ '^\d{4}-\d{2}-\d{2}' then dt_efetiva_conclusao::date
                else {{ target.schema }}.parse_date_br(dt_efetiva_conclusao)
            end as dt_conclusao_obra,
            case
                when dt_movimento is null or trim(dt_movimento) = '' then null
                when dt_movimento ~ '^\d{4}-\d{2}-\d{2}' then dt_movimento::date
                else {{ target.schema }}.parse_date_br(dt_movimento)
            end as dt_movimento,

            -- Linhagem da bronze do lake
            nullif(trim({{ target.schema }}.corrigir_mojibake(origem)), '') as origem,
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_int057_pnhr_bb_empreendimentos") }}
    )

select *
from int_bb_raw
{% endmacro %}

{% macro corpo_prata_rural_pnhr_caixa() %}
-- Silver: Integração Caixa PJ (PNHR)
-- Fonte: bronze.bronze_sftp_int065_pnhr_caixa_empreendimentos (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados cadastrais e financeiros históricos da Caixa PJ limpos e tipados

with
    int_caixa_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nu_contrato_emprendimento)), '') as nu_contrato_empreend,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_empreendimento)), '') as empreendimento_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(co_agente_financeiro)), '') as agente_financeiro_codigo,

            -- Entidade Organizadora (EO)
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_entidade_organizadora)), '') as eo_nome,
            nullif(regexp_replace(trim(nu_cnpj_entidade), '[^0-9]', '', 'g'), '') as eo_cnpj,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(sg_uf)), '') as uf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(co_municipio_ibge)), '') as cod_ibge,

            -- Quantidades
            {{ parse_int('qtde_uh_inicial') }} as qtde_uh_inicial,
            {{ parse_int('qtde_unidades') }} as qt_unidades,
            {{ parse_int('qt_unidades_concluidas') }} as qt_unidades_concluidas,
            {{ parse_int('qt_unidades_entregues') }} as qt_unidades_entregues,

            -- Valores
            {{ parse_financial_value('vr_investimento_pnhr') }} as vr_investimento,
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
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_financial_value('vr_subsidio_fgts') }} as vr_subsidio_fgts,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,

            -- Prazos e Execução
            {{ parse_int('pz_construcao') }} as prazo_construcao,
            {{ parse_numeric('pc_obra_realizado', 'numeric(6, 2)') }} as percentual_execucao_fisica,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_situacao_obra)), '') as situacao_obra,

            -- Datas
            case
                when dt_contrato is null or trim(dt_contrato) = '' then null
                when dt_contrato ~ '^\d{4}-\d{2}-\d{2}' then dt_contrato::date
                else {{ target.schema }}.parse_date_br(dt_contrato)
            end as dt_contrato,
            case
                when dt_ultima_liberacao is null or trim(dt_ultima_liberacao) = '' then null
                when dt_ultima_liberacao ~ '^\d{4}-\d{2}-\d{2}' then dt_ultima_liberacao::date
                else {{ target.schema }}.parse_date_br(dt_ultima_liberacao)
            end as dt_ultima_liberacao,
            case
                when dt_efetiva_conclusao is null or trim(dt_efetiva_conclusao) = '' then null
                when dt_efetiva_conclusao ~ '^\d{4}-\d{2}-\d{2}' then dt_efetiva_conclusao::date
                else {{ target.schema }}.parse_date_br(dt_efetiva_conclusao)
            end as dt_conclusao_obra,
            case
                when dt_movimento is null or trim(dt_movimento) = '' then null
                when dt_movimento ~ '^\d{4}-\d{2}-\d{2}' then dt_movimento::date
                else {{ target.schema }}.parse_date_br(dt_movimento)
            end as dt_movimento,

            -- Linhagem da bronze do lake
            nullif(trim({{ target.schema }}.corrigir_mojibake(origem)), '') as origem,
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_int065_pnhr_caixa_empreendimentos") }}
    )

select *
from int_caixa_raw
{% endmacro %}

{% macro corpo_prata_rural_pnhr_liberacoes() %}
-- Silver: Liberações históricas do PNHR (INT055 CAIXA/BB)
-- Fonte: bronze.bronze_sftp_int055_liberacoes_caixa_bb (parquet da staging/ carregado
-- pelo staging_para_bronze.py)
-- Saída: uma linha por liberação de recurso do PNHR, tipada, para a série financeira.
-- O filtro de programa (%PNHR%) fica aqui, junto do resto da regra de leitura da fonte.
with
    liberacoes_raw as (
        select
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_programa)), '') as programa,

            -- Valores
            {{ parse_financial_value("vr_valor") }} as vr_liberado,

            -- Datas: o INT055 chega com ISO em algumas remessas e dd/mm/aaaa em outras
            case
                when data_liberacao is null or trim(data_liberacao) = ''
                then null
                when data_liberacao ~ '^\d{4}-\d{2}-\d{2}'
                then data_liberacao::date
                else {{ target.schema }}.parse_date_br(data_liberacao)
            end as dt_liberacao,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_int055_liberacoes_caixa_bb") }}
        where
            nu_apf is not null
            and nullif(trim({{ target.schema }}.corrigir_mojibake(data_liberacao)), '') is not null
            and trim(upper(no_programa)) like '%PNHR%'
    )

select *
from liberacoes_raw
where dt_liberacao is not null
{% endmacro %}

{% macro corpo_prata_rural_prioritarios_bb() %}
-- Silver: Dados Prioritários Banco do Brasil (Rural)
-- Fonte: bronze.bronze_sftp_snh_pmcmv_dados_prioritarios_af_bb (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados prioritários do BB limpos, tipados e filtrados para modalidade Rural

with
    prioritarios_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(apf) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(agente_financeiro)), '') as agente_financeiro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_empreendimento)), '') as empreendimento_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade)), '') as modalidade,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(uf)), '') as uf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(codigo_ibge_do_municipio)), '') as cod_ibge,

            -- Situação
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_do_empreendimento)), '') as situacao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(detalhamento_da_situacao_do_empreendimento)), '') as situacao_detalhamento,

            -- Execução física (%)
            {{ parse_numeric('"exec"', 'numeric(6, 2)') }} as percentual_execucao_fisica,

            -- Valores
            {{ parse_financial_value('valor_contratado') }} as valor_contratado,
            {{ parse_financial_value('valor_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_financial_value('valor_desembolsado') }} as valor_desembolsado,
            {{ parse_financial_value('valor_desembolsado_do_ano_de_referencia') }} as valor_desembolsado_ano,

            -- UHs
            {{ parse_int('uh_contratadas') }} as uh_contratadas,
            {{ parse_int('uh_entregues') }} as uh_entregues,
            {{ parse_int('uh_vigentes') }} as uh_vigentes,
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,
            {{ parse_int('quantidade_de_uhs_distratadas') }} as uh_distratadas,

            -- Endereço
            nullif(trim({{ target.schema }}.corrigir_mojibake(logradouro_do_imovel)), '') as logradouro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(bairro_do_imovel)), '') as bairro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(cep_do_imovel)), '') as cep,

            -- Coordenadas
            {{ parse_numeric('latitude_do_imovel', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude_do_imovel', 'numeric(12, 8)') }} as longitude,

            -- Datas
            case
                when data_de_contratacao is null or trim(data_de_contratacao) = '' then null
                when data_de_contratacao ~ '^\d{4}-\d{2}-\d{2}' then data_de_contratacao::date
                else {{ target.schema }}.parse_date_br(data_de_contratacao)
            end as dt_contratacao,
            case
                when data_da_previsao_da_entrega is null or trim(data_da_previsao_da_entrega) = '' then null
                when data_da_previsao_da_entrega ~ '^\d{4}-\d{2}-\d{2}' then data_da_previsao_da_entrega::date
                else {{ target.schema }}.parse_date_br(data_da_previsao_da_entrega)
            end as dt_previsao_entrega,
            case
                when data_de_movimento is null or trim(data_de_movimento) = '' then null
                when data_de_movimento ~ '^\d{4}-\d{2}-\d{2}' then data_de_movimento::date
                else {{ target.schema }}.parse_date_br(data_de_movimento)
            end as dt_movimento,

            -- Observações
            nullif(trim({{ target.schema }}.corrigir_mojibake(observacoes)), '') as observacoes,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_snh_pmcmv_dados_prioritarios_af_bb") }}
        where trim(upper(modalidade)) = 'RURAL'
    )

select *
from prioritarios_raw
{% endmacro %}

{% macro corpo_prata_rural_prioritarios_caixa() %}
-- Silver: Dados Prioritários CAIXA (Rural)
-- Fonte: bronze.bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados prioritários da Caixa limpos, tipados e filtrados para modalidade RURAL

with
    prioritarios_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(apf) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(agente_financeiro)), '') as agente_financeiro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_empreendimento)), '') as empreendimento_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade)), '') as modalidade,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(uf)), '') as uf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(codigo_ibge_do_municipio)), '') as cod_ibge,

            -- Situação
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_do_empreendimento)), '') as situacao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(detalhamento_da_situacao_do_empreendimento)), '') as situacao_detalhamento,

            -- Execução física (%)
            {{ parse_numeric('"exec"', 'numeric(6, 2)') }} as percentual_execucao_fisica,

            -- Valores
            {{ parse_financial_value('valor_contratado') }} as valor_contratado,
            {{ parse_financial_value('valor_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_financial_value('valor_desembolsado') }} as valor_desembolsado,
            {{ parse_financial_value('valor_desembolsado_do_ano_de_referencia') }} as valor_desembolsado_ano,

            -- UHs
            {{ parse_int('uh_contratadas') }} as uh_contratadas,
            {{ parse_int('uh_entregues') }} as uh_entregues,
            {{ parse_int('uh_vigentes') }} as uh_vigentes,
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,
            {{ parse_int('quantidade_de_uhs_distratadas') }} as uh_distratadas,

            -- Endereço
            nullif(trim({{ target.schema }}.corrigir_mojibake(logradouro_do_imovel)), '') as logradouro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(bairro_do_imovel)), '') as bairro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(cep_do_imovel)), '') as cep,

            -- Coordenadas
            {{ parse_numeric('latitude_do_imovel', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude_do_imovel', 'numeric(12, 8)') }} as longitude,

            -- Datas
            case
                when data_de_contratacao is null or trim(data_de_contratacao) = '' then null
                when data_de_contratacao ~ '^\d{4}-\d{2}-\d{2}' then data_de_contratacao::date
                else {{ target.schema }}.parse_date_br(data_de_contratacao)
            end as dt_contratacao,
            case
                when data_da_previsao_da_entrega is null or trim(data_da_previsao_da_entrega) = '' then null
                when data_da_previsao_da_entrega ~ '^\d{4}-\d{2}-\d{2}' then data_da_previsao_da_entrega::date
                else {{ target.schema }}.parse_date_br(data_da_previsao_da_entrega)
            end as dt_previsao_entrega,
            case
                when data_de_movimento is null or trim(data_de_movimento) = '' then null
                when data_de_movimento ~ '^\d{4}-\d{2}-\d{2}' then data_de_movimento::date
                else {{ target.schema }}.parse_date_br(data_de_movimento)
            end as dt_movimento,

            -- Observações
            nullif(trim({{ target.schema }}.corrigir_mojibake(observacoes)), '') as observacoes,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa") }}
        where trim(upper(modalidade)) = 'RURAL'
    )

select *
from prioritarios_raw
{% endmacro %}

{% macro corpo_prata_rural_prioritarios_snh() %}
-- Silver: Dados Prioritários SNH (Rural)
-- Fonte: bronze.bronze_shpt_dados_prioritarios_snh_empreendimentos (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados prioritários gerais limpos, tipados e filtrados para modalidade Rural

with
    snh_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(codigo_da_operacao_no_agente_financeiro) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(identificador_da_operacao_na_snh)), '') as id_operacao_snh,
            nullif(trim({{ target.schema }}.corrigir_mojibake(codigo_da_operacao_no_agente_financeiro)), '') as cod_operacao_agente,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_do_agente_financeiro)), '') as agente_financeiro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_do_empreendimento)), '') as empreendimento_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade)), '') as modalidade,

            -- Construtora / Entidade
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_construtora_entidade)), '') as construtora_nome,
            nullif(regexp_replace(trim(nome_da_construtora_entidade_2_cnpj_da_construtora_entidade), '[^0-9]', '', 'g'), '') as construtora_cnpj,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(sigla_da_uf)), '') as uf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_uf)), '') as estado_nome,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_regiao)), '') as regiao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(codigo_ibge_do_municipio)), '') as cod_ibge,

            -- Endereço
            nullif(trim({{ target.schema }}.corrigir_mojibake(logradouro)), '') as logradouro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(numero_do_imovel)), '') as numero_imovel,
            nullif(trim({{ target.schema }}.corrigir_mojibake(complemento_do_logradouro)), '') as complemento_logradouro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(bairro)), '') as bairro,
            nullif(trim({{ target.schema }}.corrigir_mojibake(cep)), '') as cep,

            -- Coordenadas
            {{ parse_numeric('latitude', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude', 'numeric(12, 8)') }} as longitude,

            -- Situação e Fase
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_do_empreendimento)), '') as situacao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(detalhamento_da_situacao_do_empreendimento)), '') as situacao_detalhamento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_da_empreendimento_agrupada)), '') as situacao_agrupada,
            case when trim(mudou_de_fase) = 'Sim' then true else false end as mudou_de_fase,
            nullif(trim({{ target.schema }}.corrigir_mojibake(apf_da_fase_obra)), '') as apf_fase_obra,
            case when trim(novo_mcmv_sim_nao) = 'Sim' then true else false end as ic_novo_mcmv,

            -- Execução Física (%)
            {{ parse_numeric('percentual_da_obra', 'numeric(6, 2)') }} as percentual_execucao_fisica,

            -- UHs
            {{ parse_int('unidades_contratadas') }} as uh_contratadas,
            {{ parse_int('unidades_entregues') }} as uh_entregues,
            {{ parse_int('unidades_vigentes') }} as uh_vigentes,
            {{ parse_int('unidades_distratadas') }} as uh_distratadas,
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,

            -- Valores
            {{ parse_financial_value('valor_contratado_original') }} as valor_contratado_original,
            {{ parse_financial_value('valor_do_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_financial_value('valor_contratado_total') }} as valor_contratado,
            {{ parse_financial_value('valor_desembolsado_total') }} as valor_desembolsado,

            -- Datas
            case
                when data_da_contratacao is null or trim(data_da_contratacao) = '' then null
                when data_da_contratacao ~ '^\d{4}-\d{2}-\d{2}' then data_da_contratacao::date
                else {{ target.schema }}.parse_date_br(data_da_contratacao)
            end as dt_contratacao,
            case
                when data_de_previsao_de_termino is null or trim(data_de_previsao_de_termino) = '' then null
                when data_de_previsao_de_termino ~ '^\d{4}-\d{2}-\d{2}' then data_de_previsao_de_termino::date
                else {{ target.schema }}.parse_date_br(data_de_previsao_de_termino)
            end as dt_previsao_entrega,
            case
                when data_do_termino is null or trim(data_do_termino) = '' then null
                when data_do_termino ~ '^\d{4}-\d{2}-\d{2}' then data_do_termino::date
                else {{ target.schema }}.parse_date_br(data_do_termino)
            end as dt_termino,
            case
                when data_de_referencia is null or trim(data_de_referencia) = '' then null
                when data_de_referencia ~ '^\d{4}-\d{2}-\d{2}' then data_de_referencia::date
                else {{ target.schema }}.parse_date_br(data_de_referencia)
            end as dt_referencia,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_dados_prioritarios_snh_empreendimentos") }}
        where trim(upper(modalidade)) = 'RURAL'
    )

select *
from snh_raw
{% endmacro %}

{% macro corpo_prata_rural_selecao_portaria_162() %}
-- Prata: Empreendimentos do Rural selecionados pela Portaria MCidades nº 162/2018.
-- Fonte: bronze_sftp_portaria_162_2018_rural. Grão: empreendimento selecionado.
-- `apf` é o código do empreendimento normalizado como APF, para casar com o PNHR/SNH.

select
    {{ target.schema }}.normalize_apf(codigo_do_empreendimento::text) as apf,
    nullif(trim(codigo_do_empreendimento::text), '') as codigo_empreendimento,
    -- 14 dígitos, como na prata_rural_operacao_eo: sem isso o mesmo CNPJ com e sem o zero à
    -- esquerda parecia outra entidade, e o `ic_mesma_eo` da ouro dava falso em 410 contratos.
    {{ cnpj_normalizado("cnpj_entidade") }} as entidade_organizadora_cnpj,
    nullif(trim(uf::text), '') as uf,
    upper(nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '')) as municipio,
    initcap(nullif(trim(regiao::text), '')) as regiao,
    {{ parse_int('qtde_de_uh::text') }} as quantidade_uh_selecionadas,
    date '2018-02-27' as dt_portaria,
    'Portaria MCidades nº 162/2018' as ato_selecao,
    regexp_replace(filename::text, '^.*/', '') as arquivo_de_origem
from {{ ref("bronze_sftp_portaria_162_2018_rural") }}
where nullif(trim(codigo_do_empreendimento::text), '') is not null
{% endmacro %}

{% macro corpo_prata_rural_trabalho_social_bb() %}
-- Silver: Trabalho Social PNHR Banco do Brasil
-- Fonte: bronze.bronze_shpt_base_trabalho_social_pnhr_bb (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados de trabalho social do BB limpos e tipados

with
    ts_bb_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(contrato_registro_ao) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(contrato_registro_ao)), '') as contrato,
            nullif(trim({{ target.schema }}.corrigir_mojibake(recurso)), '') as recurso,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_empreendimento)), '') as empreendimento_nome,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(uf)), '') as uf,

            -- Quantidades e Tipologia
            {{ parse_int('uh') }} as qt_uh,
            nullif(trim({{ target.schema }}.corrigir_mojibake(tipologia)), '') as tipologia,
            nullif(trim({{ target.schema }}.corrigir_mojibake(fase_mcmv)), '') as fase_mcmv,

            -- Valores
            {{ parse_financial_value('vr_total_ts') }} as vr_global_ts,
            {{ parse_financial_value('vr_desembolsado_ts') }} as vr_desembolsado_ts,
            {{ parse_financial_value('vr_a_desembolsar_ts') }} as vr_a_desembolsar_nao_concluidos,

            -- Execução e Status
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as percentual_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as percentual_obra,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_ts)), '') as situacao_ts,
            nullif(trim({{ target.schema }}.corrigir_mojibake(motivo_situacao_ts_atrasado_paralisado)), '') as motivo_situacao_ts,

            -- Outros Metadados
            -- portaria como veio da origem ("None" da planilha vira nulo)
            case when upper(trim(portaria_ts_utilizada::text)) in ('NONE', 'NAN', 'NULL') then null
                 else nullif(trim({{ target.schema }}.corrigir_mojibake(portaria_ts_utilizada)), '') end as portaria_adotada,
            -- portaria no formato único número/ano (21/2014, 464/2018, 75/2025)
            {{ portaria_ts_normalizada("portaria_ts_utilizada") }} as portaria_ts,
            nullif(trim({{ target.schema }}.corrigir_mojibake(instrumento_de_planejamento)), '') as instrumento_planejamento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(forma_natureza_de_execucao_direta_indireta_mista_pelo_af)), '') as natureza_execucao,

            -- Datas
            {{ target.schema }}.parse_date_br(data_contratacao_empreendimento) as dt_contratacao,
            {{ target.schema }}.parse_date_br(data_primeiro_relatorio) as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(data_ultimo_relatorio) as dt_ultimo_relatorio,
            {{ target.schema }}.parse_date_br(data_da_assinatura_do_primeiro_contrato_de_pessoa_fisica) as dt_entrega,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_base_trabalho_social_pnhr_bb") }}
    )

select *
from ts_bb_raw
{% endmacro %}

{% macro corpo_prata_rural_trabalho_social_caixa() %}
-- Silver: Trabalho Social PNHR CAIXA
-- Fonte: bronze.bronze_shpt_base_trabalho_social_pnhr_rural_caixa (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados de trabalho social da Caixa limpos e tipados

with
    ts_caixa_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(contrato) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(contrato)), '') as contrato,
            nullif(trim({{ target.schema }}.corrigir_mojibake(recurso)), '') as recurso,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_empreendimento)), '') as empreendimento_nome,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(uf)), '') as uf,

            -- Quantidades e Tipologia
            {{ parse_int('uh') }} as qt_uh,
            nullif(trim({{ target.schema }}.corrigir_mojibake(tipologia)), '') as tipologia,
            nullif(trim({{ target.schema }}.corrigir_mojibake(fase_mcmv)), '') as fase_mcmv,

            -- Valores
            {{ parse_financial_value('vr_global_ts') }} as vr_global_ts,
            {{ parse_financial_value('vr_desembolsado') }} as vr_desembolsado_ts,
            {{ parse_financial_value('vr_a_desembolsar_nao_concluidos') }} as vr_a_desembolsar_nao_concluidos,
            {{ parse_financial_value('vr_nao_desembolsado_concluidos') }} as vr_nao_desembolsado_concluidos,

            -- Execução e Status
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as percentual_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as percentual_obra,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_ts)), '') as situacao_ts,
            nullif(trim({{ target.schema }}.corrigir_mojibake(motivo_situacao_ts_atrasado_paralisado)), '') as motivo_situacao_ts,

            -- Outros Metadados
            -- portaria como veio da origem ("None" da planilha vira nulo)
            case when upper(trim(portaria_adotada::text)) in ('NONE', 'NAN', 'NULL') then null
                 else nullif(trim({{ target.schema }}.corrigir_mojibake(portaria_adotada)), '') end as portaria_adotada,
            -- portaria no formato único número/ano (21/2014, 464/2018, 75/2025)
            {{ portaria_ts_normalizada("portaria_adotada") }} as portaria_ts,
            nullif(trim({{ target.schema }}.corrigir_mojibake(instrumento_de_planejamento)), '') as instrumento_planejamento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(natureza_execucao)), '') as natureza_execucao,

            -- Datas
            {{ target.schema }}.parse_date_br(data_da_contratacao) as dt_contratacao,
            -- A base da CAIXA não informa a data do primeiro relatório (só a do
            -- BB tem). Mantido nulo para as duas silver terem o mesmo formato.
            null::date as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(dt_entrega) as dt_entrega,
            {{ target.schema }}.parse_date_br(dt_ultimo_avt) as dt_ultimo_avt,
            {{ target.schema }}.parse_date_br(dt_avf) as dt_avf,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_base_trabalho_social_pnhr_rural_caixa") }}
    )

select *
from ts_caixa_raw
{% endmacro %}

{% macro corpo_prata_rural_portarias_selecao() %}
-- Prata: catálogo das portarias de seleção do Rural, tipado.
-- Fonte: bronze_seed_portarias_selecao_rural. Grão: portaria.
-- `dt_referencia_vinculo` é a data a partir da qual um contrato pode ter vindo da portaria: a data
-- da portaria ou, enquanto ela não estiver no catálogo, 1º de janeiro do ano da portaria.

select
    nullif(trim(portaria_codigo::text), '') as portaria_codigo,
    nullif(trim(ato_selecao::text), '') as ato_selecao,
    {{ parse_int('numero::text') }} as numero_portaria,
    {{ parse_int('ano::text') }} as ano_portaria,
    nullif(trim(dt_portaria::text), '')::date as dt_portaria,
    coalesce(
        nullif(trim(dt_portaria::text), '')::date,
        make_date({{ parse_int('ano::text') }}, 1, 1)
    ) as dt_referencia_vinculo,
    nullif(trim(papel::text), '') as papel_portaria,
    -- Tipo usado no filtro único de portaria do painel. "Seleção" junta o resultado e o
    -- reaproveitamento (as duas publicam/afetam a lista de selecionados).
    case nullif(trim(papel::text), '')
        when 'Resultado da seleção' then 'Seleção'
        when 'Enquadramento (Calamidade)' then 'Seleção'
        when 'Reaproveitamento' then 'Seleção'
        when 'Alteração' then 'Seleção'
        when 'Autorização de contratação' then 'Autorização de contratação'
        when 'Abertura do ciclo' then 'Abertura do ciclo'
        else 'A confirmar'
    end as tipo_portaria,
    nullif(trim(ciclo_selecao::text), '') as ciclo_selecao,
    nullif(trim(tem_lista_nominal::text), '') as tem_lista_nominal,
    nullif(trim(fonte_lista::text), '') as fonte_lista,
    nullif(trim(observacao::text), '') as observacao
from {{ ref("bronze_seed_portarias_selecao_rural") }}
where nullif(trim(portaria_codigo::text), '') is not null
{% endmacro %}
{% macro corpo_prata_rural_selecao_portarias() %}
-- Prata: propostas selecionadas por todas as portarias de seleção do Rural (e enquadradas no rito de
-- calamidade do RS, Portaria 173/2024), num formato só.
-- Fontes: a 162/2018 vem do anexo do SFTP (mesma lógica da prata_rural_selecao_portaria_162);
-- as demais vêm do seed perguntas_mcid/selecao_portarias_rural, preenchido com os anexos que a
-- SNH mandar. Grão: linha do anexo (proposta/empreendimento selecionado).
--
-- O anexo da 162/2018 traz o código do empreendimento (vira APF). Os anexos novos costumam trazer
-- só EO, município e UH; nesses o vínculo com o contrato é feito na prata_rural_selecao_contrato,
-- pelo CNPJ da EO e pelo município.

with
    cat as ({{ corpo_prata_rural_portarias_selecao() }}),
    p162 as ({{ corpo_prata_rural_selecao_portaria_162() }}),

    anexos as (
        select
            '162_2018'::text as portaria_codigo,
            p.uf,
            p.municipio,
            null::text as cod_ibge,
            p.entidade_organizadora_cnpj as eo_cnpj_selecionada,
            null::text as eo_nome_selecionada,
            null::text as protocolo_proposta,
            p.apf,
            p.codigo_empreendimento,
            p.quantidade_uh_selecionadas,
            'Selecionada'::text as situacao_selecao,
            null::text as observacao_selecao,
            'SFTP (Anexo Rural da Portaria 162/2018)'::text as fonte_linha,
            p.arquivo_de_origem
        from p162 p

        union all

        select
            nullif(trim(a.portaria_codigo::text), ''),
            upper(nullif(trim(a.uf::text), '')),
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(a.municipio::text)), '')),
            nullif(regexp_replace(a.cod_ibge::text, '[^0-9]', '', 'g'), ''),
            {{ cnpj_normalizado("a.eo_cnpj") }},
            nullif(trim({{ target.schema }}.corrigir_mojibake(a.eo_nome::text)), ''),
            nullif(trim(a.protocolo_proposta::text), ''),
            case
                when nullif(trim(a.codigo_empreendimento::text), '') is not null
                    then {{ target.schema }}.normalize_apf(a.codigo_empreendimento::text)
            end,
            nullif(trim(a.codigo_empreendimento::text), ''),
            {{ parse_int('a.quantidade_uh_selecionadas::text') }},
            coalesce(nullif(trim(a.situacao_selecao::text), ''), 'Selecionada'),
            nullif(trim(a.observacao::text), ''),
            'Seed perguntas_mcid/selecao_portarias_rural'::text,
            'selecao_portarias_rural.csv'::text
        from {{ ref("bronze_seed_selecao_portarias_rural") }} a
        where nullif(trim(a.portaria_codigo::text), '') is not null

        union all

        -- Anexos extraídos dos PDFs e carregados no schema seeds do Postgres (bronzes bronze_seed_manual_*):
        -- 354/2024 (seleção 2023, compilada) e 697/2026 (seleção 2025). Trazem o número da proposta,
        -- EO, município e UH, mas não o APF. Propostas excluídas por portaria posterior ficam de fora.
        select
            m.portaria_codigo,
            upper(nullif(trim(m.uf::text), '')),
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(m.municipio::text)), '')),
            null::text,
            {{ cnpj_normalizado("m.cnpj_eo") }},
            nullif(trim({{ target.schema }}.corrigir_mojibake(m.entidade_organizadora::text)), ''),
            lower(nullif(trim(m.id_proposta::text), '')),
            null::text,
            null::text,
            m.qtd_uh::integer,
            case lower(trim(m.situacao::text))
                when 'alterada' then 'Selecionada (alterada)'
                when 'incluída' then 'Selecionada (incluída)'
                else 'Selecionada'
            end,
            nullif(trim(m.nota_alteracao::text), ''),
            'Anexo extraído do PDF (seeds.' || m.tabela || ')',
            m.portaria_arquivo::text
        from (
            select '354_2024'::text as portaria_codigo, 'rural_portaria_selecao_354_2024'::text as tabela, *
            from {{ ref("bronze_seed_manual_rural_portaria_selecao_354_2024") }}
            union all
            select '697_2026'::text, 'rural_portaria_selecao_697_2026'::text, *
            from {{ ref("bronze_seed_manual_rural_portaria_selecao_697_2026") }}
        ) m
        where coalesce(lower(trim(m.situacao::text)), '') <> 'excluída'

        union all

        -- Calamidade RS (rito da 1.417/2023): anexos vigentes da 173/2024, da 840/2025 e da 1.207/2025, na
        -- redação dada pela 1.207/2025 (bronze_seed_manual_rural_portaria_enquadramento_1207_2025). Trazem a APF, então o
        -- vínculo com o contrato é exato. O proponente é a entidade privada (tratada como EO).
        select
            nullif(trim(c.portaria_codigo::text), ''),
            upper(nullif(trim(c.uf::text), '')),
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(c.municipio::text)), '')),
            null::text,
            {{ cnpj_normalizado("c.cnpj_eo") }},
            null::text,
            null::text,
            {{ target.schema }}.normalize_apf(c.apf::text),
            nullif(trim(c.apf::text), ''),
            c.qt_uh::integer,
            'Enquadrada (Calamidade)'::text,
            'Redação dada pela Portaria ' || replace(c.redacao_dada_por::text, '_', '/'),
            'Anexo vigente extraído do PDF (seeds.rural_portaria_enquadramento_1207_2025)'::text,
            c.arquivo::text
        from {{ ref("bronze_seed_manual_rural_portaria_enquadramento_1207_2025") }} c
    ),

    numerado as (
        select
            a.*,
            row_number() over (
                partition by a.portaria_codigo, a.protocolo_proposta, a.codigo_empreendimento,
                    a.eo_cnpj_selecionada, a.cod_ibge, a.uf, a.municipio, a.quantidade_uh_selecionadas
                order by a.arquivo_de_origem
            ) as ordem_repeticao
        from anexos a
    )

select
    md5(concat_ws('|', a.portaria_codigo, a.protocolo_proposta, a.codigo_empreendimento,
        a.eo_cnpj_selecionada, a.cod_ibge, a.uf, a.municipio,
        a.quantidade_uh_selecionadas::text, a.ordem_repeticao::text)) as id_selecao,
    a.portaria_codigo,
    coalesce(c.ato_selecao, 'Portaria ' || a.portaria_codigo) as ato_selecao,
    c.papel_portaria,
    c.dt_portaria,
    c.dt_referencia_vinculo,
    c.ciclo_selecao,
    a.uf,
    a.municipio,
    a.cod_ibge,
    {{ municipio_chave("a.municipio") }} as municipio_chave,
    a.eo_cnpj_selecionada,
    a.eo_nome_selecionada,
    a.protocolo_proposta,
    a.apf,
    a.codigo_empreendimento,
    a.quantidade_uh_selecionadas,
    a.situacao_selecao,
    a.observacao_selecao,
    (c.portaria_codigo is null) as ic_portaria_fora_do_catalogo,
    a.fonte_linha,
    a.arquivo_de_origem
from numerado a
left join cat c on c.portaria_codigo = a.portaria_codigo
{% endmacro %}
{% macro corpo_prata_rural_selecao_contrato() %}
-- Prata: vínculo de cada proposta selecionada (todas as portarias de seleção do Rural) com o
-- contrato que saiu dela. Grão: linha do anexo da portaria (id_selecao).
--
-- Universo de contratos: a visão consolidada do Rural (prata_rural_operacao_eo, uma linha por
-- APF) e, para APF que não estão nela, o INT065 (PNHR CAIXA), no retrato mais recente.
--
-- Ordem de tentativa (vale a primeira que achar contrato):
--   1. APF: quando o anexo traz o código do empreendimento (caso da 162/2018).
--   2. CNPJ da EO + município pelo código IBGE (6 primeiros dígitos, porque a SNH guarda sem o DV).
--   3. CNPJ da EO + município pelo nome e UF, quando falta o código IBGE de um dos lados.
-- Nos passos 2 e 3 só entram contratos assinados a partir da portaria (dt_referencia_vinculo).
-- Se houver mais de um contrato candidato, fica o de UH contratadas mais perto das UH
-- selecionadas e, no empate, o mais antigo. `confianca_vinculo` resume o quanto o vínculo é firme
-- e `qt_selecoes_mesmo_contrato` mostra quando duas linhas do anexo caíram no mesmo contrato.

with
    sel as ({{ corpo_prata_rural_selecao_portarias() }}),
    op as ({{ corpo_prata_rural_operacao_eo() }}),
    pnhr_raw as ({{ corpo_prata_rural_pnhr_caixa() }}),

    pnhr as (
        select distinct on (apf) *
        from pnhr_raw
        where apf is not null
        order by apf, dt_movimento desc nulls last
    ),

    contratos as (
        select
            o.apf,
            o.agente_financeiro,
            'visão consolidada Rural'::text as fonte_contrato,
            o.eo_cnpj,
            o.eo_nome,
            o.uf,
            o.municipio,
            left(nullif(regexp_replace(o.cod_ibge::text, '[^0-9]', '', 'g'), ''), 6) as cod_ibge6,
            {{ municipio_chave("o.municipio") }} as municipio_chave,
            o.dt_contratacao,
            o.quantidade_uh_contratadas as uh_contratadas,
            o.quantidade_uh_entregues as uh_entregues,
            o.status_operacao,
            o.situacao_empreendimento as situacao_origem
        from op o

        union all

        select
            p.apf,
            'CAIXA'::text,
            'INT065 PNHR CAIXA'::text,
            {{ cnpj_normalizado("p.eo_cnpj") }},
            p.eo_nome,
            p.uf,
            p.municipio,
            left(nullif(regexp_replace(p.cod_ibge::text, '[^0-9]', '', 'g'), ''), 6),
            {{ municipio_chave("p.municipio") }},
            p.dt_contrato,
            p.qt_unidades,
            p.qt_unidades_entregues,
            case
                when p.situacao_obra ~* 'conclu' then 'Concluída'
                when p.situacao_obra ~* 'paralis' then 'Paralisada'
                when p.situacao_obra is not null then 'Em obra'
            end,
            p.situacao_obra
        from pnhr p
        where not exists (select 1 from op o where o.apf = p.apf)
    ),

    candidatos as (
        select s.id_selecao, c.*, 1 as prioridade, 'APF'::text as metodo_vinculo
        from sel s
        join contratos c on c.apf = s.apf
        where s.apf is not null

        union all

        select s.id_selecao, c.*, 2, 'CNPJ da EO + município (código IBGE)'::text
        from sel s
        join contratos c
            on c.eo_cnpj = s.eo_cnpj_selecionada
            and c.cod_ibge6 = left(s.cod_ibge, 6)
        where s.eo_cnpj_selecionada is not null
          and s.cod_ibge is not null
          and (s.dt_referencia_vinculo is null or c.dt_contratacao is null
               or c.dt_contratacao >= s.dt_referencia_vinculo)

        union all

        select s.id_selecao, c.*, 3, 'CNPJ da EO + município (nome e UF)'::text
        from sel s
        join contratos c
            on c.eo_cnpj = s.eo_cnpj_selecionada
            and c.uf = s.uf
            and c.municipio_chave = s.municipio_chave
        where s.eo_cnpj_selecionada is not null
          and s.municipio_chave is not null
          and (s.cod_ibge is null or c.cod_ibge6 is null)
          and (s.dt_referencia_vinculo is null or c.dt_contratacao is null
               or c.dt_contratacao >= s.dt_referencia_vinculo)
    ),

    melhor_passo as (
        select id_selecao, min(prioridade) as prioridade
        from candidatos
        group by id_selecao
    ),

    ordenados as (
        select
            c.*,
            count(*) over (partition by c.id_selecao) as qt_contratos_candidatos,
            row_number() over (
                partition by c.id_selecao
                order by abs(coalesce(c.uh_contratadas, 0) - coalesce(s.quantidade_uh_selecionadas, 0)),
                    c.dt_contratacao nulls last,
                    c.apf
            ) as ordem
        from candidatos c
        join melhor_passo m on m.id_selecao = c.id_selecao and m.prioridade = c.prioridade
        join sel s on s.id_selecao = c.id_selecao
    ),

    escolhido as (select * from ordenados where ordem = 1)

select
    s.id_selecao,
    s.portaria_codigo,
    s.ato_selecao,
    s.papel_portaria,
    s.dt_portaria,
    s.ciclo_selecao,
    s.uf,
    s.municipio,
    s.cod_ibge,
    s.eo_cnpj_selecionada,
    s.eo_nome_selecionada,
    s.protocolo_proposta,
    s.codigo_empreendimento,
    s.apf as apf_selecao,
    s.quantidade_uh_selecionadas,
    s.situacao_selecao,
    (e.apf is not null) as ic_contratado,
    coalesce(e.metodo_vinculo, 'Sem vínculo') as metodo_vinculo,
    case
        when e.apf is null then null
        when e.prioridade = 1 then 'Exata'
        when e.qt_contratos_candidatos = 1 and e.uh_contratadas = s.quantidade_uh_selecionadas then 'Alta'
        when e.qt_contratos_candidatos = 1 then 'Média'
        else 'Baixa'
    end as confianca_vinculo,
    coalesce(e.qt_contratos_candidatos, 0) as qt_contratos_candidatos,
    e.apf as apf_contrato,
    e.agente_financeiro,
    e.fonte_contrato,
    e.dt_contratacao,
    e.dt_contratacao - s.dt_portaria as dias_selecao_ate_contrato,
    e.eo_cnpj as eo_cnpj_contrato,
    e.eo_nome as eo_nome_contrato,
    case when e.apf is not null then e.eo_cnpj = s.eo_cnpj_selecionada end as ic_mesma_eo,
    e.uh_contratadas,
    e.uh_entregues,
    coalesce(e.status_operacao, 'Não contratado (sem vínculo)') as status_operacao,
    e.situacao_origem,
    case when e.apf is not null then count(*) over (partition by e.apf) end as qt_selecoes_mesmo_contrato,
    s.ic_portaria_fora_do_catalogo,
    s.fonte_linha,
    s.arquivo_de_origem
from sel s
left join escolhido e on e.id_selecao = s.id_selecao
{% endmacro %}
{% macro corpo_prata_rural_autorizacao_proposta() %}
-- Prata: propostas autorizadas a contratar pelas portarias de autorização do Rural, tipadas e com o
-- catálogo. Fonte: bronze_seed_manual_rural_portaria_autorizacao (anexos extraídos dos PDFs) e o catálogo
-- bronze_seed_portarias_selecao_rural. Grão: portaria de autorização × proposta (id_autorizacao).
--
-- O número da proposta (UUID) é o mesmo do anexo de seleção (354/2024): é por ele que o funil liga
-- seleção → autorização. CNPJ normalizado para 14 dígitos; município com a mesma chave do resto do Rural.

with
    cat as ({{ corpo_prata_rural_portarias_selecao() }}),

    a as (
        select
            nullif(trim(b.portaria_codigo::text), '') as portaria_codigo,
            upper(nullif(trim(b.uf::text), '')) as uf,
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(b.municipio::text)), '')) as municipio,
            {{ cnpj_normalizado("b.eo_cnpj") }} as eo_cnpj,
            nullif(trim({{ target.schema }}.corrigir_mojibake(b.eo_nome::text)), '') as eo_nome,
            lower(nullif(trim(b.id_proposta::text), '')) as protocolo_proposta,
            nullif(trim(b.empreendimento::text), '') as empreendimento_nome,
            {{ parse_int('b.qt_uh::text') }} as quantidade_uh_autorizadas,
            {{ parse_int('b.pagina::text') }} as pagina,
            nullif(trim(b.dt_portaria::text), '')::date as dt_portaria_origem,
            nullif(trim(b.arquivo::text), '') as arquivo_de_origem,
            nullif(trim(b.metodo::text), '') as metodo_extracao,
            b.tabela_origem
        from {{ ref("bronze_seed_manual_rural_portaria_autorizacao") }} b
    )

select
    md5(concat_ws('|', a.portaria_codigo, a.protocolo_proposta)) as id_autorizacao,
    a.portaria_codigo,
    coalesce(c.ato_selecao, 'Portaria ' || replace(a.portaria_codigo, '_', '/')) as ato_autorizacao,
    coalesce(c.dt_portaria, a.dt_portaria_origem) as dt_autorizacao,
    c.ciclo_selecao,
    a.uf,
    a.municipio,
    {{ municipio_chave("a.municipio") }} as municipio_chave,
    a.eo_cnpj,
    a.eo_nome,
    a.protocolo_proposta,
    a.empreendimento_nome,
    a.quantidade_uh_autorizadas,
    (c.portaria_codigo is null) as ic_portaria_fora_do_catalogo,
    a.pagina,
    a.metodo_extracao,
    a.tabela_origem,
    a.arquivo_de_origem
from a
left join cat c on c.portaria_codigo = a.portaria_codigo
where a.portaria_codigo is not null
{% endmacro %}
{% macro corpo_prata_rural_proposta_funil() %}
-- Prata: funil de cada proposta selecionada do Rural — seleção → autorização de contratação → contrato.
-- Grão: linha do anexo da portaria de seleção (id_selecao), a mesma da prata_rural_selecao_contrato,
-- acrescida da autorização que liberou a contratação.
--
-- Seleção → contrato: regra da prata_rural_selecao_contrato (APF; CNPJ da EO + município).
-- Seleção → autorização: pelo número da proposta (UUID), que é o mesmo nos dois anexos. Se uma proposta
-- aparecer em mais de uma autorização, vale a mais antiga e `qt_autorizacoes` mostra quantas são.
--
-- `etapa_funil` é a etapa mais avançada com evidência:
--   1. Selecionada        — está no anexo de seleção, sem autorização carregada e sem contrato;
--   2. Autorizada         — está numa autorização carregada, sem contrato identificado;
--   3. Contratada         — tem contrato vinculado (com ou sem autorização carregada).
-- Hoje só 4 das 46 autorizações da Seleção 2023 estão carregadas; por isso "Contratada sem autorização
-- carregada" é falta de dado, não irregularidade.

with
    sel as ({{ corpo_prata_rural_selecao_contrato() }}),
    aut as ({{ corpo_prata_rural_autorizacao_proposta() }}),

    aut_por_proposta as (
        select
            a.*,
            count(*) over (partition by a.protocolo_proposta) as qt_autorizacoes,
            row_number() over (
                partition by a.protocolo_proposta
                order by a.dt_autorizacao nulls last, a.portaria_codigo
            ) as ordem
        from aut a
        where a.protocolo_proposta is not null
    )

select
    s.*,
    a.portaria_codigo as portaria_autorizacao_codigo,
    replace(a.portaria_codigo, '_', '/') as portaria_autorizacao,
    a.ato_autorizacao,
    a.dt_autorizacao,
    a.quantidade_uh_autorizadas,
    (a.id_autorizacao is not null) as ic_autorizada,
    coalesce(a.qt_autorizacoes, 0) as qt_autorizacoes,
    case when a.id_autorizacao is not null then a.quantidade_uh_autorizadas = s.quantidade_uh_selecionadas end
        as ic_uh_autorizada_igual_selecionada,
    case when a.id_autorizacao is not null and s.dt_contratacao is not null
        then s.dt_contratacao - a.dt_autorizacao end as dias_autorizacao_ate_contrato,
    (a.id_autorizacao is not null and s.dt_contratacao is not null and s.dt_contratacao < a.dt_autorizacao)
        as ic_contrato_antes_da_autorizacao,
    case
        when s.ic_contratado then '3. Contratada'
        when a.id_autorizacao is not null then '2. Autorizada'
        else '1. Selecionada'
    end as etapa_funil,
    case
        when s.ic_contratado and a.id_autorizacao is not null then 'Contratada com autorização carregada'
        when s.ic_contratado then 'Contratada sem autorização carregada'
        when a.id_autorizacao is not null then 'Autorizada, sem contrato identificado'
        else 'Selecionada, sem autorização carregada e sem contrato'
    end as situacao_funil
from sel s
left join aut_por_proposta a
    on a.protocolo_proposta = s.protocolo_proposta
    and a.ordem = 1
{% endmacro %}
