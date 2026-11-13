{#
    Corpos SQL de modelos do produto pro_moradia que outros modelos da MESMA camada precisam reaproveitar.

    Regra das camadas (medallion estrito): bronze lê a fonte; prata lê só bronze; ouro lê só prata.
    Quando uma prata precisava do resultado de outra prata (ou uma ouro de outra ouro), em vez de
    ler a tabela ela chama o corpo da outra como subconsulta. O SQL é o mesmo, então o dado é o
    mesmo; muda só de onde ele é lido: a linhagem passa a ir da camada anterior direto ao modelo.

    Cada modelo listado aqui é uma casca fina (config + chamada do corpo) em models/.
#}

{% macro corpo_prata_pro_moradia_contrato() %}
-- Prata: Contrato do Pró-Moradia — visão unificada
-- Fonte: bronze_shpt_fgts_canal_contratos, filtrada para `cod_linha = '26'`, enriquecida com o
-- cadastro do empreendimento, a última posição de obra e as tabelas de domínio do Canal FGTS.
--
-- Por que a linha, e não a modalidade: o Canal FGTS traz todos os programas do fundo (Apoio à
-- Produção, Carta de Crédito, Pró-Transporte, Saneamento para Todos...) e o Pró-Moradia não é
-- uma modalidade — é a LINHA `26` (`HAB / PRO-MORADIA` em bronze_shpt_fgts_canal_tdom_linha).
-- A modalidade diz o TIPO de intervenção dentro do programa (urbanização, produção de conjunto,
-- cesta de materiais...). O teste `pro_moradia_linha_26_e_pro_moradia` falha se o código mudar
-- de significado na origem.
--
-- Grão: um contrato. Conferido em 2026-09 sobre a remessa MC20260220: 686 contratos, um por
-- empreendimento, e `cod_contrato` já é único sem o dígito verificador.
--
-- Datas do canal vêm como `MM/DD/YY HH:MI:SS` (padrão americano, ano com dois dígitos): o
-- `parse_date_br` não reconhece esse formato e devolveria NULL em todas.

with
    contrato as (
        select *
        from {{ ref("bronze_shpt_fgts_canal_contratos") }}
        where trim(cod_linha::text) = '26'
    ),

    empreendimento as (
        select * from {{ ref("bronze_shpt_fgts_canal_empreendimentos") }}
    ),

    posicao as (
        select * from {{ ref("bronze_shpt_fgts_canal_empreendimentos_posicoes") }}
    ),

    linha as (
        select trim(codigo::text) as codigo, trim(linha::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_linha") }}
    ),

    modalidade as (
        select trim(codigo::text) as codigo, trim(modalidade::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_modalidade") }}
    ),

    situacao as (
        select trim(codigo::text) as codigo, trim(situacao_do_contrato::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_situacao_contrato") }}
    ),

    entidade as (
        select
            lpad(trim(codigo::text), 5, '0') as codigo,
            nullif(trim(entidade::text), '') as nome,
            nullif(regexp_replace(cgc::text, '[^0-9]', '', 'g'), '') as cnpj
        from {{ ref("bronze_shpt_fgts_canal_tdom_entidades") }}
    ),

    municipio as (
        select
            lpad(trim(codigo::text), 4, '0') as codigo,
            nullif(trim(municipio::text), '') as nome,
            nullif(trim(uf::text), '') as uf,
            nullif(trim(ibge_codigo::text), '') as cod_ibge
        from {{ ref("bronze_shpt_fgts_canal_tdom_municipios") }}
    ),

    uor as (
        select trim(codigo::text) as codigo, nullif(trim(uor::text), '') as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_uor") }}
    )

select
    -- Identificação
    trim(c.cod_contrato::text) as cod_contrato,
    trim(c.cod_contrato_dv::text) as cod_contrato_dv,
    trim(c.cod_contrato::text) || '-' || trim(c.cod_contrato_dv::text) as contrato,
    trim(c.cod_empreendimento::text) as cod_empreendimento,
    nullif(trim(c.cod_operacao_agente_financeiro::text), '') as cod_operacao_agente_financeiro,

    -- Programa e tipo de intervenção
    trim(c.cod_linha::text) as linha_codigo,
    l.nome as linha,
    trim(c.cod_modalidade::text) as modalidade_codigo,
    m.nome as modalidade,
    -- As quatro frentes da carta-consulta do Pró-Moradia (Desenvolvimento Institucional,
    -- Provisão Habitacional, Provisão de Lote Urbanizado e Urbanização), por código de
    -- modalidade. Lote urbanizado entra em provisão: as modalidades de lote do canal
    -- (014, 019, 082) são mistas com unidade habitacional.
    case
        when trim(c.cod_modalidade::text) in ('002', '003', '070', '078', '218', '981')
            then 'Urbanização'
        when trim(c.cod_modalidade::text) in ('014', '018', '019', '021', '082', '194')
            then 'Provisão habitacional'
        when trim(c.cod_modalidade::text) = '011'
            then 'Desenvolvimento institucional'
        when trim(c.cod_modalidade::text) = '221'
            then 'Situação de emergência'
        else 'Não classificada'
    end as tipo_intervencao,

    -- Empreendimento
    upper(nullif(trim(e.txt_nome_empreendimento::text), '')) as empreendimento_nome,
    nullif(trim(e.txt_projeto::text), '') as projeto,
    nullif(trim(e.txt_objeto::text), '') as objeto,

    -- Tomador: o ente público que contrai o financiamento
    lpad(trim(c.cod_tomador::text), 5, '0') as tomador_codigo,
    t.nome as tomador_nome,
    t.cnpj as tomador_cnpj,
    case
        when t.nome ~* '^(ESTADO|GOVERNO DO ESTADO)' or t.nome ~* '^DISTRITO FEDERAL' then 'Estado/DF'
        when t.nome ~* '^(MUNICIPIO|PREFEITURA)' then 'Município'
        when t.nome is null then null
        else 'Companhia ou autarquia'
    end as tomador_esfera,

    -- Agente financeiro e acompanhamento
    trim(c.cod_af::text) as agente_financeiro_codigo,
    af.nome as agente_financeiro,
    trim(c.cod_uor::text) as uor_codigo,
    u.nome as uor,

    -- Localização: a UF do contrato é a referência; o município vem do empreendimento.
    -- Conferido em 2026-09: nos 678 contratos com município traduzido, as duas UFs batem.
    coalesce(nullif(trim(c.uf::text), ''), mu.uf) as uf,
    mu.nome as municipio,
    mu.cod_ibge,

    -- Situação do contrato
    trim(c.cod_situacao_contrato::text) as situacao_contrato_codigo,
    s.nome as situacao_contrato,
    -- distratado (01, 04), cancelado (02, 10, 21) ou extinto (03): o contrato deixou de
    -- existir. Cláusula suspensiva e liminar seguem vigentes.
    trim(c.cod_situacao_contrato::text) not in ('01', '02', '03', '04', '10', '21') as ic_contrato_vigente,

    -- Datas
    case
        when trim(c.dte_assinatura::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(c.dte_assinatura::text), 8), 'MM/DD/YY')
    end as dt_assinatura,
    {{ parse_int("c.dte_orcamento_ano::text") }} as ano_orcamento,

    -- Valores (o canal usa ponto decimal): o financiamento é o contratado; o investimento
    -- soma a contrapartida do tomador.
    {{ parse_numeric("c.vlr_contratado::text", "numeric(15, 2)") }} as valor_contratado,
    {{ parse_numeric("c.vlr_investimento::text", "numeric(15, 2)") }} as valor_investimento,
    {{ parse_numeric("c.vlr_investimento::text", "numeric(15, 2)") }}
    - {{ parse_numeric("c.vlr_contratado::text", "numeric(15, 2)") }} as valor_contrapartida,

    -- Metas: 0 UH é legítimo em urbanização, que não produz unidade.
    {{ parse_int("e.qtd_unidades_financiadas::text") }} as quantidade_uh_financiadas,
    {{ parse_numeric("e.qtd_populacao_beneficiada::text", "numeric(15, 0)") }} as populacao_beneficiada,
    {{ parse_numeric("e.qtd_empregos_gerados::text", "numeric(15, 0)") }} as empregos_gerados,

    -- PAC e seleção de origem
    trim(c.cod_pac::text) = 'S' as ic_pac,
    nullif(trim(c.cod_ident_externo::text), '') as identificador_selecao,

    -- Última posição de obra do empreendimento (o acompanhamento mensal por contrato está em
    -- prata_pro_moradia_execucao_obra; esta é a foto do cadastro)
    {{ parse_numeric("p.prc_obra_executada_ult::text", "numeric(6, 2)") }} as percentual_obra_posicao,
    case
        when trim(p.dte_ano_mes::text) ~ '^\d{6}$'
        then to_date(trim(p.dte_ano_mes::text) || '01', 'YYYYMMDD')
    end as dt_posicao,
    case
        when trim(p.dt_inicio::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(p.dt_inicio::text), 8), 'MM/DD/YY')
    end as dt_inicio_obra,
    case
        when trim(p.dt_termino::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(p.dt_termino::text), 8), 'MM/DD/YY')
    end as dt_termino_obra,
    case
        when trim(p.dt_inauguracao::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(p.dt_inauguracao::text), 8), 'MM/DD/YY')
    end as dt_inauguracao,

    -- Linhagem: a remessa do Canal FGTS (MCaaaammdd.zip) de onde veio o contrato
    nullif(trim(c.arquivo_de_origem::text), '') as remessa_canal_fgts,
    to_date(substring(c.arquivo_de_origem::text from 'MC(\d{8})'), 'YYYYMMDD') as dt_remessa,
    nullif(trim(c._ingested_at::text), '')::timestamp as criado_em

from contrato c
left join empreendimento e on trim(e.cod_empreendimento::text) = trim(c.cod_empreendimento::text)
left join posicao p on trim(p.cod_empreendimento::text) = trim(c.cod_empreendimento::text)
left join linha l on l.codigo = trim(c.cod_linha::text)
left join modalidade m on m.codigo = trim(c.cod_modalidade::text)
left join situacao s on s.codigo = trim(c.cod_situacao_contrato::text)
left join entidade t on t.codigo = lpad(trim(c.cod_tomador::text), 5, '0')
left join entidade af on af.codigo = lpad(trim(c.cod_af::text), 5, '0')
left join municipio mu on mu.codigo = lpad(trim(e.cod_municipio::text), 4, '0')
left join uor u on u.codigo = trim(c.cod_uor::text)
{% endmacro %}

{% macro corpo_prata_pro_moradia_desembolso_mensal() %}
-- Prata: Desembolsos mensais do Pró-Moradia
-- Fonte: bronze_shpt_fgts_canal_desembolsos, restrita aos contratos de prata_pro_moradia_contrato.
--
-- Grão: contrato × competência. A origem tem 17 pares repetidos em 5.252 lançamentos (conferido
-- em 2026-09); somá-los é o correto, porque são liberações distintas no mesmo mês, e é o que
-- mantém o grão declarado. Valor negativo é estorno e entra com sinal: some do acumulado.
--
-- A série começa em 2000. Contrato assinado antes disso tem liberação anterior ao arquivo,
-- então o acumulado desta tabela é um PISO do desembolsado, não o total.

with
    contrato as (
        select cod_contrato, cod_contrato_dv from ({{ corpo_prata_pro_moradia_contrato() }}) as prata_pro_moradia_contrato
    ),

    lancamento as (
        select
            trim(d.cod_contrato::text) as cod_contrato,
            trim(d.cod_contrato_dv::text) as cod_contrato_dv,
            make_date(
                {{ parse_int("d.dte_ano::text") }}, {{ parse_int("d.dte_mes_ref::text") }}, 1
            ) as dt_competencia,
            {{ parse_numeric("d.vlr_liberado::text", "numeric(15, 2)") }} as vr_liberado
        from {{ ref("bronze_shpt_fgts_canal_desembolsos") }} d
        join contrato c
            on c.cod_contrato = trim(d.cod_contrato::text)
            and c.cod_contrato_dv = trim(d.cod_contrato_dv::text)
    )

select
    cod_contrato,
    cod_contrato_dv,
    cod_contrato || '-' || cod_contrato_dv as contrato,
    dt_competencia,
    sum(vr_liberado) as vr_liberado,
    coalesce(sum(vr_liberado) filter (where vr_liberado < 0), 0) as vr_estornado,
    count(*) as qt_lancamentos
from lancamento
group by cod_contrato, cod_contrato_dv, dt_competencia
{% endmacro %}

{% macro corpo_prata_pro_moradia_execucao_obra() %}
-- Prata: Execução de obra do Pró-Moradia, avaliação mensal
-- Fonte: bronze_shpt_fgts_canal_execucoes_obras, restrita aos contratos de
-- prata_pro_moradia_contrato. A tabela de origem não tem dígito verificador: `cod_contrato`
-- sozinho já é único no canal.
--
-- Grão: contrato × mês de avaliação (conferido único em 2026-09: 12.796 linhas, 665 contratos).
--
-- A origem traz meses FUTUROS (até 2027-12): são linhas do cronograma, com o previsto
-- preenchido e o realizado ainda não medido. Ficam, marcadas em `ic_avaliacao_futura`, porque o
-- previsto é o que permite medir atraso; a última medição válida filtra por elas.

with
    contrato as (
        select cod_contrato from ({{ corpo_prata_pro_moradia_contrato() }}) as prata_pro_moradia_contrato
    ),

    situacao_obra as (
        select trim(codigo::text) as codigo, trim(situacao_da_obra::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_situacao_obra") }}
    )

select
    trim(x.cod_contrato::text) as cod_contrato,
    to_date(trim(x.dte_ano_mes_avaliacao::text) || '01', 'YYYYMMDD') as dt_avaliacao,
    {{ parse_numeric("x.prc_prev_acum_mes::text", "numeric(6, 2)") }} as percentual_obra_previsto,
    {{ parse_numeric("x.prc_real_acum_mes::text", "numeric(6, 2)") }} as percentual_obra_realizado,
    nullif(trim(x.cod_situacao_obra::text), '') as situacao_obra_codigo,
    so.nome as situacao_obra,
    nullif(trim(x.cod_execucao_obra::text), '') as execucao_obra_codigo,
    case
        when trim(x.dte_ultima_vistoria::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(x.dte_ultima_vistoria::text), 8), 'MM/DD/YY')
    end as dt_ultima_vistoria,
    nullif(trim({{ target.schema }}.corrigir_mojibake(x.txt_providencias::text)), '') as providencias,
    to_date(trim(x.dte_ano_mes_avaliacao::text) || '01', 'YYYYMMDD')
    > date_trunc('month', current_date)::date as ic_avaliacao_futura
from {{ ref("bronze_shpt_fgts_canal_execucoes_obras") }} x
join contrato c on c.cod_contrato = trim(x.cod_contrato::text)
left join situacao_obra so on so.codigo = trim(x.cod_situacao_obra::text)
where trim(x.dte_ano_mes_avaliacao::text) ~ '^\d{6}$'
{% endmacro %}

{% macro corpo_prata_pro_moradia_ficha_contrato() %}
-- Prata: Ficha do Contrato (Pró-Moradia)
-- Uma linha por contrato, com as regras de negócio finais: tipo de intervenção, status de
-- execução, execução física × financeira e paralisação. Consome as pratas do produto.
--
-- Física: vale a última avaliação mensal já MEDIDA (não o cronograma futuro); sem ela, a
-- posição do cadastro do empreendimento. A fonte fica ao lado do número.
--
-- Financeira: o canal não informa o estoque desembolsado, só a série de liberações desde 2000.
-- `vr_desembolsado` é a soma da série, e por isso é um PISO para contrato anterior a 2000 —
-- `ic_serie_financeira_incompleta` marca esses casos para o painel não ler 0% como "nada pago".

with
    contrato as (
        select * from ({{ corpo_prata_pro_moradia_contrato() }}) as prata_pro_moradia_contrato
    ),

    ultima_medicao as (
        select distinct on (cod_contrato)
            cod_contrato,
            dt_avaliacao,
            percentual_obra_realizado,
            percentual_obra_previsto,
            situacao_obra_codigo,
            situacao_obra,
            dt_ultima_vistoria
        from ({{ corpo_prata_pro_moradia_execucao_obra() }}) as prata_pro_moradia_execucao_obra
        where not ic_avaliacao_futura
        order by cod_contrato, dt_avaliacao desc
    ),

    financeiro as (
        select
            cod_contrato,
            sum(vr_liberado) as vr_desembolsado,
            min(dt_competencia) as dt_primeiro_desembolso,
            max(dt_competencia) as dt_ultimo_desembolso,
            count(*) as qt_meses_com_desembolso
        from ({{ corpo_prata_pro_moradia_desembolso_mensal() }}) as prata_pro_moradia_desembolso_mensal
        group by cod_contrato
    ),

    paralisacao as (
        select * from ({{ corpo_prata_pro_moradia_paralisacao() }}) as prata_pro_moradia_paralisacao
    ),

    calculado as (
        select
            c.*,
            m.dt_avaliacao as dt_ultima_avaliacao,
            m.situacao_obra_codigo,
            m.situacao_obra,
            m.dt_ultima_vistoria,
            m.percentual_obra_previsto,
            coalesce(m.percentual_obra_realizado, c.percentual_obra_posicao) as pct_fisico,
            case
                when m.percentual_obra_realizado is not null then 'execucao_obra_mensal'
                when c.percentual_obra_posicao is not null then 'posicao_empreendimento'
            end as fonte_percentual_fisico,
            f.vr_desembolsado,
            f.dt_primeiro_desembolso,
            f.dt_ultimo_desembolso,
            coalesce(f.qt_meses_com_desembolso, 0) as qt_meses_com_desembolso,
            case
                when coalesce(c.valor_contratado, 0) > 0 and f.vr_desembolsado is not null
                then round(f.vr_desembolsado / c.valor_contratado * 100, 2)
            end as pct_financeiro,
            p.cod_contrato is not null as ic_paralisada,
            p.dias_sem_evolucao,
            p.faixa_paralisacao,
            p.motivo_paralisacao,
            p.dt_previsao_conclusao
        from contrato c
        left join ultima_medicao m on m.cod_contrato = c.cod_contrato
        left join financeiro f on f.cod_contrato = c.cod_contrato
        left join paralisacao p on p.cod_contrato = c.cod_contrato
    )

select
    -- Identificação
    contrato,
    cod_contrato,
    cod_contrato_dv,
    cod_empreendimento,
    'Pró-Moradia' as programa,
    tipo_intervencao,
    modalidade_codigo,
    modalidade,
    empreendimento_nome,
    objeto,

    -- Tomador e acompanhamento
    tomador_codigo,
    tomador_nome,
    tomador_esfera,
    agente_financeiro,
    uor,

    -- Localização
    uf,
    {{ regiao_da_uf("uf") }} as regiao,
    municipio,
    cod_ibge,
    concat(municipio, '/', uf) as municipio_uf,
    concat(contrato, ' - ', municipio, '/', uf, ' - ', coalesce(empreendimento_nome, objeto)) as contrato_municipio_empreendimento,

    -- Contrato
    situacao_contrato_codigo,
    situacao_contrato,
    ic_contrato_vigente,
    ano_orcamento,
    dt_assinatura,
    ic_pac,
    identificador_selecao,

    -- Escopo
    quantidade_uh_financiadas as quantidade_uh,
    populacao_beneficiada,
    empregos_gerados,

    -- Valores
    valor_contratado,
    valor_investimento,
    valor_contrapartida,
    case
        when coalesce(quantidade_uh_financiadas, 0) > 0
        then round(valor_investimento / quantidade_uh_financiadas, 2)
    end as valor_investimento_por_uh,

    -- Execução física
    pct_fisico as percentual_execucao_fisica,
    fonte_percentual_fisico,
    percentual_obra_previsto,
    situacao_obra_codigo,
    situacao_obra,
    dt_ultima_avaliacao,
    dt_ultima_vistoria,
    dt_inicio_obra,
    dt_termino_obra,

    -- Regra de negócio: status de execução. A ordem importa: contrato que deixou de existir
    -- não está "não iniciado", e obra paralisada com 40% não está "em andamento".
    case
        when not ic_contrato_vigente or situacao_obra_codigo in ('0', 'D') then 'Cancelado ou distratado'
        when situacao_obra_codigo in ('6', '8', 'E') or pct_fisico >= 100 then 'Concluído'
        when ic_paralisada or situacao_obra_codigo in ('4', 'H') then 'Paralisado'
        when situacao_obra_codigo in ('5', 'G') or pct_fisico = 0 then 'Não Iniciado'
        when pct_fisico > 0 then 'Em Andamento'
        else 'Sem Informação'
    end as status_execucao_simplificado,

    -- Execução financeira
    vr_desembolsado,
    pct_financeiro as percentual_execucao_financeira,
    dt_primeiro_desembolso,
    dt_ultimo_desembolso,
    qt_meses_com_desembolso,
    coalesce(dt_assinatura < date '2000-01-01', false) as ic_serie_financeira_incompleta,

    -- Regra de negócio: ritmo físico × financeiro, com a mesma tolerância de 5 pontos do Rural
    case
        when pct_fisico is null or pct_financeiro is null then 'Sem Informação'
        when pct_financeiro - pct_fisico > 5 then 'Desembolso Adiantado'
        when pct_fisico - pct_financeiro > 5 then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro,

    -- Paralisação
    ic_paralisada,
    dias_sem_evolucao,
    faixa_paralisacao,
    motivo_paralisacao,
    dt_previsao_conclusao,

    -- Linhagem
    remessa_canal_fgts,
    dt_remessa
from calculado
{% endmacro %}

{% macro corpo_prata_pro_moradia_paralisacao() %}
-- Prata: Contratos do Pró-Moradia com obra paralisada
-- Fonte: bronze_shpt_fgts_canal_paralisadas_setor_publico, restrita aos contratos de
-- prata_pro_moradia_contrato. É a lista que a CAIXA acompanha com o tomador — 13 contratos do
-- Pró-Moradia em 2026-09 —, com o motivo e o plano de retomada. Uma linha por contrato.

with
    contrato as (
        select cod_contrato, cod_contrato_dv from ({{ corpo_prata_pro_moradia_contrato() }}) as prata_pro_moradia_contrato
    )

select
    trim(p.cod_contrato::text) as cod_contrato,
    trim(p.cod_contrato_dv::text) as cod_contrato_dv,
    {{ parse_int("p.dias_sem_evolucao::text") }} as dias_sem_evolucao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.faixa_paralisacao::text)), '') as faixa_paralisacao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.ultimo_motivo_paralisacao::text)), '') as motivo_paralisacao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.ultimo_entrave_preenchido::text)), '') as entrave,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.situacao_atual::text)), '') as situacao_atual,
    upper(trim({{ target.schema }}.corrigir_mojibake(p.plano_acao_aprovado_af::text))) in ('SIM', 'S') as ic_plano_acao_aprovado,
    case
        when trim(p.dt_ultimo_bm::text) ~ '^\d{4}-\d{2}-\d{2}' then left(trim(p.dt_ultimo_bm::text), 10)::date
    end as dt_ultimo_boletim_medicao,
    {{ target.schema }}.parse_date_br(nullif(trim(p.dt_previsao_conclusao_objeto::text), '')) as dt_previsao_conclusao
from {{ ref("bronze_shpt_fgts_canal_paralisadas_setor_publico") }} p
join contrato c on c.cod_contrato = trim(p.cod_contrato::text)
{% endmacro %}

{% macro corpo_prata_pro_moradia_tomador_recontratacao() %}
-- Prata: Para cada contrato do Pró-Moradia, o que o MESMO tomador (município, estado ou
-- companhia) ainda tinha em aberto quando o assinou. Réplica, para o Pró-Moradia, da pergunta do
-- Rural "concluiu o anterior antes de concorrer ao próximo?".
-- Grão: contrato.
--
-- Contrato anterior "pendente" na data D = assinado antes de D, não cancelado/distratado, e com
-- obra não terminada até D (dt_termino_obra > D, ou sem término e status ainda aberto). A data de
-- assinatura substitui a data da carta-consulta, que não existe no Canal FGTS.

with
    c as (
        select f.*, p.tomador_cnpj
        from ({{ corpo_prata_pro_moradia_ficha_contrato() }}) f
        left join ({{ corpo_prata_pro_moradia_contrato() }}) p on p.cod_contrato = f.cod_contrato
        where f.tomador_codigo is not null and f.dt_assinatura is not null
    ),

    pendente as (
        select
            n.cod_contrato,
            count(a.cod_contrato) as qt_anteriores,
            count(a.cod_contrato) filter (where
                a.status_execucao_simplificado <> 'Cancelado ou distratado'
                and (a.dt_termino_obra > n.dt_assinatura
                     or (a.dt_termino_obra is null
                         and a.status_execucao_simplificado in ('Em Andamento', 'Paralisado', 'Não Iniciado')))
            ) as qt_anteriores_pendentes,
            coalesce(sum(a.valor_contratado) filter (where
                a.status_execucao_simplificado <> 'Cancelado ou distratado'
                and (a.dt_termino_obra > n.dt_assinatura
                     or (a.dt_termino_obra is null
                         and a.status_execucao_simplificado in ('Em Andamento', 'Paralisado', 'Não Iniciado')))
            ), 0) as valor_anteriores_pendentes,
            count(a.cod_contrato) filter (where a.status_execucao_simplificado = 'Paralisado') as qt_anteriores_hoje_paralisados,
            count(a.cod_contrato) filter (where a.dt_termino_obra is null and a.status_execucao_simplificado in ('Concluído', 'Sem Informação')) as qt_anteriores_sem_data_termino
        from c n
        left join c a on a.tomador_codigo = n.tomador_codigo and a.dt_assinatura < n.dt_assinatura
        group by n.cod_contrato
    )

select
    c.cod_contrato,
    c.contrato,
    c.tomador_codigo,
    c.tomador_nome,
    c.tomador_esfera,
    c.tomador_cnpj,
    c.uf,
    c.municipio,
    c.tipo_intervencao,
    c.identificador_selecao,
    c.dt_assinatura,
    extract(year from c.dt_assinatura)::int as ano_assinatura,
    c.valor_contratado,
    c.status_execucao_simplificado,
    p.qt_anteriores,
    p.qt_anteriores_pendentes,
    p.valor_anteriores_pendentes,
    p.qt_anteriores_hoje_paralisados,
    p.qt_anteriores_sem_data_termino,
    p.qt_anteriores_pendentes > 0 as ic_contratou_com_pendencia,
    case
        when p.qt_anteriores = 0 then 'Primeiro contrato do tomador'
        when p.qt_anteriores_pendentes = 0 then 'Anteriores concluídos'
        when p.qt_anteriores_hoje_paralisados > 0 then 'Contratou com anterior pendente (hoje paralisado)'
        else 'Contratou com anterior pendente'
    end as situacao_recontratacao
from c
join pendente p on p.cod_contrato = c.cod_contrato
{% endmacro %}
