{{ config(materialized="table") }}

-- Gold: Infraestrutura de Água e Saneamento Rural (Cisternas e Efluentes)
-- Consolida a quantidade e o investimento em cisternas e efluentes nos empreendimentos rurais.

with
    fichas as (
        select
            apf,
            nome_empreendimento,
            municipio,
            uf,
            programa,
            agente_financeiro,
            quantidade_uh
        from {{ ref("gold_ficha_empreendimento") }}
    ),

    cpj as (
        select
            apf,
            coalesce(qt_cisterna, 0) as qt_cisterna_pj,
            coalesce(vr_cisterna, 0.00) as vr_cisterna_pj,
            coalesce(qt_efluente, 0) as qt_efluente_pj,
            coalesce(vr_efluente, 0.00) as vr_efluente_pj
        from {{ ref("silver_cadastro_pj") }}
    ),

    cpf_agregado as (
        select
            apf,
            sum(coalesce(qt_cisterna, 0)) as qt_cisterna_pf,
            sum(coalesce(qt_efluente, 0)) as qt_efluente_pf
        from {{ ref("silver_cadastro_pf") }}
        group by apf
    ),

    pcx as (
        select
            apf,
            coalesce(vr_cisterna, 0.00) as vr_cisterna_cx,
            coalesce(vr_efluentes, 0.00) as vr_efluentes_cx
        from {{ ref("silver_pnhr_caixa") }}
    ),

    pbb as (
        select
            apf,
            coalesce(vr_cisterna, 0.00) as vr_cisterna_bb,
            coalesce(vr_efluentes, 0.00) as vr_efluentes_bb
        from {{ ref("silver_pnhr_bb") }}
    )

select
    f.apf,
    f.nome_empreendimento,
    f.municipio,
    f.uf,
    f.programa,
    f.agente_financeiro,
    f.quantidade_uh,

    -- Quantidades de Cisternas e Efluentes (cadastro PJ, ou soma do PF). Os coalesce não
    -- terminam em 0: as fontes cobrem ~1% da carteira, e nos outros 99% a leitura correta
    -- é "a origem não informa", não "não tem cisterna".
    coalesce(c.qt_cisterna_pj, cpf.qt_cisterna_pf) as qt_cisternas,
    coalesce(c.qt_efluente_pj, cpf.qt_efluente_pf) as qt_efluentes,

    -- Valores de Investimento
    coalesce(c.vr_cisterna_pj, pcx.vr_cisterna_cx, pbb.vr_cisterna_bb) as valor_investimento_cisternas,
    coalesce(c.vr_efluente_pj, pcx.vr_efluentes_cx, pbb.vr_efluentes_bb) as valor_investimento_efluentes,

    -- Investimento Total em Saneamento Alternativo Rural.
    -- Nulo quando NENHUM dos dois componentes é conhecido; quando um dos dois é conhecido,
    -- soma o que se sabe (o `+` sozinho anularia o total por causa do outro nulo).
    case
        when coalesce(c.vr_cisterna_pj, pcx.vr_cisterna_cx, pbb.vr_cisterna_bb) is null
         and coalesce(c.vr_efluente_pj, pcx.vr_efluentes_cx, pbb.vr_efluentes_bb) is null
        then null
        else coalesce(coalesce(c.vr_cisterna_pj, pcx.vr_cisterna_cx, pbb.vr_cisterna_bb), 0.00)
           + coalesce(coalesce(c.vr_efluente_pj, pcx.vr_efluentes_cx, pbb.vr_efluentes_bb), 0.00)
    end as valor_investimento_saneamento_total,

    -- Permite separar "não instalou" de "não sabemos", que a soma de zeros misturava.
    (
        coalesce(c.qt_cisterna_pj, cpf.qt_cisterna_pf) is not null
        or coalesce(c.qt_efluente_pj, cpf.qt_efluente_pf) is not null
    ) as tem_informacao_saneamento

from fichas f
left join cpj c on f.apf = c.apf
left join cpf_agregado cpf on f.apf = cpf.apf
left join pcx on f.apf = pcx.apf and f.agente_financeiro = 'CAIXA'
left join pbb on f.apf = pbb.apf and f.agente_financeiro = 'BB'
