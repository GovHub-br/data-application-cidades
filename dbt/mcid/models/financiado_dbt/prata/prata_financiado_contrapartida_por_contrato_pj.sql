{{ config(materialized="table") }}

-- Prata: Contrapartida e emenda consolidadas no CONTRATO PJ.
-- Fonte: prata_financiado_cidades_emenda + prata_financiado_contrato_pf
-- Grão: uma linha por `cod_contrato` que tenha ao menos um contrato PF vinculado.
--
-- É a tabela que responde "quais contrapartidas estão ligadas a cada contrato PJ" —
-- e é preciso ser explícito sobre o que ela NÃO responde.
--
-- O que ela responde: o VALOR. A ponte é o campo `operacao` do lado PF, que casa com
-- `cod_contrato` do lado PJ. A resolução é alta mas não total: parte das operações
-- citadas pelo lado PF não existe no retrato de contratos da mesma remessa, e essas
-- ficam de fora. De `cod_contrato` para `cod_empreendimento` não há perda.
--
-- O que ela NÃO responde: a NATUREZA. A origem da linha financiada só tem
-- `vlrcontrapartidaparceria`, um valor único e sem qualificação. Não existe aqui o
-- equivalente ao que o FAR tem em `HIS_MCIDADES_CONSOLIDADO` — `ic_terreno_doado`,
-- `vr_contrapartida_estado_terreno`, `vr_contrapartida_municipio_infra` e a
-- decomposição por ente e por tipo. Varridas as tabelas `MCidades_*` do pacote
-- semanal, o único campo de contrapartida é o valor.
--
-- Consequência prática, e o motivo de as colunas abaixo existirem nulas em vez de
-- não existirem: contrapartida em TERRENO DOADO ou em INFRAESTRUTURA não movimenta
-- dinheiro na operação de crédito, então entra aqui como ZERO. Somar
-- `vr_contrapartida_total` e concluir que um município não contribuiu é somar certo e
-- afirmar algo falso. As colunas `ic_terreno_doado` e `vr_contrapartida_*_terreno`
-- ficam declaradas e nulas para que a lacuna apareça no catálogo e na ouro, em vez de
-- ser lida como zero.
with
    emenda_por_operacao as (
        select
            cod_operacao,
            count(*) as qt_contratos_pf_emenda,
            count(distinct emenda) as qt_emendas,
            string_agg(distinct emenda, ', ' order by emenda) as emendas,
            sum(vr_contrapartida_parceria) as vr_contrapartida_emenda,
            sum(vr_financiamento_bruto) as vr_financiamento_emenda,
            min(dt_contratacao) as dt_primeira_contratacao_pf,
            max(dt_contratacao) as dt_ultima_contratacao_pf
        from {{ ref("prata_financiado_cidades_emenda") }}
        where cod_operacao is not null
        group by cod_operacao
    ),

    -- O CCA é a outra porta de entrada da contrapartida: contrato de parceria
    -- (`caracteristica` = `003`) que não passa por emenda nenhuma.
    pf_por_operacao as (
        select
            cod_operacao,
            count(*) as qt_contratos_pf,
            count(*) filter (where ic_parceria) as qt_contratos_pf_parceria,
            sum(vr_contrapartida_parceria) as vr_contrapartida_pf,
            sum(vr_financiamento) as vr_financiamento_pf
        from {{ ref("prata_financiado_contrato_pf") }}
        where cod_operacao is not null
        group by cod_operacao
    )

select
    c.cod_contrato,
    c.apf,
    c.cod_empreendimento,
    e.empreendimento_nome,
    e.municipio,
    e.uf,
    e.cod_ibge,
    e.qt_unidades_financiadas,
    c.linha,
    c.tomador_nome,
    c.tomador_cnpj,
    c.vr_contratado,
    c.vr_investimento,
    c.dt_assinatura,

    coalesce(pf.qt_contratos_pf, 0) as qt_contratos_pf,
    coalesce(pf.qt_contratos_pf_parceria, 0) as qt_contratos_pf_parceria,
    coalesce(em.qt_contratos_pf_emenda, 0) as qt_contratos_pf_emenda,
    coalesce(em.qt_emendas, 0) as qt_emendas,
    em.emendas,
    em.dt_primeira_contratacao_pf,
    em.dt_ultima_contratacao_pf,

    coalesce(pf.vr_contrapartida_pf, 0) as vr_contrapartida_cci_cca,
    coalesce(em.vr_contrapartida_emenda, 0) as vr_contrapartida_emenda,

    -- Só o que passou por dinheiro na operação. Ver o bloco de cabeçalho.
    coalesce(pf.vr_contrapartida_pf, 0)
    + coalesce(em.vr_contrapartida_emenda, 0) as vr_contrapartida_total,

    em.cod_operacao is not null as ic_tem_emenda,
    coalesce(pf.vr_contrapartida_pf, 0) + coalesce(em.vr_contrapartida_emenda, 0) > 0
    as ic_tem_contrapartida_financeira,

    -- LACUNA DECLARADA. A origem da financiada não registra a natureza da
    -- contrapartida; estas colunas existem para que a ausência seja visível e para
    -- que a ouro não precise mudar de forma quando o dado aparecer.
    cast(null as boolean) as ic_terreno_doado,
    cast(null as numeric) as vr_contrapartida_estado_terreno,
    cast(null as numeric) as vr_contrapartida_estado_infra,
    cast(null as numeric) as vr_contrapartida_municipio_terreno,
    cast(null as numeric) as vr_contrapartida_municipio_infra,
    cast(null as text) as co_qualificacao_terreno,

    c.arquivo_de_origem,
    c.criado_em
from {{ ref("prata_financiado_contrato_pj") }} as c
left join {{ ref("prata_financiado_empreendimento") }} as e
    on c.cod_empreendimento = e.cod_empreendimento
left join emenda_por_operacao as em on c.cod_contrato = em.cod_operacao
left join pf_por_operacao as pf on c.cod_contrato = pf.cod_operacao
where em.cod_operacao is not null or pf.cod_operacao is not null
