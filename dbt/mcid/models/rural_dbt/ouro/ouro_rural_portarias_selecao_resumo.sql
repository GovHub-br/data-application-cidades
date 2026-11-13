{{ config(materialized="table") }}

-- Gold: resumo por portaria do ciclo de seleção do Rural — abertura, seleção e autorização de
-- contratação. Mostra todas as portarias do catálogo, inclusive as que ainda não têm anexo carregado,
-- com quantas propostas cada uma lista e quantas viraram contrato. Grão: portaria.
--
-- Portaria de seleção: propostas = linhas do anexo de seleção; `qt_autorizadas` = quantas delas estão
-- numa autorização carregada.
-- Portaria de autorização: propostas = linhas do anexo de autorização; o contrato vem do funil, pela
-- proposta (a autorização não traz APF).
-- `uh_contratadas` soma as UH dos contratos vinculados; se duas propostas caírem no mesmo contrato
-- (qt_selecoes_mesmo_contrato > 1), as UH desse contrato entram duas vezes.

with
    cat as (select * from {{ ref("prata_rural_portarias_selecao") }}),
    f as (select * from {{ ref("prata_rural_proposta_funil") }}),
    au as (select * from {{ ref("prata_rural_autorizacao_proposta") }}),

    por_selecao as (
        select
            f.portaria_codigo,
            count(*) as qt_propostas,
            sum(f.quantidade_uh_selecionadas) as uh_propostas,
            count(distinct f.eo_cnpj_selecionada) as qt_eo,
            count(*) filter (where f.ic_autorizada) as qt_autorizadas,
            count(*) filter (where f.ic_contratado) as qt_contratadas,
            sum(f.uh_contratadas) filter (where f.ic_contratado) as uh_contratadas,
            count(*) filter (where f.confianca_vinculo = 'Exata') as qt_vinculo_exato,
            count(*) filter (where f.confianca_vinculo = 'Alta') as qt_vinculo_alta,
            count(*) filter (where f.confianca_vinculo = 'Média') as qt_vinculo_media,
            count(*) filter (where f.confianca_vinculo = 'Baixa') as qt_vinculo_baixa,
            count(*) filter (where f.ic_mesma_eo is false) as qt_eo_diferente,
            round(avg(f.dias_selecao_ate_contrato) filter (where f.ic_contratado)) as dias_medios_ate_contrato
        from f
        group by f.portaria_codigo
    ),

    por_autorizacao as (
        select
            a.portaria_codigo,
            count(*) as qt_propostas,
            sum(a.quantidade_uh_autorizadas) as uh_propostas,
            count(distinct a.eo_cnpj) as qt_eo,
            count(*) as qt_autorizadas,
            count(*) filter (where f.ic_contratado) as qt_contratadas,
            sum(f.uh_contratadas) filter (where f.ic_contratado) as uh_contratadas,
            count(*) filter (where f.confianca_vinculo = 'Exata') as qt_vinculo_exato,
            count(*) filter (where f.confianca_vinculo = 'Alta') as qt_vinculo_alta,
            count(*) filter (where f.confianca_vinculo = 'Média') as qt_vinculo_media,
            count(*) filter (where f.confianca_vinculo = 'Baixa') as qt_vinculo_baixa,
            count(*) filter (where f.ic_mesma_eo is false) as qt_eo_diferente,
            round(avg(f.dias_autorizacao_ate_contrato) filter (where f.ic_contratado)) as dias_medios_ate_contrato
        from au a
        left join f
            on f.protocolo_proposta = a.protocolo_proposta
            and f.portaria_autorizacao_codigo = a.portaria_codigo
        group by a.portaria_codigo
    ),

    medidas as (
        select * from por_selecao
        union all
        select * from por_autorizacao
    )

select
    c.portaria_codigo,
    replace(c.portaria_codigo, '_', '/') as portaria,
    c.ato_selecao,
    c.numero_portaria,
    c.ano_portaria,
    c.dt_portaria,
    c.papel_portaria,
    c.tipo_portaria,
    c.ciclo_selecao,
    c.tem_lista_nominal,
    c.fonte_lista,
    case
        when c.tem_lista_nominal = 'não' then 'Não lista propostas'
        when m.portaria_codigo is null then 'Anexo não carregado'
        else 'Anexo carregado'
    end as situacao_anexo,
    coalesce(m.qt_propostas, 0) as qt_propostas,
    m.uh_propostas,
    coalesce(m.qt_eo, 0) as qt_eo,
    coalesce(m.qt_autorizadas, 0) as qt_autorizadas,
    coalesce(m.qt_contratadas, 0) as qt_contratadas,
    round(100.0 * m.qt_contratadas / nullif(m.qt_propostas, 0), 1) as pct_contratadas,
    m.uh_contratadas,
    coalesce(m.qt_vinculo_exato, 0) as qt_vinculo_exato,
    coalesce(m.qt_vinculo_alta, 0) as qt_vinculo_alta,
    coalesce(m.qt_vinculo_media, 0) as qt_vinculo_media,
    coalesce(m.qt_vinculo_baixa, 0) as qt_vinculo_baixa,
    coalesce(m.qt_eo_diferente, 0) as qt_eo_diferente,
    m.dias_medios_ate_contrato,
    c.observacao
from cat c
left join medidas m on m.portaria_codigo = c.portaria_codigo
