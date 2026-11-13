{{ config(materialized="table") }}

-- Gold: quais contratos do Pró-Moradia fogem da curva S de referência. Para cada contrato, a medição mais recente
-- do Canal FGTS comparada com a distribuição de todos os contratos na MESMA idade (ouro_pro_moradia_curva_s_referencia):
-- desvio da mediana (p.p.), desvio do próprio cronograma (p.p.) e faixa. Grão: contrato. Lê só prata.
--   Muito abaixo da curva  ≤ p10 da idade ou 20 p.p. ou mais abaixo da mediana;
--   Abaixo da curva        < p25;  Na curva  p25 a p75;  Acima da curva  > p75;
--   Sem referência         idade com menos de 10 medições.
-- ic_obra_em_aberto separa as obras que ainda importam (contrato vigente e físico < 100%).
-- situacao_acompanhamento junta as duas coisas para a gestão:
--   Atrasada                      obra em aberto, medição nos últimos 12 meses e abaixo da curva (< p25 da idade);
--   Sem medição há 12+ meses      obra em aberto cuja última medição tem 12 meses ou mais (provável paralisação
--                                 ou contrato que deixou de reportar; não dá para dizer se está atrasada);
--   Em dia                        obra em aberto, medição recente, na curva ou acima;
--   Sem referência                obra em aberto, medição recente, idade com menos de 10 medições;
--   Concluída ou encerrada        contrato não vigente ou físico = 100%.
-- A data de corte é o mês mais recente de medição na base (dt_referencia_base).

with
    base as (select * from ({{ pro_moradia_curva_s_obra_mes() }}) b),
    ref as (select * from ({{ curva_s_referencia_de(pro_moradia_curva_s_obra_mes()) }}) r),
    ultimo as (select distinct on (chave) * from base order by chave, mes desc),
    corte as (select max(mes) as dt_referencia_base from base),
    c as (select * from {{ ref("prata_pro_moradia_contrato") }})

select
    u.chave as cod_contrato,
    c.contrato,
    c.empreendimento_nome,
    c.tomador_nome,
    c.tomador_esfera,
    c.uf,
    c.municipio,
    c.modalidade,
    c.tipo_intervencao,
    c.situacao_contrato,
    c.ic_contrato_vigente,
    c.dt_assinatura,
    extract(year from c.dt_assinatura)::int as ano_assinatura,
    c.valor_contratado,
    c.quantidade_uh_financiadas,
    c.identificador_selecao,
    u.mes as dt_ultima_medicao,
    u.idade_meses,
    u.pct_fisico_realizado,
    u.pct_fisico_previsto,
    u.pct_financeiro,
    u.situacao as situacao_obra,
    r.qt_contratos as qt_contratos_na_idade,
    r.fis_p10, r.fis_p25, r.fis_mediana, r.fis_p75, r.fis_p90,
    r.fin_mediana,
    round(u.pct_fisico_realizado - r.fis_mediana, 1) as desvio_mediana_pp,
    round(u.pct_fisico_realizado - u.pct_fisico_previsto, 1) as desvio_cronograma_pp,
    round(u.pct_financeiro - u.pct_fisico_realizado, 1) as diferenca_financeiro_fisico_pp,
    case
        when r.idade_meses is null or not r.ic_referencia_valida then 'Sem referência'
        when u.pct_fisico_realizado <= r.fis_p10 or u.pct_fisico_realizado - r.fis_mediana <= -20 then 'Muito abaixo da curva'
        when u.pct_fisico_realizado < r.fis_p25 then 'Abaixo da curva'
        when u.pct_fisico_realizado > r.fis_p75 then 'Acima da curva'
        else 'Na curva'
    end as faixa_curva_fisica,
    (coalesce(c.ic_contrato_vigente, false) and u.pct_fisico_realizado < 100) as ic_obra_em_aberto,
    k.dt_referencia_base,
    (
        (extract(year from k.dt_referencia_base)::int - extract(year from u.mes)::int) * 12
        + extract(month from k.dt_referencia_base)::int - extract(month from u.mes)::int
    ) as meses_sem_medicao,
    case
        when not (coalesce(c.ic_contrato_vigente, false) and u.pct_fisico_realizado < 100) then 'Concluída ou encerrada'
        when u.mes <= (k.dt_referencia_base - interval '12 months')::date then 'Sem medição há 12+ meses'
        when r.idade_meses is null or not r.ic_referencia_valida then 'Sem referência'
        when u.pct_fisico_realizado <= r.fis_p10 or u.pct_fisico_realizado - r.fis_mediana <= -20
             or u.pct_fisico_realizado < r.fis_p25 then 'Atrasada'
        else 'Em dia'
    end as situacao_acompanhamento
from ultimo u
cross join corte k
join c on c.cod_contrato = u.chave
left join ref r on r.idade_meses = u.idade_meses
