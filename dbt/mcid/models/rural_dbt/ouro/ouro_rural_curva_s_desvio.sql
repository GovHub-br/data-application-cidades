{{ config(materialized="table") }}

-- Gold: quais empreendimentos do Novo MCMV Rural fogem da curva S de referência.
-- Para cada APF, o retrato mais recente do MONIT_MOV_OBRA: tempo de contrato, % físico realizado e previsto
-- (cronograma do próprio contrato) e % financeiro liberado, comparados com a curva de referência NA MESMA
-- IDADE (mediana e quartis de todos os empreendimentos; ver ouro_rural_curva_s_referencia).
--
-- faixa_curva_fisica:
--   Muito abaixo da curva   realizado ≤ p10 da idade ou 20 p.p. ou mais abaixo da mediana;
--   Abaixo da curva         realizado < p25 da idade;
--   Na curva                entre p25 e p75;
--   Acima da curva          realizado > p75;
--   Sem referência          idade com poucos retratos (menos de 10) para comparar.
-- desvio_mediana_pp = realizado − mediana da idade (negativo = atrás dos demais).
-- desvio_cronograma_pp = realizado − previsto do próprio contrato.
--
-- Grão: APF × tipo_portaria (3 linhas por APF, como a ouro_rural_painel_portaria), para o filtro único de
-- portaria do painel funcionar. Filtre UM tipo de portaria antes de contar. Lê só prata.

with
    base as (select * from ({{ rural_curva_s_apf_mes() }}) b),
    ref as (select * from ({{ rural_curva_s_referencia() }}) r),
    ultimo as (
        select distinct on (apf) *
        from base
        order by apf, mes desc
    ),
    op as (select * from {{ ref("prata_rural_vinculo_operacao") }}),

    desvio as (
        select
            u.apf,
            u.mes as dt_ultimo_retrato,
            u.idade_meses,
            u.pct_fisico_realizado,
            u.pct_fisico_previsto,
            u.pct_financeiro,
            u.situacao_operacao,
            u.estagio_obra,
            r.qt_empreendimentos as qt_empreendimentos_na_idade,
            r.fis_p10, r.fis_p25, r.fis_mediana, r.fis_p75, r.fis_p90,
            r.fin_mediana,
            round(u.pct_fisico_realizado - r.fis_mediana, 1) as desvio_mediana_pp,
            round(u.pct_fisico_realizado - u.pct_fisico_previsto, 1) as desvio_cronograma_pp,
            round(u.pct_financeiro - u.pct_fisico_realizado, 1) as diferenca_financeiro_fisico_pp,
            case
                when r.idade_meses is null or not r.ic_referencia_valida then 'Sem referência'
                when u.pct_fisico_realizado <= r.fis_p10 or u.pct_fisico_realizado - r.fis_mediana <= -20
                    then 'Muito abaixo da curva'
                when u.pct_fisico_realizado < r.fis_p25 then 'Abaixo da curva'
                when u.pct_fisico_realizado > r.fis_p75 then 'Acima da curva'
                else 'Na curva'
            end as faixa_curva_fisica
        from ultimo u
        left join ref r on r.idade_meses = u.idade_meses
    )

select
    md5(d.apf || '|' || tp.tipo_portaria) as id_apf_tipo_portaria,
    tp.tipo_portaria,
    tp.portaria,
    d.*,
    case d.faixa_curva_fisica
        when 'Muito abaixo da curva' then 1 when 'Abaixo da curva' then 2 when 'Na curva' then 3
        when 'Acima da curva' then 4 else 5
    end as ordem_faixa,
    op.nome_empreendimento,
    op.municipio,
    op.uf,
    op.regiao,
    op.agente_financeiro,
    op.programa,
    op.eo_nome,
    op.eo_cnpj,
    op.dt_contratacao,
    op.ano_contratacao,
    op.quantidade_uh,
    op.valor_contratado,
    op.status_execucao,
    op.portaria_selecao,
    op.portaria_autorizacao,
    op.portaria_ts
from desvio d
join op on op.apf = d.apf
{{ rural_portaria_por_tipo("op") }}
