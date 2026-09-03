-- Cruza duas extrações INDEPENDENTES da ABECIP e exige que concordem.
--
-- Item 9 do checklist ("cruzamentos de dados de bases antigas com bases
-- novas"). As duas fontes chegam por caminhos completamente diferentes:
--
--   XLSX de unidades          -> `slv_abecip_financiamentos`
--   relatório mensal (OCR)    -> `slv_abecip_instituicoes`
--
-- Se as duas concordam, é forte indício de que ambas estão certas. Quando
-- divergirem, uma das duas quebrou — e é exatamente isso que queremos saber
-- antes do número chegar no boletim.
--
-- Este tipo de cruzamento não é só defesa: foi um deles que revelou, em
-- 2026-08-29, que o SBPE Const preenchido à mão estava ERRADO (13.115 no
-- 1T2025 contra os 19.130 que o boletim publica).
--
-- Tolerância de 0,5%: as duas fontes arredondam diferente na origem.

with do_xlsx as (
    select
        ano,
        sum(unidades_total)      as unidades,
        sum(valor_total_milhoes) as volume
    from {{ ref('slv_abecip_financiamentos') }}
    group by ano
),

do_relatorio as (
    select
        ano,
        max(unidades_acumuladas_ano)      as unidades,
        max(volume_acumulado_ano_milhoes) as volume
    from {{ ref('slv_abecip_instituicoes') }}
    where modalidade = 'total_aquisicao_construcao'
      and instituicao = 'TOTAL'
    group by ano
),

-- só compara anos em que o relatório cobre o ano inteiro; se a competência
-- for de meio de ano, o acumulado dele não equivale à soma anual do xlsx
comparavel as (
    select r.ano, r.unidades as un_rel, r.volume as vl_rel,
           x.unidades as un_xlsx, x.volume as vl_xlsx
    from do_relatorio r
    join do_xlsx x on x.ano = r.ano
    where r.ano < (select max(ano) from do_relatorio)
)

select ano, un_rel, un_xlsx, vl_rel, vl_xlsx
from comparavel
where abs(un_rel - un_xlsx) > greatest(un_xlsx * 0.005, 1)
   or abs(vl_rel - vl_xlsx) > greatest(vl_xlsx * 0.005, 1)
