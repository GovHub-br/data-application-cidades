{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 3: Novos Financiamentos Imobiliários por Banco (acum. no ano)
-- Seção do impresso: 6. Crédito
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- Tabela única: ABECIP automatizada onde existe, planilha manual no histórico.
--
-- O boletim impresso NÃO lista todas as instituições: nomeia as seis maiores e
-- soma o resto em DEMAIS. Listar BANRISUL, BANPARA, SAFRA, POUPEX e AILOS uma a
-- uma, com 0,1% cada, alonga o quadro sem acrescentar leitura, e afasta a página
-- do impresso. A ordem também é do impresso — fixa, não por volume.
--
-- A coluna `fonte` (`abecip_automatizado` / manual) é procedência de ingestão,
-- não conteúdo do boletim: fica fora do quadro. Quem precisa dela consulta a
-- silver.

with
edicoes as (
    select
        (extract(quarter from t)::int::text || 'T'
         || extract(year from t)::int::text)                as edicao,
        extract(year from t)::int                           as ano_ed,
        extract(quarter from t)::int                        as tri_ed
    from generate_series(
        make_date(2025, 1, 1),
        date_trunc('quarter', current_date)::date,
        interval '3 months'
    ) as t
),

ref as (select edicao, ano_ed, tri_ed * 3 as mes_ed from edicoes),

-- Nome e posição vêm do impresso. Instituição fora da lista cai em DEMAIS.
rotulos as (
    select * from (values
        ('TOTAL',           'TOTAL',     0),
        ('CAIXA',           'CEF',       1),
        ('ITAU UNIBANCO',   'ITAÚ',      2),
        ('BRADESCO',        'BRADESCO',  3),
        ('SANTANDER',       'SANTANDER', 4),
        ('BRB',             'BRB',       5),
        ('BANCO DO BRASIL', 'BB',        6)
    ) as r(origem, rotulo, ordem)
),

bruto as (
    select
        r.edicao,
        coalesce(x.rotulo, 'DEMAIS')  as banco,
        coalesce(x.ordem, 7)          as ordem,
        g.uh_acumulado_ano            as uh,
        g.volume_acumulado_ano_milhoes as volume
    from ref r
    join {{ ref('gld_financiamentos_instituicao') }} g
      on g.ano = r.ano_ed and g.mes = r.mes_ed
    left join rotulos x on x.origem = g.instituicao
),

-- O TOTAL da fonte é a base das participações; some do somatório de DEMAIS.
totais as (
    select edicao, sum(uh) as uh_total, sum(volume) as volume_total
    from bruto where banco = 'TOTAL' group by edicao
),

agregado as (
    select b.edicao, b.banco, b.ordem,
           sum(b.uh) as uh, sum(b.volume) as volume
    from bruto b group by b.edicao, b.banco, b.ordem
)

select
    a.edicao                                                    as "edicao",
    a.banco                                                     as "banco",
    round((a.volume / 1000)::numeric, 1)                        as "VALOR (R$ bi)",
    round((100.0 * a.volume / nullif(t.volume_total, 0))::numeric, 1) as "% valor",
    a.uh                                                        as "UH",
    round((100.0 * a.uh / nullif(t.uh_total, 0))::numeric, 1)   as "% UH"
from agregado a
join totais t on t.edicao = a.edicao
order by a.edicao, a.ordem
