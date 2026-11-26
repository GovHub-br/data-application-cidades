{{ config(materialized='table') }}

-- Novo CAGED por competência e recorte (edifícios = divisão CNAE 41, serviços
-- especializados = 43, total = seção F), a partir dos microdados do PDET.
--
-- O saldo de uma competência no painel do MTE é: movimentações do mês (MOV) + declarações
-- fora do prazo que chegam depois com aquela competência (FOR) − exclusões (EXC).
-- Conferido em 26/11/2026 contra o painel: mai/26 total 12.677 e jun/26 14.136, exatos.
-- Cada linha é uma movimentação, com `saldomovimentacao` = 1 (admissão) ou -1 (desligamento).
-- Uma agregação só sobre as ~14 M linhas, sem join: o pg_duckdb da homolog tem 1,5 GB e
-- disco temporário desligado, e um join ou `union` sobre as linhas estoura a memória
-- ("LocalFileSystem has been disabled"). Cada arquivo do FTP pousa uma vez só (a DAG
-- pula o que já está na raw), então não há carga repetida para descartar aqui.
with
    por_divisao as (
        select
            competenciamov::int as competencia,
            left(subclasse::text, 2) as divisao,
            sum(case when saldomovimentacao::int = 1 then sinal else 0 end) as admitidos,
            sum(case when saldomovimentacao::int = -1 then sinal else 0 end) as desligados,
            sum(saldomovimentacao::int * sinal) as saldo
        from
            (
                select *, case when _source_file like 'CAGEDEXC%' then -1 else 1 end as sinal
                from {{ ref('bronze_novo_caged_pdet') }}
            ) b
        where secao = 'F' and competenciamov::int >= 202401
        group by 1, 2
    ),

    recortes as (
        select competencia, 'total' as recorte, admitidos, desligados, saldo
        from por_divisao
        union all
        select competencia, 'edificios', admitidos, desligados, saldo
        from por_divisao
        where divisao = '41'
        union all
        select competencia, 'servicos_especializados', admitidos, desligados, saldo
        from por_divisao
        where divisao = '43'
    )

select
    make_date(floor(competencia / 100)::int, (competencia % 100)::int, 1) as data_referencia,
    recorte,
    sum(admitidos) as admitidos,
    sum(desligados) as desligados,
    sum(saldo) as saldo
from recortes
group by 1, 2
