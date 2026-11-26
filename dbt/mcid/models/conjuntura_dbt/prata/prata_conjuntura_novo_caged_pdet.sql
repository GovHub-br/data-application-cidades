{{ config(materialized='table') }}

-- Novo CAGED por competência e recorte (edifícios = divisão CNAE 41, serviços
-- especializados = 43, total = seção F), a partir dos microdados do PDET.
--
-- O saldo de uma competência no painel do MTE é: movimentações do mês (MOV) + declarações
-- fora do prazo que chegam depois com aquela competência (FOR) − exclusões (EXC).
-- Conferido em 26/11/2026 contra o painel: mai/26 total 12.677 e jun/26 14.136, exatos.
-- Cada linha é uma movimentação, com `saldomovimentacao` = 1 (admissão) ou -1 (desligamento).
with
    arquivos as (
        -- o mesmo arquivo pousado em duas cargas conta uma vez: fica a mais recente
        select _source_file, max(_load_id) as _load_id
        from {{ ref('bronze_novo_caged_pdet') }}
        group by 1
    ),

    movimentos as (
        select
            b.competenciamov::int as competencia,
            left(b.subclasse::text, 2) as divisao,
            b.saldomovimentacao::int as movimento,
            case when b._source_file like 'CAGEDEXC%' then -1 else 1 end as sinal
        from {{ ref('bronze_novo_caged_pdet') }} b
        join arquivos a on a._source_file = b._source_file and a._load_id = b._load_id
        where b.secao = 'F' and b.competenciamov::int >= 202401
    ),

    recortes as (
        select competencia, 'total' as recorte, movimento, sinal
        from movimentos
        union all
        select competencia, 'edificios', movimento, sinal
        from movimentos
        where divisao = '41'
        union all
        select competencia, 'servicos_especializados', movimento, sinal
        from movimentos
        where divisao = '43'
    )

select
    make_date(floor(competencia / 100)::int, (competencia % 100)::int, 1) as data_referencia,
    recorte,
    sum(case when movimento = 1 then sinal else 0 end) as admitidos,
    sum(case when movimento = -1 then sinal else 0 end) as desligados,
    sum(movimento * sinal) as saldo
from recortes
group by 1, 2
