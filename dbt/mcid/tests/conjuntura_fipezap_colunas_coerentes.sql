-- Confere que número-índice e variações do FipeZap contam a mesma história.
--
-- A prata escolhe as colunas de locação por POSIÇÃO na planilha (o cabeçalho é
-- mesclado e se repete: `Total`, `Total_2`…). O pior modo de falha desta fonte é
-- a FIPE inserir ou reordenar uma coluna: a ingestão continua rodando, os valores
-- continuam parecendo percentuais plausíveis, e a série errada vai para o boletim
-- sem erro nenhum. A defesa é semântica, não posicional:
--
--     indice[t] / indice[t-1]  - 1  =  var_mensal[t]
--     indice[t] / indice[t-12] - 1  =  var_ano[t]
--
-- Tolerância de 0,15 p.p. (a FIPE arredonda e revisa a série), e só falha com
-- desacordo generalizado (mais de 20% dos meses): revisão pontual não derruba o
-- build, coluna trocada desalinha quase tudo. Veio do `ClienteFipeZap`, que fazia
-- a mesma conferência na ingestão antiga.

with serie as (
    select
        data_referencia,
        imoveis_residenciais_locacao_var_mensal_total as var_mensal,
        imoveis_residenciais_locacao_var_ano_total    as var_ano,
        imoveis_residenciais_locacao_numero_indice_total
            / lag(imoveis_residenciais_locacao_numero_indice_total, 1)
              over (order by data_referencia) - 1     as esperado_mensal,
        imoveis_residenciais_locacao_numero_indice_total
            / lag(imoveis_residenciais_locacao_numero_indice_total, 12)
              over (order by data_referencia) - 1     as esperado_ano
    from {{ ref('prata_conjuntura_fipezap_locacao') }}
),

conferencia as (
    select
        'mensal' as variacao,
        count(*) as comparaveis,
        count(*) filter (where abs(esperado_mensal - var_mensal) > 0.0015) as fora,
        13 as minimo
    from serie
    where esperado_mensal is not null and var_mensal is not null
    union all
    select
        '12 meses',
        count(*),
        count(*) filter (where abs(esperado_ano - var_ano) > 0.0015),
        24
    from serie
    where esperado_ano is not null and var_ano is not null
)

select variacao, comparaveis, fora
from conferencia
where comparaveis >= minimo
  and fora::numeric / comparaveis > 0.20
