-- Confere Total = Construção + Aquisição nas unidades e nos valores da ABECIP.
--
-- A prata lê as colunas da aba `BD_Unidades` por POSIÇÃO (o cabeçalho é mesclado
-- e repete `Construção | Aquisição | Total` para unidades e para valores). Se a
-- ABECIP inserir ou reordenar uma coluna, a série errada segue sem erro nenhum; a
-- identidade amarra a posição ao significado. Tolerância relativa de 0,1% + 1
-- (os valores em R$ vêm arredondados na origem). Veio do `ClienteAbecip`, que
-- fazia a mesma conferência na ingestão antiga.

select data_referencia, 'unidades' as metrica,
       unidades_total as total, unidades_construcao + unidades_aquisicao as soma
from {{ ref('prata_conjuntura_abecip_financiamentos') }}
where abs(unidades_total - (unidades_construcao + unidades_aquisicao))
      > abs(unidades_total) * 0.001 + 1
union all
select data_referencia, 'valor',
       valor_total_milhoes, valor_construcao_milhoes + valor_aquisicao_milhoes
from {{ ref('prata_conjuntura_abecip_financiamentos') }}
where abs(valor_total_milhoes - (valor_construcao_milhoes + valor_aquisicao_milhoes))
      > abs(valor_total_milhoes) * 0.001 + 1
