-- Corrige três variações da Tenda em 1T2026.
-- Fonte: Boletim de Conjuntura 2026 1T Final, página 2, tabela
-- "Balanços das empresas". A variação de lançamentos contra o mesmo trimestre
-- estava em 56,7% em vez de 67%; os dois campos de 12 meses repetiam a
-- variação anual (56,7% e 30,0%) em vez de 0% e 9%.
--
-- A tabela manual é a fonte controlada dessas variações divulgadas; não
-- derivar a métrica de séries de unidades, pois o recorte consolidado da
-- companhia pode incluir negócios ausentes da série de lançamentos.

update manual_conjuntura.dados_trimestrais
set
    tenda_var_lancamentos_mesmo_tri_ano_anterior = '0.67',
    tenda_var_lancamento_acumulado_mesmo_periodo_ano_anterior = '0',
    tenda_var_vendas_acumulado_mesmo_periodo_ano_anterior = '0.09'
where periodo = '1T2026';

-- Deve afetar exatamente a linha da edição. Se não afetar uma linha, a
-- migração não deve ser considerada aplicada: conferir antes de executar.
