-- Atualização manual de conjuntura_bronze.bronze_cbic_lancamentos_vendas a
-- partir do relatório "Indicadores Imobiliários Nacionais" da CBIC/Brain,
-- 2º Trimestre de 2026 (publicado 24/08/2026). CBIC não tem API pública —
-- ver conjuntura-fontes-dbt.md (documentação local, fora do repositório), Página 1.
--
-- Sem PK/unique constraint na tabela: rodar este script mais de uma vez
-- duplica as linhas de INSERT (a parte de UPDATE é idempotente).

-- =========================================================================
-- PARTE 1 — Revisão de 3T2024 a 4T2025.
--
-- O relatório 2T2026 trouxe a série histórica de 2T24 a 2T26 revisada pra
-- cima em TODOS os trimestres desse intervalo (incorporações reportadas
-- com atraso pelas construtoras). Maior diferença: 4T2025 lançamentos
-- total, que estava em 133.811 e passa para 148.513 (+11%).
--
-- Atualiza total/mcmv/demais e os totais por região (lançamentos e
-- vendas). NÃO mexe em cbic_*_mcmv_regiao_* nem em
-- cbic_*_mcmv_perc_regiao_* desses trimestres — o relatório só detalha a
-- quebra MCMV × região para o trimestre mais recente (2T2026), então os
-- valores desses campos para os trimestres antigos permanecem os que já
-- estavam na tabela.
-- =========================================================================

UPDATE conjuntura_bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 109382, cbic_lancamentos_mcmv = 54037, cbic_lancamentos_demais = 55345,
    cbic_lancamentos_regiao_norte = 3059, cbic_lancamentos_regiao_nordeste = 21107,
    cbic_lancamentos_regiao_centro_oeste = 8515, cbic_lancamentos_regiao_sudeste = 53447,
    cbic_lancamentos_regiao_sul = 23254,
    cbic_lancamentos_mcmv_perc_total_brasil = 50,
    cbic_vendas_total = 108046, cbic_vendas_mcmv = 46871, cbic_vendas_demais = 61175,
    cbic_vendas_regiao_norte = 2602, cbic_vendas_regiao_nordeste = 19302,
    cbic_vendas_regiao_centro_oeste = 7186, cbic_vendas_regiao_sudeste = 56203,
    cbic_vendas_regiao_sul = 22753,
    cbic_vendas_mcmv_perc_total_brasil = 44
WHERE ano = 2024 AND trimestre = 3;

UPDATE conjuntura_bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 129180, cbic_lancamentos_mcmv = 59359, cbic_lancamentos_demais = 69821,
    cbic_lancamentos_regiao_norte = 1888, cbic_lancamentos_regiao_nordeste = 23962,
    cbic_lancamentos_regiao_centro_oeste = 7120, cbic_lancamentos_regiao_sudeste = 69119,
    cbic_lancamentos_regiao_sul = 27091,
    cbic_lancamentos_mcmv_perc_total_brasil = 47,
    cbic_vendas_total = 108311, cbic_vendas_mcmv = 47639, cbic_vendas_demais = 60672,
    cbic_vendas_regiao_norte = 3576, cbic_vendas_regiao_nordeste = 20263,
    cbic_vendas_regiao_centro_oeste = 6765, cbic_vendas_regiao_sudeste = 54518,
    cbic_vendas_regiao_sul = 23189,
    cbic_vendas_mcmv_perc_total_brasil = 44
WHERE ano = 2024 AND trimestre = 4;

UPDATE conjuntura_bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 103738, cbic_lancamentos_mcmv = 54600, cbic_lancamentos_demais = 49138,
    cbic_lancamentos_regiao_norte = 3110, cbic_lancamentos_regiao_nordeste = 21561,
    cbic_lancamentos_regiao_centro_oeste = 4916, cbic_lancamentos_regiao_sudeste = 51190,
    cbic_lancamentos_regiao_sul = 22961,
    cbic_lancamentos_mcmv_perc_total_brasil = 53,
    cbic_vendas_total = 106986, cbic_vendas_mcmv = 49809, cbic_vendas_demais = 57177,
    cbic_vendas_regiao_norte = 3126, cbic_vendas_regiao_nordeste = 21795,
    cbic_vendas_regiao_centro_oeste = 5798, cbic_vendas_regiao_sudeste = 53369,
    cbic_vendas_regiao_sul = 22898,
    cbic_vendas_mcmv_perc_total_brasil = 47
WHERE ano = 2025 AND trimestre = 1;

UPDATE conjuntura_bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 108600, cbic_lancamentos_mcmv = 51056, cbic_lancamentos_demais = 57544,
    cbic_lancamentos_regiao_norte = 4200, cbic_lancamentos_regiao_nordeste = 17063,
    cbic_lancamentos_regiao_centro_oeste = 5056, cbic_lancamentos_regiao_sudeste = 59110,
    cbic_lancamentos_regiao_sul = 23171,
    cbic_lancamentos_mcmv_perc_total_brasil = 47,
    cbic_vendas_total = 107924, cbic_vendas_mcmv = 48876, cbic_vendas_demais = 59048,
    cbic_vendas_regiao_norte = 3207, cbic_vendas_regiao_nordeste = 20648,
    cbic_vendas_regiao_centro_oeste = 6091, cbic_vendas_regiao_sudeste = 56042,
    cbic_vendas_regiao_sul = 21936,
    cbic_vendas_mcmv_perc_total_brasil = 46
WHERE ano = 2025 AND trimestre = 2;

UPDATE conjuntura_bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 117193, cbic_lancamentos_mcmv = 54101, cbic_lancamentos_demais = 63092,
    cbic_lancamentos_regiao_norte = 2905, cbic_lancamentos_regiao_nordeste = 19135,
    cbic_lancamentos_regiao_centro_oeste = 7785, cbic_lancamentos_regiao_sudeste = 60498,
    cbic_lancamentos_regiao_sul = 26870,
    cbic_lancamentos_mcmv_perc_total_brasil = 47,
    cbic_vendas_total = 105784, cbic_vendas_mcmv = 46696, cbic_vendas_demais = 59088,
    cbic_vendas_regiao_norte = 3649, cbic_vendas_regiao_nordeste = 20571,
    cbic_vendas_regiao_centro_oeste = 6131, cbic_vendas_regiao_sudeste = 51366,
    cbic_vendas_regiao_sul = 24067,
    cbic_vendas_mcmv_perc_total_brasil = 44
WHERE ano = 2025 AND trimestre = 3;

UPDATE conjuntura_bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 148513, cbic_lancamentos_mcmv = 76009, cbic_lancamentos_demais = 72504,
    cbic_lancamentos_regiao_norte = 3266, cbic_lancamentos_regiao_nordeste = 25557,
    cbic_lancamentos_regiao_centro_oeste = 9394, cbic_lancamentos_regiao_sudeste = 81489,
    cbic_lancamentos_regiao_sul = 28807,
    cbic_lancamentos_mcmv_perc_total_brasil = 52,
    cbic_vendas_total = 114930, cbic_vendas_mcmv = 55255, cbic_vendas_demais = 59675,
    cbic_vendas_regiao_norte = 3059, cbic_vendas_regiao_nordeste = 21214,
    cbic_vendas_regiao_centro_oeste = 6935, cbic_vendas_regiao_sudeste = 59743,
    cbic_vendas_regiao_sul = 23979,
    cbic_vendas_mcmv_perc_total_brasil = 49
WHERE ano = 2025 AND trimestre = 4;

-- =========================================================================
-- PARTE 2 — 1T2026 e 2T2026 (trimestres que faltavam por completo).
--
-- 1T2026: só os totais e as 5 regiões, tanto de lançamentos quanto de
-- vendas (mais o % MCMV nacional) — o relatório 2T2026 não detalha a
-- quebra MCMV × região dos trimestres anteriores ao mais recente, então
-- essas 10 colunas (5 regiões × lançamentos/vendas) ficam NULL até
-- surgir o relatório do 1T2026 avulso, se você tiver.
--
-- 2T2026: linha completa — é o trimestre corrente do relatório, com todo
-- o detalhamento por região.
-- =========================================================================

DELETE FROM conjuntura_bronze.bronze_cbic_lancamentos_vendas
WHERE (ano, trimestre) IN ((2026, 1), (2026, 2));

INSERT INTO conjuntura_bronze.bronze_cbic_lancamentos_vendas (
    ano, trimestre,

    cbic_lancamentos_total, cbic_lancamentos_mcmv, cbic_lancamentos_demais,

    cbic_lancamentos_regiao_norte, cbic_lancamentos_regiao_nordeste,
    cbic_lancamentos_regiao_centro_oeste, cbic_lancamentos_regiao_sudeste,
    cbic_lancamentos_regiao_sul,

    cbic_lancamentos_mcmv_regiao_norte, cbic_lancamentos_mcmv_regiao_nordeste,
    cbic_lancamentos_mcmv_regiao_centro_oeste, cbic_lancamentos_mcmv_regiao_sudeste,
    cbic_lancamentos_mcmv_regiao_sul,

    cbic_lancamentos_mcmv_perc_regiao_norte, cbic_lancamentos_mcmv_perc_regiao_nordeste,
    cbic_lancamentos_mcmv_perc_regiao_centro_oeste, cbic_lancamentos_mcmv_perc_regiao_sudeste,
    cbic_lancamentos_mcmv_perc_regiao_sul, cbic_lancamentos_mcmv_perc_total_brasil,

    cbic_vendas_total, cbic_vendas_mcmv, cbic_vendas_demais,

    cbic_vendas_regiao_norte, cbic_vendas_regiao_nordeste,
    cbic_vendas_regiao_centro_oeste, cbic_vendas_regiao_sudeste,
    cbic_vendas_regiao_sul,

    cbic_vendas_mcmv_regiao_norte, cbic_vendas_mcmv_regiao_nordeste,
    cbic_vendas_mcmv_regiao_centro_oeste, cbic_vendas_mcmv_regiao_sudeste,
    cbic_vendas_mcmv_regiao_sul,

    cbic_vendas_mcmv_perc_regiao_norte, cbic_vendas_mcmv_perc_regiao_nordeste,
    cbic_vendas_mcmv_perc_regiao_centro_oeste, cbic_vendas_mcmv_perc_regiao_sudeste,
    cbic_vendas_mcmv_perc_regiao_sul, cbic_vendas_mcmv_perc_total_brasil
) VALUES
-- 1T2026 — totais/regiões da linha REVISADOS (relatório 2T2026), mas a
-- quebra MCMV x região abaixo veio do boletim próprio (fonte: CBIC),
-- que reflete o relatório ORIGINAL do 1T2026 — antes da revisão pra cima.
-- Ou seja: região_mcmv + "demais" implícito dessa região NÃO fecha
-- exatamente com o total revisado da região (a diferença é a revisão
-- tardia, que não sabemos se caiu em MCMV ou demais padrões). Percentuais
-- de lançamentos vieram prontos do boletim; percentuais de vendas foram
-- calculados aqui (o boletim não trouxe essa coluna pra vendas).
(2026, 1,
    104490, 51829, 52661,
    3126, 21996, 8054, 51653, 19661,
    2608, 12704, 3236, 26877, 3223,
    83, 64, 48, 53, 18, 50,
    112860, 55596, 57264,
    3463, 22970, 7480, 56748, 22199,
    2976, 11540, 2121, 31537, 6717,
    87, 52, 34, 56, 29, 49
),
-- 2T2026 — completo
(2026, 2,
    107161, 62129, 45032,
    2722, 21265, 6020, 59801, 17353,
    1864, 12758, 3136, 39273, 5098,
    68, 60, 52, 66, 29, 58,
    113676, 58392, 55284,
    3687, 23464, 7778, 56772, 21975,
    2463, 13098, 3354, 33159, 6318,
    67, 56, 43, 58, 29, 51
);
