-- CBIC — Indicadores Imobiliários Nacionais 3T2026 (CBIC/Brain, coletiva de 23/11/2026,
-- arquivo IND_PPT_3T2026_vAP23nov_10h.pdf enviado pelo Lucas em 26/11/2026), Página 1 do
-- boletim, em bronze.bronze_cbic_lancamentos_vendas. Mesmo padrão do 0003.
--
-- PARTE 1: o relatório revisa a série de 3T2024 a 2T2026 (págs. 6 e 13: lançamentos e
-- vendas por região; págs. 34 e 38: MCMV × demais por trimestre). Atualiza total, MCMV,
-- demais, regiões e % MCMV Brasil; NÃO mexe na quebra MCMV × região desses trimestres (o
-- relatório só a traz para o trimestre corrente).
-- PARTE 2: insere o 3T2026 completo, com a quebra MCMV × região (págs. 36 e 40).
--
-- As séries MCMV e a quebra por região vêm de gráficos; cada número lido foi conferido:
-- a variação trimestral de cada MCMV bate com a impressa sob o gráfico (págs. 34/38), as
-- regiões MCMV somam o total MCMV (70.129 / 56.812) e cada % regional = MCMV ÷ total da
-- região. Os % MCMV Brasil são os impressos no gráfico (págs. 34/38).
-- Idempotente: as atualizações reescrevem os mesmos valores e a linha 3T2026 é refeita.

BEGIN;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 110188, cbic_lancamentos_mcmv = 54327, cbic_lancamentos_demais = 55861,
    cbic_lancamentos_regiao_norte = 3059, cbic_lancamentos_regiao_nordeste = 21347, cbic_lancamentos_regiao_centro_oeste = 8639, cbic_lancamentos_regiao_sudeste = 53402, cbic_lancamentos_regiao_sul = 23741,
    cbic_lancamentos_mcmv_perc_total_brasil = 50,
    cbic_vendas_total = 107907, cbic_vendas_mcmv = 46818, cbic_vendas_demais = 61089,
    cbic_vendas_regiao_norte = 2603, cbic_vendas_regiao_nordeste = 19365, cbic_vendas_regiao_centro_oeste = 7165, cbic_vendas_regiao_sudeste = 56084, cbic_vendas_regiao_sul = 22690,
    cbic_vendas_mcmv_perc_total_brasil = 44
WHERE ano = 2024 AND trimestre = 3;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 128981, cbic_lancamentos_mcmv = 59349, cbic_lancamentos_demais = 69632,
    cbic_lancamentos_regiao_norte = 1920, cbic_lancamentos_regiao_nordeste = 23658, cbic_lancamentos_regiao_centro_oeste = 7120, cbic_lancamentos_regiao_sudeste = 69382, cbic_lancamentos_regiao_sul = 26901,
    cbic_lancamentos_mcmv_perc_total_brasil = 47,
    cbic_vendas_total = 108287, cbic_vendas_mcmv = 47641, cbic_vendas_demais = 60646,
    cbic_vendas_regiao_norte = 3579, cbic_vendas_regiao_nordeste = 20169, cbic_vendas_regiao_centro_oeste = 6828, cbic_vendas_regiao_sudeste = 54593, cbic_vendas_regiao_sul = 23118,
    cbic_vendas_mcmv_perc_total_brasil = 44
WHERE ano = 2024 AND trimestre = 4;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 103752, cbic_lancamentos_mcmv = 54673, cbic_lancamentos_demais = 49079,
    cbic_lancamentos_regiao_norte = 3110, cbic_lancamentos_regiao_nordeste = 21944, cbic_lancamentos_regiao_centro_oeste = 4684, cbic_lancamentos_regiao_sudeste = 51314, cbic_lancamentos_regiao_sul = 22700,
    cbic_lancamentos_mcmv_perc_total_brasil = 53,
    cbic_vendas_total = 106777, cbic_vendas_mcmv = 49767, cbic_vendas_demais = 57010,
    cbic_vendas_regiao_norte = 3134, cbic_vendas_regiao_nordeste = 21832, cbic_vendas_regiao_centro_oeste = 5517, cbic_vendas_regiao_sudeste = 53417, cbic_vendas_regiao_sul = 22877,
    cbic_vendas_mcmv_perc_total_brasil = 47
WHERE ano = 2025 AND trimestre = 1;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 109314, cbic_lancamentos_mcmv = 51356, cbic_lancamentos_demais = 57958,
    cbic_lancamentos_regiao_norte = 4200, cbic_lancamentos_regiao_nordeste = 17151, cbic_lancamentos_regiao_centro_oeste = 5056, cbic_lancamentos_regiao_sudeste = 59422, cbic_lancamentos_regiao_sul = 23485,
    cbic_lancamentos_mcmv_perc_total_brasil = 47,
    cbic_vendas_total = 107314, cbic_vendas_mcmv = 48706, cbic_vendas_demais = 58608,
    cbic_vendas_regiao_norte = 3196, cbic_vendas_regiao_nordeste = 20424, cbic_vendas_regiao_centro_oeste = 5941, cbic_vendas_regiao_sudeste = 56168, cbic_vendas_regiao_sul = 21585,
    cbic_vendas_mcmv_perc_total_brasil = 46
WHERE ano = 2025 AND trimestre = 2;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 118603, cbic_lancamentos_mcmv = 54719, cbic_lancamentos_demais = 63884,
    cbic_lancamentos_regiao_norte = 2905, cbic_lancamentos_regiao_nordeste = 19677, cbic_lancamentos_regiao_centro_oeste = 7785, cbic_lancamentos_regiao_sudeste = 61040, cbic_lancamentos_regiao_sul = 27196,
    cbic_lancamentos_mcmv_perc_total_brasil = 47,
    cbic_vendas_total = 105637, cbic_vendas_mcmv = 46701, cbic_vendas_demais = 58936,
    cbic_vendas_regiao_norte = 3595, cbic_vendas_regiao_nordeste = 20642, cbic_vendas_regiao_centro_oeste = 6093, cbic_vendas_regiao_sudeste = 51520, cbic_vendas_regiao_sul = 23787,
    cbic_vendas_mcmv_perc_total_brasil = 44
WHERE ano = 2025 AND trimestre = 3;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 150131, cbic_lancamentos_mcmv = 76727, cbic_lancamentos_demais = 73404,
    cbic_lancamentos_regiao_norte = 3266, cbic_lancamentos_regiao_nordeste = 25735, cbic_lancamentos_regiao_centro_oeste = 9347, cbic_lancamentos_regiao_sudeste = 81876, cbic_lancamentos_regiao_sul = 29907,
    cbic_lancamentos_mcmv_perc_total_brasil = 51,
    cbic_vendas_total = 114657, cbic_vendas_mcmv = 55179, cbic_vendas_demais = 59478,
    cbic_vendas_regiao_norte = 3060, cbic_vendas_regiao_nordeste = 21172, cbic_vendas_regiao_centro_oeste = 6621, cbic_vendas_regiao_sudeste = 59824, cbic_vendas_regiao_sul = 23980,
    cbic_vendas_mcmv_perc_total_brasil = 49
WHERE ano = 2025 AND trimestre = 4;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 106345, cbic_lancamentos_mcmv = 52741, cbic_lancamentos_demais = 53604,
    cbic_lancamentos_regiao_norte = 3138, cbic_lancamentos_regiao_nordeste = 23093, cbic_lancamentos_regiao_centro_oeste = 8054, cbic_lancamentos_regiao_sudeste = 51833, cbic_lancamentos_regiao_sul = 20227,
    cbic_lancamentos_mcmv_perc_total_brasil = 50,
    cbic_vendas_total = 113022, cbic_vendas_mcmv = 55606, cbic_vendas_demais = 57416,
    cbic_vendas_regiao_norte = 3470, cbic_vendas_regiao_nordeste = 22994, cbic_vendas_regiao_centro_oeste = 7526, cbic_vendas_regiao_sudeste = 56571, cbic_vendas_regiao_sul = 22461,
    cbic_vendas_mcmv_perc_total_brasil = 49
WHERE ano = 2026 AND trimestre = 1;

UPDATE bronze.bronze_cbic_lancamentos_vendas SET
    cbic_lancamentos_total = 114621, cbic_lancamentos_mcmv = 66242, cbic_lancamentos_demais = 48379,
    cbic_lancamentos_regiao_norte = 3392, cbic_lancamentos_regiao_nordeste = 24019, cbic_lancamentos_regiao_centro_oeste = 6585, cbic_lancamentos_regiao_sudeste = 61696, cbic_lancamentos_regiao_sul = 18929,
    cbic_lancamentos_mcmv_perc_total_brasil = 58,
    cbic_vendas_total = 113797, cbic_vendas_mcmv = 58388, cbic_vendas_demais = 55409,
    cbic_vendas_regiao_norte = 3788, cbic_vendas_regiao_nordeste = 23497, cbic_vendas_regiao_centro_oeste = 7931, cbic_vendas_regiao_sudeste = 56407, cbic_vendas_regiao_sul = 22174,
    cbic_vendas_mcmv_perc_total_brasil = 51
WHERE ano = 2026 AND trimestre = 2;

DELETE FROM bronze.bronze_cbic_lancamentos_vendas WHERE ano = 2026 AND trimestre = 3;

INSERT INTO bronze.bronze_cbic_lancamentos_vendas (
    ano,
    trimestre,
    cbic_lancamentos_total,
    cbic_lancamentos_mcmv,
    cbic_lancamentos_demais,
    cbic_lancamentos_regiao_norte,
    cbic_lancamentos_regiao_nordeste,
    cbic_lancamentos_regiao_centro_oeste,
    cbic_lancamentos_regiao_sudeste,
    cbic_lancamentos_regiao_sul,
    cbic_lancamentos_mcmv_regiao_norte,
    cbic_lancamentos_mcmv_regiao_nordeste,
    cbic_lancamentos_mcmv_regiao_centro_oeste,
    cbic_lancamentos_mcmv_regiao_sudeste,
    cbic_lancamentos_mcmv_regiao_sul,
    cbic_lancamentos_mcmv_perc_regiao_norte,
    cbic_lancamentos_mcmv_perc_regiao_nordeste,
    cbic_lancamentos_mcmv_perc_regiao_centro_oeste,
    cbic_lancamentos_mcmv_perc_regiao_sudeste,
    cbic_lancamentos_mcmv_perc_regiao_sul,
    cbic_lancamentos_mcmv_perc_total_brasil,
    cbic_vendas_total,
    cbic_vendas_mcmv,
    cbic_vendas_demais,
    cbic_vendas_regiao_norte,
    cbic_vendas_regiao_nordeste,
    cbic_vendas_regiao_centro_oeste,
    cbic_vendas_regiao_sudeste,
    cbic_vendas_regiao_sul,
    cbic_vendas_mcmv_regiao_norte,
    cbic_vendas_mcmv_regiao_nordeste,
    cbic_vendas_mcmv_regiao_centro_oeste,
    cbic_vendas_mcmv_regiao_sudeste,
    cbic_vendas_mcmv_regiao_sul,
    cbic_vendas_mcmv_perc_regiao_norte,
    cbic_vendas_mcmv_perc_regiao_nordeste,
    cbic_vendas_mcmv_perc_regiao_centro_oeste,
    cbic_vendas_mcmv_perc_regiao_sudeste,
    cbic_vendas_mcmv_perc_regiao_sul,
    cbic_vendas_mcmv_perc_total_brasil
) VALUES (
    2026, 3, 119774, 70129, 49645, 3154, 24056, 7726, 65434, 19404, 1504, 14983, 3989, 43466, 6187, 48, 62, 52, 66, 32, 59, 108464, 56812, 51652, 3289, 22286, 7267, 55689, 19933, 2242, 12436, 3567, 33455, 5112, 68, 56, 49, 60, 26, 52
);

COMMIT;
