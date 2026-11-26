-- Balanço das empresas e ticket médio de lançamentos do 3T2026 (Páginas 2 e 6 do
-- boletim) em bronze.bronze_manual_dados_trimestrais. Mesmos critérios do 0004
-- (unidades, variações) e do 0005 (ticket médio); a linha 3T2026 não existia.
--
-- Fontes (prévias operacionais do 3T26, baixadas em 26/11/2026 dos sites de RI):
--   Cury        → Prévia Operacional 3T26 e 9M26 (filemanager-cdn.mziq.com/published/
--                 702b9586-4f10-4a79-a7e6-232ce8803136/dc1d8384-e9b1-4272-8db0-1fade2c4f712)
--   Tenda       → Prévia Operacional 3T26, 08/10/2026 (…/36c72d10-4c63-4c06-a420-f7797fcde0bf/
--                 8bdede3e-f088-4fc8-af11-9dc6717520e9)
--   Direcional  → Prévia Operacional 3T26 (…/ada9bc2c-f7d0-4359-9eaf-851b679ab788/
--                 ec557d57-e4bc-4648-bb3f-e272f7f0ba0a)
--   Pacaembu    → Prévia Operacional 3T26 e 9M26 (…/e7eb7558-1a9a-4262-b6a6-1167e239272e/
--                 2ba20002-b445-4894-be2a-3dca18355f74)
--   Plano&Plano → Prévia Operacional 3T26 (…/dd0335cb-6079-40d0-95fc-fbcb3fa580ef/
--                 6fc3d39a-2b3c-4681-b418-8518a70fe5f8)
--   MRV         → planilha interativa do RI (a que a lancamentos/vendas_ingest_dag pousa
--                 em snh/staging/mrv/); a prévia 3T26 não estava listada no site. A planilha
--                 reproduz exato o 2T26 já salvo (10.679 / 10.148).
--
-- Conferência: em todas as seis, o 2T26 impresso na prévia do 3T26 é igual ao 2T2026
-- já salvo (mesma linha/segmento do 0004): MRV 10.679/10.148, Cury 6.549/6.697,
-- Tenda Consolidado 7.099/6.657, Direcional 5.511/4.955, Pacaembu 3.100/3.397,
-- Plano&Plano 3.138/3.601.
--
-- Variações: copiadas da prévia ("% T/T", "% A/A", "9M26 x 9M25"), como no 0004. A MRV
-- não tem prévia publicada: as dela saem da própria planilha oficial (mesma base do
-- release). Totais e suas variações são nossos (soma das seis), como no 0004.
--
-- Ticket médio (0005): MRV = "Preço Médio/Unid." da planilha (283,8 → 284, a coluna é
-- bigint); Cury = "Preço Médio/Unid. (R$ mil)" da prévia, 326,5; Direcional e Tenda não
-- publicam, VGV lançado ÷ unidades lançadas: Direcional 1.352,5 mi / 3.534 = 382,7 → 383;
-- Tenda Consolidado 1.639,7 mi / 6.211 = 264,0 → 264. var_tri_ant: MRV pela planilha,
-- Cury impressa, Direcional e Tenda calculadas sobre o 2T2026 salvo. var_acum_4t2020
-- com as bases do 0005 (MRV 179,0; Direcional 169,4; Tenda 144,3) e, para a Cury, a base
-- implícita no 2T2026 salvo (344,6 / 1,757143 = 196,1).
--
-- Idempotente: refaz só a linha 3T2026.

BEGIN;

DELETE FROM bronze.bronze_manual_dados_trimestrais WHERE periodo = '3T2026';

INSERT INTO bronze.bronze_manual_dados_trimestrais (
    periodo, trimestre_completo, trimestre, ano,

    mrv_lancamentos, mrv_vendas,
    mrv_var_lancamentos_tri_anterior, mrv_var_lancamentos_mesmo_tri_ano_anterior,
    mrv_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
    mrv_var_vendas_tri_anterior, mrv_var_vendas_mesmo_tri_ano_anterior,
    mrv_var_vendas_acumulado_mesmo_periodo_ano_anterior,

    cury_lancamentos, cury_vendas,
    cury_var_lancamentos_tri_anterior, cury_var_lancamentos_mesmo_tri_ano_anterior,
    cury_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
    cury_var_vendas_tri_anterior, cury_var_vendas_mesmo_tri_ano_anterior,
    cury_var_vendas_acumulado_mesmo_periodo_ano_anterior,

    tenda_lancamentos, tenda_vendas,
    tenda_var_lancamentos_tri_anterior, tenda_var_lancamentos_mesmo_tri_ano_anterior,
    tenda_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
    tenda_var_vendas_tri_anterior, tenda_var_vendas_mesmo_tri_ano_anterior,
    tenda_var_vendas_acumulado_mesmo_periodo_ano_anterior,

    direcional_lancamentos, direcional_vendas,
    direcional_var_lancamentos_tri_anterior, direcional_var_lancamentos_mesmo_tri_ano_anterior,
    direcional_var_lancamento_acumulado_mesmo_periodo_ano_anteri,
    direcional_var_vendas_tri_anterior, direcional_var_vendas_mesmo_tri_ano_anterior,
    direcional_var_vendas_acumulado_mesmo_periodo_ano_anterior,

    pacaembu_lancamentos, pacaembu_vendas,
    pacaembu_var_lancamentos_tri_anterior, pacaembu_var_lancamentos_mesmo_tri_ano_anterior,
    pacaembu_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
    pacaembu_var_vendas_tri_anterior, pacaembu_var_vendas_mesmo_tri_ano_anterior,
    pacaembu_var_vendas_acumulado_mesmo_periodo_ano_anterior,

    plano_plano_lancamentos, plano_plano_vendas,
    plano_plano_var_lancamentos_tri_anterior, plano_plano_var_lancamentos_mesmo_tri_ano_anterior,
    plano_plano_var_lancamento_acumulado_mesmo_periodo_ano_anter,
    plano_plano_var_vendas_tri_anterior, plano_plano_var_vendas_mesmo_tri_ano_anterior,
    plano_plano_var_vendas_acumulado_mesmo_periodo_ano_anterior,

    lancamentos_totais, vendas_totais,
    var_lancamentos_totais_tri_anterior, var_lancamentos_totais_mesmo_tri_ano_anterior,
    var_lancamentos_totais_acumulado_mesmo_periodo_ano_anterior,
    var_vendas_totais_tri_anterior, var_vendas_totais_mesmo_tri_ano_anterior,
    var_vendas_totais_acumulado_mesmo_periodo_ano_anterior,

    ticket_medio_lancamentos_mrv, ticket_medio_lancamentos_mrv_var_tri_ant,
    ticket_medio_lancamentos_mrv_var_acum_4t2020,
    ticket_medio_lancamentos_cury, ticket_medio_lancamentos_cury_var_tri_ant,
    ticket_medio_lancamentos_cury_var_acum_4t2020,
    ticket_medio_lancamentos_direcional, ticket_medio_lancamentos_direcional_var_tri_ant,
    ticket_medio_lancamentos_direcional_var_acum_4t2020,
    ticket_medio_lancamentos_tenda, ticket_medio_lancamentos_tenda_var_tri_ant,
    ticket_medio_lancamentos_tenda_var_acum_4t2020
) VALUES (
    '3T2026', '3T2026', '3T', 2026,

    -- MRV (planilha): lanç. 9.321,9 | vendas líq. 11.015,3
    9322, 11015, '-0.127', '0.116', '-0.034', '0.085', '0.257', '0.120',

    -- Cury: lanç. 6.503 (-0,7% | -4,6% | -6,6%), vendas 7.416 (10,7% | 8,3% | -0,9%)
    6503, 7416, '-0.007', '-0.046', '-0.066', '0.107', '0.083', '-0.009',

    -- Tenda Consolidado: lanç. 6.211 ((12,5%) | (7,7%) | 23,6%),
    --                    vendas 6.918 (3,9% | 6,5% | 13,5%)
    6211, 6918, '-0.125', '-0.077', '0.236', '0.039', '0.065', '0.135',

    -- Direcional (total, com Riva): lanç. 3.534 (-35,9% | -45,2% | -18,8%),
    --                              contratadas 3.642 (-26,5% | -25,0% | -6,4%)
    3534, 3642, '-0.359', '-0.452', '-0.188', '-0.265', '-0.250', '-0.064',

    -- Pacaembu: lanç. 5.222 (68,5% | -4,8% | -2,9%), vendas líq. 4.217 (24,1% | -17,8% | 3,8%)
    5222, 4217, '0.685', '-0.048', '-0.029', '0.241', '-0.178', '0.038',

    -- Plano&Plano: lanç. 3.819 (21,7% | -54,7% | -31,4%),
    --              vendas brutas 3.799 (5,5% | -5,6% | -5,3%)
    3819, 3799, '0.217', '-0.547', '-0.314', '0.055', '-0.056', '-0.053',

    -- Totais (soma das seis) e variações sobre os totais já salvos
    -- (2T2026 36.076/35.455; 3T2025 42.266/36.121; 9M25 113.307/104.896)
    34611, 37007,
    '-0.0406087149', '-0.1811148441', '-0.0604375723',
    '0.0437737978', '0.0245286675', '0.0423181056',

    -- Ticket médio de lançamentos (R$ mil)
    284, 0.026, 0.586592,
    326.5, -0.053, 0.66485,
    383, 0.021333, 1.260921,
    264, 0.060241, 0.829522
);

COMMIT;
