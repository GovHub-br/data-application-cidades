-- Preenche o 2T2026 (linha já existe vazia) em
-- manual_conjuntura.dados_trimestrais — Balanço das empresas (RI de cada
-- construtora), Página 2. Sem API — dado sai do release trimestral de
-- cada construtora. Ver conjuntura-fontes-dbt.md (documentação local, fora do repositório).
--
-- Unidades por empresa (checado contra o histórico já na tabela pra
-- garantir que estou pegando a mesma linha/segmento que os trimestres
-- anteriores usaram):
--   MRV      → "Lançamentos Unidades" / "Vendas Líquidas Unidades"
--              (bate exato com 1T2026: 10.386 / 9.141 já na tabela).
--   Cury     → "Número de Unidades" da tabela de Lançamentos / mesma
--              coluna da tabela "Vendas, %VSO" (bate exato com 1T2026:
--              8.001 / 7.786 já na tabela).
--   Tenda    → segmento "Consolidado" (Tenda + Alea), não "Tenda" sozinho
--              — só o Consolidado bate com o histórico (1T2026 vendas
--              7.260 bate exato; lançamentos 1T2026 do print, 6.344,
--              ficou 10 unidades acima do 6.334 já salvo — diferença
--              pequena, provável pequeno ajuste do release novo; mantive
--              o valor já salvo de 1T2026 como estava).
--   Direcional → linha TOTAL "Unidades Lançadas"/"Unidades Contratadas"
--              da Direcional Engenharia — inclui a marca Riva, não é só a
--              marca "Direcional" isolada (bate exato com 1T2026: 3.109 /
--              4.848 já na tabela).
--   Plano &    → "Unidades" (lançamentos) / "Vendas Contratadas Brutas
--   Plano        (Unidades)" (bate exato com 1T2026: 3.663 / 3.536 já na
--              tabela — vendas usa BRUTAS, não líquidas).
--
--   Pacaembu → "Número de Unidades" tanto de Lançamentos quanto de
--              "Vendas Líquidas" (bate exato com 1T2026: 4.279 / 4.302
--              já na tabela).
--
-- Variações (colunas `*_var_*`, todas TEXT): copiadas DIRETO das colunas
-- "Var. 2T26 x 1T26" / "Var. 2T26 x 2T25" / "Var. 1S26 x 1S25" (ou
-- "Δ%(a/b)"/"Δ%(a/c)"/"Δ%(d/e)", dependendo do layout do release) que já
-- vêm prontas nos releases de cada construtora — NÃO recalculadas por
-- nós. Isso importa porque a construtora às vezes usa uma base ligeiramente
-- diferente da que temos salva (ex.: a MRV publicou vendas 2T25 = 9.922,
-- nossa tabela tem 9.927 salvo desde antes) — o % oficial do release é o
-- que vale, não o que sairia de "nosso número atual vs nosso número
-- salvo". Convertido de % pra fração (2,8% -> 0.028) na mesma casa
-- decimal que o release publica, sem inventar mais dígitos.
--
-- `lancamentos_totais`/`vendas_totais` = soma das 6 construtoras (bate
-- exato com o valor já salvo em 1T2026: 35.772 / 36.873) — como essa soma
-- é nossa, não de nenhuma construtora, a variação dela (`var_*_totais_*`)
-- É calculada por nós mesmo, não tem release pra copiar.

UPDATE manual_conjuntura.dados_trimestrais SET
    mrv_lancamentos = 10679,
    mrv_vendas = 10148,
    mrv_var_lancamentos_tri_anterior = '0.028',
    mrv_var_lancamentos_mesmo_tri_ano_anterior = '-0.128',
    mrv_var_lancamento_acumulado_mesmo_periodo_ano_anterior = '-0.088',
    mrv_var_vendas_tri_anterior = '0.110',
    mrv_var_vendas_mesmo_tri_ano_anterior = '0.023',
    mrv_var_vendas_acumulado_mesmo_periodo_ano_anterior = '0.054',

    cury_lancamentos = 6549,
    cury_vendas = 6697,
    cury_var_lancamentos_tri_anterior = '-0.181',
    cury_var_lancamentos_mesmo_tri_ano_anterior = '-0.006',
    cury_var_lancamento_acumulado_mesmo_periodo_ano_anterior = '-0.074',
    cury_var_vendas_tri_anterior = '-0.140',
    cury_var_vendas_mesmo_tri_ano_anterior = '-0.170',
    cury_var_vendas_acumulado_mesmo_periodo_ano_anterior = '-0.050',

    tenda_lancamentos = 7099,
    tenda_vendas = 6657,
    tenda_var_lancamentos_tri_anterior = '0.133',
    tenda_var_lancamentos_mesmo_tri_ano_anterior = '0.384',
    tenda_var_lancamento_acumulado_mesmo_periodo_ano_anterior = '0.466',
    tenda_var_vendas_tri_anterior = '-0.083',
    tenda_var_vendas_mesmo_tri_ano_anterior = '0.061',
    tenda_var_vendas_acumulado_mesmo_periodo_ano_anterior = '0.173',

    direcional_lancamentos = 5511,
    direcional_vendas = 4955,
    direcional_var_lancamentos_tri_anterior = '0.773',
    direcional_var_lancamentos_mesmo_tri_ano_anterior = '0.081',
    direcional_var_lancamento_acumulado_mesmo_periodo_ano_anteri = '0.012',
    direcional_var_vendas_tri_anterior = '0.022',
    direcional_var_vendas_mesmo_tri_ano_anterior = '-0.043',
    direcional_var_vendas_acumulado_mesmo_periodo_ano_anterior = '0.031',

    pacaembu_lancamentos = 3100,
    pacaembu_vendas = 3397,
    pacaembu_var_lancamentos_tri_anterior = '-0.276',
    pacaembu_var_lancamentos_mesmo_tri_ano_anterior = '-0.402',
    pacaembu_var_lancamento_acumulado_mesmo_periodo_ano_anterior = '-0.015',
    pacaembu_var_vendas_tri_anterior = '-0.210',
    pacaembu_var_vendas_mesmo_tri_ano_anterior = '-0.055',
    pacaembu_var_vendas_acumulado_mesmo_periodo_ano_anterior = '0.213',

    plano_plano_lancamentos = 3138,
    plano_plano_vendas = 3601,
    plano_plano_var_lancamentos_tri_anterior = '-0.143',
    plano_plano_var_lancamentos_mesmo_tri_ano_anterior = '0.142',
    plano_plano_var_lancamento_acumulado_mesmo_periodo_ano_anter = '-0.035',
    plano_plano_var_vendas_tri_anterior = '0.018',
    plano_plano_var_vendas_mesmo_tri_ano_anterior = '0.009',
    plano_plano_var_vendas_acumulado_mesmo_periodo_ano_anterior = '-0.005',

    lancamentos_totais = 36076,
    vendas_totais = 35455,
    var_lancamentos_totais_tri_anterior = '0.008498266801',
    var_lancamentos_totais_mesmo_tri_ano_anterior = '-0.02481483484',
    var_lancamentos_totais_acumulado_mesmo_periodo_ano_anterior = '0.01135963739',
    var_vendas_totais_tri_anterior = '-0.03845632305',
    var_vendas_totais_mesmo_tri_ano_anterior = '-0.03154875717',
    var_vendas_totais_acumulado_mesmo_periodo_ano_anterior = '0.0516612141'
WHERE periodo = '2T2026';

-- Balanço das empresas 2T2026: completo (6 construtoras + totais,
-- lançamentos/vendas e todas as variações).
