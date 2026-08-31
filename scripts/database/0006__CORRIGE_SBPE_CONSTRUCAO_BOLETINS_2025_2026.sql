-- Corrige os indicadores manuais de SBPE Construção (UH) usados na página
-- "Financiamentos Habitacionais" do Boletim de Conjuntura.
--
-- Fonte primária e valores publicados:
--   * Boletim de Conjuntura 2025 4T FINAL.pdf
--   * Boletim de Conjuntura 2026 1T Final.pdf
--
-- A carga anterior deixou o SBPE vazio ou desalinhado no histórico. Este
-- script é idempotente e limita-se aos quatro períodos necessários para
-- reconciliar os dois boletins disponíveis.

UPDATE manual_conjuntura.dados_trimestrais
SET
    financ_hab_sbpe_constr = 40623,
    financ_hab_sbpe_constr_acumulado_12_meses = 190968
WHERE periodo = '4T2024';

UPDATE manual_conjuntura.dados_trimestrais
SET
    financ_hab_sbpe_constr = 19130,
    financ_hab_sbpe_constr_acumulado_12_meses = 177376
WHERE periodo = '1T2025';

UPDATE manual_conjuntura.dados_trimestrais
SET
    financ_hab_sbpe_constr = 47766,
    financ_hab_sbpe_constr_acumulado_12_meses = 132859
WHERE periodo = '4T2025';

UPDATE manual_conjuntura.dados_trimestrais
SET
    financ_hab_sbpe_constr = 47609,
    financ_hab_sbpe_constr_acumulado_12_meses = 161338
WHERE periodo = '1T2026';
