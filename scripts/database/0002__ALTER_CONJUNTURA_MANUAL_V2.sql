-- Versão 2 do script de dados manuais do relatório de conjuntura.
-- Ajustes levantados na reunião de 2026-08-24 com o setor de economia (CEAG).

SET search_path TO manual_conjuntura;

-- Novos Financiamentos Imobiliários por banco (aba "Dados Mensais" do boletim):
-- faltava a coluna "demais" (bancos fora dos 5 nominados). Total já existia
-- (abecip_sbpe_fin_uh_acum_total / abecip_sbpe_fin_milhoes_acum_total).
ALTER TABLE dados_mensais
    ADD COLUMN IF NOT EXISTS abecip_sbpe_fin_uh_acum_demais double precision,
    ADD COLUMN IF NOT EXISTS abecip_sbpe_fin_milhoes_acum_demais double precision;
