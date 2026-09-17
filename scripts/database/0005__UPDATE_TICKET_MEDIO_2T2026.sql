-- Preenche o ticket médio de lançamentos (MRV, Direcional, Tenda, Cury)
-- pra 2T2026 em manual_conjuntura.dados_trimestrais — Página 6/7.
-- Sem API — dado sai do release trimestral de cada construtora (mesmos
-- releases já usados no 0004__INSERT_EMPRESAS_2T2026_MANUAL.sql).
--
-- Fonte de cada valor:
--   MRV        → linha "Ticket Médio (R$ mil)" do próprio release
--                (bate exato com 1T2026: 281 já salvo na tabela).
--   Cury       → linha "Preço Médio/Unid. (R$ mil)" do próprio release
--                (bate exato com 1T2026: 330,8... arredondado no que já
--                tinha? conferido, coluna é double precision, aceita
--                casa decimal).
--   Direcional → NÃO publica "preço médio" pronto — calculado aqui como
--                VGV Lançado (VGV 100%) ÷ Unidades Lançadas, igual a
--                própria Cury/Tenda fazem (conferido: bate exato com o
--                que essas duas publicam prontas). 2T26: 2.065,2mi /
--                5.511 un = R$374,8 mil, arredondado pra 375 (coluna é
--                bigint, só inteiro).
--   Tenda      → mesma situação da Direcional, usando o segmento
--                Consolidado (Tenda + Alea, mesmo critério do
--                0004__INSERT_EMPRESAS_2T2026_MANUAL.sql): VGV 1.766,4mi
--                / 7.099 un = R$248,8 mil, arredondado pra 249.
--
-- Variação trimestre anterior (var_tri_ant): MRV e Cury usam a % que o
-- próprio release publica (mesma regra do 0004 — não recalculamos por
-- cima do nosso número salvo). Direcional e Tenda calculadas (não têm %
-- publicada porque a própria construtora não publica "preço médio").
--
-- Variação acumulada desde 4T2020 (var_acum_4t2020): a tabela não tem
-- linha "4T2020" (não guardamos mais). Primeiro tentei reconstruir o
-- valor-base de cada construtora invertendo a fórmula a partir de
-- 3T2025/4T2025 (base = valor ÷ (1 + var_acum_4t2020)) — deu MRV=179,
-- Direcional=169, Tenda=144.
--
-- CONFERIDO CONTRA OS BOLETINS PUBLICADOS DE VERDADE (2026-08-25, Lucas
-- mandou os PDFs de 3T2025/4T2025/1T2026): os três têm a tabela
-- "Variação do ticket médio das unidades lançadas... comparação com o
-- INCC", que publica a base de 4T2020 direto: INCC=842,683,
-- MRV=R$179,0 mil, Direcional=R$169,4 mil, Tenda=R$144,3 mil. MRV bateu
-- exato; Direcional e Tenda tinham uma casa decimal que o arredondamento
-- do bigint escondeu — corrigido aqui pra usar a base certa (169,4 e
-- 144,3, não 169 e 144).
--
-- Cury NÃO aparece nessa tabela de nenhum dos 3 boletins (comparação
-- histórica sempre foi só INCC/MRV/Direcional/Tenda) — a base de 196
-- usada aqui continua sendo só a reconstrução por 3T2025/4T2025, **sem
-- confirmação externa**. Se aparecer um boletim futuro com Cury nessa
-- tabela, conferir de novo.

UPDATE manual_conjuntura.dados_trimestrais SET
    ticket_medio_lancamentos_mrv = 277,
    ticket_medio_lancamentos_mrv_var_tri_ant = -0.015,
    ticket_medio_lancamentos_mrv_var_acum_4t2020 = 0.547486,

    ticket_medio_lancamentos_cury = 344.6,
    ticket_medio_lancamentos_cury_var_tri_ant = 0.042,
    ticket_medio_lancamentos_cury_var_acum_4t2020 = 0.757143,

    ticket_medio_lancamentos_direcional = 375,
    ticket_medio_lancamentos_direcional_var_tri_ant = 0.157407,
    ticket_medio_lancamentos_direcional_var_acum_4t2020 = 1.213695,

    ticket_medio_lancamentos_tenda = 249,
    ticket_medio_lancamentos_tenda_var_tri_ant = 0.082609,
    ticket_medio_lancamentos_tenda_var_acum_4t2020 = 0.725572
WHERE periodo = '2T2026';
