-- Aposenta os schemas antigos do conjuntura. Tudo passa a viver em `conjuntura`.
--
-- Antes, o produto estava espalhado por até sete schemas: a geração legada
-- (`conjuntura_bronze`, `_silver`, `_gold`, que a própria governança já marcava
-- como `retirement_candidate`), a geração seguinte (`conjuntura_continuo_*`) e
-- o `manual_conjuntura` das cargas feitas à mão.
--
-- Agora a camada de cada tabela vem do PREFIXO DO NOME, não do schema:
--
--   bnz_   ingestão fiel          bnz_manual_*  carga manual, sem DAG
--   slv_   dado conformado
--   gld_   consumo (boletim, dashboards)
--   snap_  congelamento das edições publicadas
--
-- É o mesmo desenho já usado por `empreendimento_far` e `entidades_fds`.
--
-- ORDEM: rode o 0008 ANTES deste script. Ele recria as seis tabelas de carga
-- manual dentro de `conjuntura`, a partir do conteúdo que estava só no banco.
-- Sem isso, este script apaga dado que não volta.
--
-- ⚠️ DESTRUTIVO. Faça o dump antes:
--
--   pg_dump -n 'conjuntura_*' -n manual_conjuntura -Fc \
--           -f conjuntura_antes_da_unificacao_$(date +%F).dump "$DB"

BEGIN;

-- ── 1. Confere que o 0008 já rodou ──────────────────────────────────────────
DO $$
DECLARE
    esperada text;
BEGIN
    FOREACH esperada IN ARRAY ARRAY[
        'bnz_manual_dados_mensais',
        'bnz_manual_dados_trimestrais',
        'bnz_manual_empresas_balanco_lancamentos_vendas',
        'bnz_manual_fgts_valor_medio_imoveis',
        'bnz_manual_sbpe_financiamentos_aquisicao_bancos',
        'bnz_cbic_lancamentos_vendas'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_schema = 'conjuntura' AND table_name = esperada) THEN
            RAISE EXCEPTION
                'conjuntura.% não existe — rode 0008__CENTRALIZA_DADOS_MANUAIS_CONJUNTURA.sql primeiro',
                esperada;
        END IF;
    END LOOP;
END $$;

-- ── 2. Aposenta os schemas antigos ──────────────────────────────────────────
--
-- `manual_conjuntura` fica de fora de propósito: é a origem histórica das
-- cargas manuais, e o 0008 acabou de copiá-las. Só remova depois de conferir a
-- paridade linha a linha — a decisão é da equipe, não deste script.
DROP SCHEMA IF EXISTS conjuntura_bronze             CASCADE;
DROP SCHEMA IF EXISTS conjuntura_silver             CASCADE;
DROP SCHEMA IF EXISTS conjuntura_gold               CASCADE;
DROP SCHEMA IF EXISTS conjuntura_mart               CASCADE;
DROP SCHEMA IF EXISTS conjuntura_snapshots          CASCADE;
DROP SCHEMA IF EXISTS conjuntura_continuo           CASCADE;
DROP SCHEMA IF EXISTS conjuntura_continuo_bronze    CASCADE;
DROP SCHEMA IF EXISTS conjuntura_continuo_silver    CASCADE;
DROP SCHEMA IF EXISTS conjuntura_continuo_mart      CASCADE;
DROP SCHEMA IF EXISTS conjuntura_continuo_snapshots CASCADE;

COMMIT;

-- ── 3. Confirmação ──────────────────────────────────────────────────────────
-- Esperado: apenas `conjuntura` e `manual_conjuntura`.
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name LIKE '%conjuntura%'
ORDER BY schema_name;

-- As seis tabelas manuais têm de responder no schema novo:
SELECT 'bnz_manual_dados_mensais'  AS tabela, count(*) FROM conjuntura.bnz_manual_dados_mensais
UNION ALL SELECT 'bnz_manual_dados_trimestrais', count(*) FROM conjuntura.bnz_manual_dados_trimestrais
UNION ALL SELECT 'bnz_manual_empresas_balanco', count(*) FROM conjuntura.bnz_manual_empresas_balanco_lancamentos_vendas
UNION ALL SELECT 'bnz_manual_fgts_valor_medio', count(*) FROM conjuntura.bnz_manual_fgts_valor_medio_imoveis
UNION ALL SELECT 'bnz_manual_sbpe_aquisicao', count(*) FROM conjuntura.bnz_manual_sbpe_financiamentos_aquisicao_bancos
UNION ALL SELECT 'bnz_cbic_lancamentos_vendas', count(*) FROM conjuntura.bnz_cbic_lancamentos_vendas;

-- ── 4. Depois desta migração ────────────────────────────────────────────────
--
--   a) dbt — reconstruir tudo no schema novo:
--        dbt build --project-dir dbt/mcid --select conjuntura_dbt
--
--   b) Superset — os datasets apontam para os schemas e nomes antigos:
--        python scripts/superset/bootstrap_conjuntura.py
--        python scripts/superset/build_boletim.py
--
--   c) OpenMetadata — o conector não apaga o que some
--      (`markDeletedTables: false`, proposital). Disparar a
--      `openmetadata_ingestion_dag` e remover à mão as entradas dos schemas
--      aposentados, que ficarão órfãs no catálogo.
--
--   d) Catálogo semântico do repositório, que carrega os nomes:
--        make openmetadata-catalog
