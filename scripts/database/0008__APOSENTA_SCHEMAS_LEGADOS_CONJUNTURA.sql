-- Aposenta os schemas legados do conjuntura e renomeia os do projeto vivo.
--
-- Contexto: o produto tinha duas gerações convivendo no banco.
--
--   conjuntura_bronze / _silver / _gold   legado, marcado em schemas.yml como
--                                         `conjuntura_legacy` e
--                                         `retirement_candidate`
--   conjuntura_continuo_bronze / _silver  projeto vivo, escrito pelo dbt
--   / _mart / _snapshots
--
-- Auditoria feita antes desta migração: das 14 tabelas declaradas no bronze
-- legado, 13 estão órfãs (nenhum modelo as lê) e todas têm cobertura na
-- arquitetura nova, que passou a ler parquet de staging via pg_duckdb. A única
-- ainda usada é `bronze_cbic_lancamentos_vendas`, carga manual sem DAG de
-- ingestão (ver 0003__INSERT_CBIC_MANUAL.sql), lida por 3 modelos gold da
-- página 1 do boletim.
--
-- Por isso a ordem abaixo: a CBIC é preservada primeiro, o legado só cai
-- depois, e o rename só acontece com o nome já livre. Ao final, o script de
-- carga manual da CBIC continua válido sem alteração — ele aponta para
-- `conjuntura_bronze.bronze_cbic_lancamentos_vendas`, que passa a ser o
-- bronze novo.
--
-- ⚠️ DESTRUTIVO. O DROP ... CASCADE elimina as 13 tabelas órfãs e tudo que
-- depender delas. Faça o dump antes:
--
--   pg_dump -n conjuntura_bronze -n conjuntura_silver -n conjuntura_gold \
--           -Fc -f legado_conjuntura_$(date +%F).dump "$DB"
--
-- Depois de rodar: `dbt build` recria nada (os schemas apenas mudam de nome),
-- mas o Superset e o OpenMetadata precisam de reprocessamento — ver o final.

BEGIN;

-- ── 1. Confere as premissas antes de destruir qualquer coisa ────────────────
DO $$
DECLARE
    faltando text;
BEGIN
    -- os schemas do projeto vivo têm de existir com o nome antigo
    FOREACH faltando IN ARRAY ARRAY[
        'conjuntura_continuo_bronze',
        'conjuntura_continuo_silver',
        'conjuntura_continuo_mart'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM information_schema.schemata
                       WHERE schema_name = faltando) THEN
            RAISE EXCEPTION
                'schema % não existe — a migração já rodou ou o banco diverge do repositório',
                faltando;
        END IF;
    END LOOP;

    -- a CBIC tem de estar onde a auditoria a encontrou
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'conjuntura_bronze'
                     AND table_name = 'bronze_cbic_lancamentos_vendas') THEN
        RAISE EXCEPTION
            'conjuntura_bronze.bronze_cbic_lancamentos_vendas não encontrada — pare e reaudite';
    END IF;
END $$;

-- ── 2. Preserva a única tabela legada ainda lida ────────────────────────────
ALTER TABLE conjuntura_bronze.bronze_cbic_lancamentos_vendas
    SET SCHEMA conjuntura_continuo_bronze;

-- ── 3. Aposenta o legado ────────────────────────────────────────────────────
DROP SCHEMA IF EXISTS conjuntura_bronze CASCADE;
DROP SCHEMA IF EXISTS conjuntura_silver CASCADE;
DROP SCHEMA IF EXISTS conjuntura_gold   CASCADE;

-- ── 4. Renomeia o projeto vivo para os nomes livres ─────────────────────────
ALTER SCHEMA conjuntura_continuo_bronze RENAME TO conjuntura_bronze;
ALTER SCHEMA conjuntura_continuo_silver RENAME TO conjuntura_silver;
ALTER SCHEMA conjuntura_continuo_mart   RENAME TO conjuntura_mart;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.schemata
               WHERE schema_name = 'conjuntura_continuo_snapshots') THEN
        EXECUTE 'ALTER SCHEMA conjuntura_continuo_snapshots RENAME TO conjuntura_snapshots';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.schemata
               WHERE schema_name = 'conjuntura_continuo') THEN
        EXECUTE 'ALTER SCHEMA conjuntura_continuo RENAME TO conjuntura';
    END IF;
END $$;

COMMIT;

-- ── 5. Confirmação ──────────────────────────────────────────────────────────
-- Esperado: conjuntura_bronze, conjuntura_silver, conjuntura_mart
-- (e conjuntura_snapshots, se havia snapshots materializados).
-- Nenhum `conjuntura_continuo*`, nenhum `conjuntura_gold`.
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name LIKE 'conjuntura%'
ORDER BY schema_name;

-- A CBIC tem de continuar respondendo, agora no bronze novo:
SELECT count(*) AS linhas_cbic
FROM conjuntura_bronze.bronze_cbic_lancamentos_vendas;

-- ── 6. Depois desta migração ────────────────────────────────────────────────
--
--   a) Superset — os datasets apontam para os nomes antigos. Re-rodar:
--        python scripts/superset/bootstrap_conjuntura.py
--        python scripts/superset/build_boletim.py
--
--   b) OpenMetadata — o catálogo tem as tabelas sob os FQN antigos, e o
--      conector não apaga o que sumiu (`markDeletedTables: false`, proposital).
--      Disparar a DAG `openmetadata_ingestion_dag` e remover à mão as entradas
--      de `conjuntura_continuo*` e `conjuntura_gold`, que ficarão órfãs.
--
--   c) Catálogo semântico do repositório — regenerar, porque ele carrega os
--      nomes de schema:
--        make openmetadata-catalog
