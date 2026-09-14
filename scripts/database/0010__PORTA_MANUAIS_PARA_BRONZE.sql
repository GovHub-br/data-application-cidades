-- Move as cargas manuais do conjuntura para o schema `bronze` da nova arquitetura.
--
-- O 0009 unificou o conjuntura no schema `conjuntura` com a camada no prefixo
-- do nome (`bnz_`/`slv_`/`gld_`). Desde então o projeto migrou para os três
-- schemas compartilhados `bronze`/`prata`/`ouro`, e os models do conjuntura já
-- vivem lá. Só estas seis tabelas ficaram para trás: são carga manual, não são
-- produzidas por model, então o dbt não tinha como recriá-las no lugar novo.
--
-- Aqui elas mudam de endereço. Não há reinserção de dados: `ALTER TABLE` move
-- a tabela inteira, preservando conteúdo, tipos e ordem das colunas.
--
-- Na bronze o nome carrega a ORIGEM, não o domínio. `manual` é a origem de
-- fato — planilha boletim.xlsx revisada à mão, sem DAG de ingestão — e `cbic`
-- é a entidade que publica o dado de lançamentos e vendas.
--
-- Idempotente: cada bloco só age se a origem ainda existir.
--
-- ORDEM: rode depois do 0008. Sem ele as tabelas de origem não existem.

BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze;

DO $$
DECLARE
    -- origem em `conjuntura` → destino em `bronze`
    par record;
BEGIN
    FOR par IN
        SELECT *
        FROM (VALUES
            ('bnz_manual_dados_mensais',                    'bronze_manual_dados_mensais'),
            ('bnz_manual_dados_trimestrais',                'bronze_manual_dados_trimestrais'),
            ('bnz_manual_empresas_balanco_lancamentos_vendas',
                                        'bronze_manual_empresas_balanco_lancamentos_vendas'),
            ('bnz_manual_fgts_valor_medio_imoveis',         'bronze_manual_fgts_valor_medio_imoveis'),
            ('bnz_manual_sbpe_financiamentos_aquisicao_bancos',
                                        'bronze_manual_sbpe_financiamentos_aquisicao_bancos'),
            ('bnz_cbic_lancamentos_vendas',                 'bronze_cbic_lancamentos_vendas')
        ) AS t(origem, destino)
    LOOP
        -- destino já no lugar: nada a fazer, o script é reexecutável
        IF EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'bronze' AND table_name = par.destino) THEN
            RAISE NOTICE 'bronze.% já existe, pulando', par.destino;
            CONTINUE;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_schema = 'conjuntura' AND table_name = par.origem) THEN
            RAISE EXCEPTION 'conjuntura.% não existe — rode o 0008 antes', par.origem;
        END IF;

        EXECUTE format('ALTER TABLE conjuntura.%I SET SCHEMA bronze', par.origem);
        EXECUTE format('ALTER TABLE bronze.%I RENAME TO %I', par.origem, par.destino);
        RAISE NOTICE 'conjuntura.% → bronze.%', par.origem, par.destino;
    END LOOP;
END $$;

COMMIT;
