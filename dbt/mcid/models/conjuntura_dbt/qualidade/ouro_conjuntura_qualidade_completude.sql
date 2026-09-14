{{ config(materialized='table') }}

-- Perfil de completude dos models da prata e da ouro do conjuntura.
-- Uma linha por coluna, com % de preenchimento.
--
-- Responde ao item 7 do checklist de qualidade ("porcentagem de completude
-- das bases"). Colunas de metadado de ingestão (`dt_ingest`, `_source_*`)
-- ficam de fora — completude delas não diz nada sobre o dado.
--
-- Lido junto com `ouro_conjuntura_qualidade_schema` (item 5), dá o retrato da
-- saúde das bases sem precisar abrir model por model.
--
-- A bronze fica fora de propósito: ela espelha a origem e pode conter coluna
-- com identificador de pessoa, que não deve aparecer em camada de consumo.
-- Ver `macros/coluna_sensivel.sql`.

{{ perfil_completude(
    relacoes=relacoes_do_produto(
        'conjuntura_dbt',
        camadas=['prata', 'ouro'],
        excluir_prefixo='ouro_conjuntura_qualidade',
    )
) }}
