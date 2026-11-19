# Revisao da issue #96 - classificacao das tabelas

## Escopo

- Projeto/pasta:
- Data da revisao:
- Modo usado: CSV / DB / arquivos gerados existentes
- Comandos executados:

## Resumo

- Total de tabelas revisadas:
- Tabelas corrigidas:
- Tabelas duvidosas:
- Divergencias automatico vs manual restantes:
- Observacoes de impacto SFTP/MinIO:

## Tabelas revisadas

| tabela | classificacao atual | classificacao revisada | decisao | evidencia pandas |
|---|---|---|---|---|
|  |  |  | mantida/corrigida/duvidosa | shape, colunas, linhas vazias, pipes, print |

## Prints de amostras

Inclua prints compactos das tabelas corrigidas ou duvidosas. Para revisoes completas, inclua prints completos apenas dos casos alterados/incertos e uma tabela de metricas para os demais.

```text
=== nome_da_tabela ===
shape=(linhas, colunas)
colunas=[...]
metricas: empty_row_ratio=..., unnamed_col_ratio=..., pipe_cells=...

<df.head(8).to_string()>
```

## Metricas pandas por tabela

| tabela | shape | empty_row_ratio | unnamed_col_ratio | pipe_cells | indicio estrutural |
|---|---:|---:|---:|---:|---|
|  |  |  |  |  |  |

## Correcoes aplicadas

| tabela/padrao | arquivo alterado | antes | depois | motivo |
|---|---|---|---|---|
|  |  |  |  |  |

## Classificacao duvidosa

| tabela | classificacao candidata | duvida | proxima verificacao |
|---|---|---|---|
|  |  |  |  |

## Inconsistencias observadas

- 

## Impacto SFTP/MinIO

| tabela/path | artefato SFTP | impacto na classificacao | acao |
|---|---|---|---|
|  |  |  |  |

## Verificacao

- Resultado de `uv run python main.py --classify-only`:
- Resultado de testes:
- Limitacoes:
