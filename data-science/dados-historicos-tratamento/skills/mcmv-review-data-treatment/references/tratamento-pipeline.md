# Pipeline de Tratamento MCMV

Use esta referencia para entender a sequencia esperada antes de revisar tratamentos.

## Fluxo

```text
classificacao_formacao.csv
  -> deduplicacao por MD5
  -> roteador tratar_tabela()
  -> tratamento por categoria
  -> validacao pos-tratamento
  -> data/treated_tables/*_tratado.csv
  -> _quality_report.csv ou _qualidade_*.csv
```

## Pipeline de Tabelas Bem Formadas

- Normalizar nomes de colunas: lowercase, sem acentos, caracteres especiais como `_`, fallback `col`.
- Detectar separador decimal: amostra de valores, virgula decimal brasileira vira ponto antes de `float64`.
- Parsear datas: colunas com `dat_`, `dt_`, `data_`; reverter se falhas excederem o limite de seguranca.
- Reparar encoding: detectar mojibake/latin1 e aplicar reparo quando confiavel.
- Canonizar tipos: CNPJ/cod/nr/nu como string; valor/total como float; qtd/unidades como inteiro nullable; datas como datetime.
- Classificar perfil: `lookup`, `event_level`, `colunar_denso`, `agregado_uf`.
- Extrair periodo e instituicao do nome do arquivo.
- Adicionar metadados: `source_table`, `report_date`, `institution`, `profile`, `content_hash`.

## Tratamentos Especiais

- `sem_cabecalho`: inferir nomes por referencia de colunas ou heuristicas de CNPJ, datas, valores, codigos e frentes MCMV.
- `cabecalho_*`: promover linha correta para header, remover metadados/totais e aplicar pipeline comum.
- `cabecalho_composto_*`: forward-fill em linhas de header, concatenar hierarquia e aplicar pipeline comum.
- `sub_tabelas_*`: identificar blocos separados por vazios, extrair headers e consolidar ou gerar multiplos DataFrames.
- `separador_|`: expandir celulas separadas por pipe, usar primeira linha como cabecalho e aplicar pipeline comum.
- `vazia` e `dados_sem_utilidade`: descartar com registro em qualidade.

## Saida de Qualidade

Campos esperados: `table_name`, `status`, `n_rows`, `n_cols`, `profile`, `institution`, `report_date`, `missing_pct`, `encoding_issues`, `date_parse_errors`, `type_coercion_warnings`, `error`.

## Comandos

```bash
uv run python main.py
uv run python main.py --skip-classify
uv run pytest tests/ -k tratamento
uv run pytest tests/ -k validacao
```
