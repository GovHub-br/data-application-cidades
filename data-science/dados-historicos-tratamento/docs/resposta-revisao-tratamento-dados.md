## Resumo da revisao de tratamento

Auditoria pandas executada sobre relatorios de qualidade, deduplicacao, classificacao, inventario e CSVs tratados disponiveis.

- Tabelas revisadas: 395
- Status `treated`: 356 (90.1%)
- CSVs tratados encontrados: 0
- Flags de revisao: 158
- Erros: 34
- Descartes: 5
- Erros por identificador longo: 23

**Veredito:** Parcialmente tratados, ainda nao aprovados para uso preditivo final. O pipeline processou a maior parte do acervo, mas ainda nao ha aprovacao final para uso preditivo porque faltam os CSVs tratados (`data/treated_tables/`) para inspecao linha a linha e existem erros/flags relevantes.

**Evidencias principais:**
- `docs/evidencias/revisao-tratamento-dados/auditoria_tratamento_pandas.csv`: base completa da auditoria.
- `docs/evidencias/revisao-tratamento-dados/flags_tratamento_pandas.csv`: 158 linhas com flags.
- `docs/evidencias/revisao-tratamento-dados/amostras_tratadas_pandas.csv`: registra que nao havia CSV tratado disponivel para prints reais.
- `docs/evidencias/revisao-tratamento-dados/impacto_sftp_tratamento_pandas.csv`: sem diffs SFTP encontrados nesta branch.

**O que falta localizar/regenerar:**
- `data/treated_tables/`
- `diff_sftp_minio*.json`

Artefatos:
- `docs/evidencias/revisao-tratamento-dados/auditoria_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/flags_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/amostras_tratadas_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/impacto_sftp_tratamento_pandas.csv`
- `docs/revisao-tratamento-dados.html`
- `docs/revisao-tratamento-dados.pdf`
