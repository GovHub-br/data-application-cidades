## Resumo da revisao de tratamento

Auditoria pandas executada sobre relatorios de qualidade, deduplicacao, classificacao, inventario e CSVs tratados disponiveis.

- Fonte principal: banco `dados_historicos_formatados`
- Tabelas no `_qualidade`: 395
- Status `treated`: 385 (97.5%)
- Erros no banco: 0
- Descartes: 10
- Discordancias amostradas linha a linha: 35
- `missing_pct > 30%`: 13
- `institution` desconhecida/vazia: 80
- `report_date` ausente: 99

**Veredito:** Tratamento sem erros no banco, mas ainda com discordancias a corrigir antes do uso preditivo final. A leitura do banco em dados_historicos_formatados mostra 385 de 395 registros de qualidade com status treated (97.5%), 10 descartes e 0 erros. A aprovacao final depende de corrigir as 35 discordancias amostradas: 13 tabelas com missing_pct > 30%, 80 com instituicao desconhecida, 99 com report_date ausente e descartes pipe com dados estruturaveis.

**Evidencias principais:**
- `docs/evidencias/revisao-tratamento-dados/auditoria_tratamento_pandas.csv`: base completa da auditoria.
- `docs/evidencias/revisao-tratamento-dados/flags_tratamento_pandas.csv`: 158 linhas com flags.
- `docs/evidencias/revisao-tratamento-dados/amostras_tratadas_pandas.csv`: registra que nao havia CSV tratado disponivel para prints reais.
- `docs/evidencias/revisao-tratamento-dados/impacto_sftp_tratamento_pandas.csv`: sem diffs SFTP encontrados nesta branch.
- `docs/evidencias/revisao-tratamento-dados/resumo_db_tratamento_pandas.csv`: resumo da leitura DB.
- `docs/evidencias/revisao-tratamento-dados/discordancias_tratamento_db_pandas.csv`: amostras brutas e tratadas das discordancias.

**Como consertar:**
- Corrigir descartes pipe para `separador_|` e reprocessar `bb_2013_06_junho_pmcmv_18062013_tab_*`.
- Revisar `sub_tabelas_3` com missing alto; separar blocos, remover colunas vazias e avaliar formato longo.
- Ampliar `extrair_periodo_filename()` para datas textuais/abreviadas como `25jan2011`, `22mar11`, `31dez09`.
- Revisar descartes restantes antes de aceitar como `vazia` ou `dados_sem_utilidade`.

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
