## Resumo da revisao - issue #96

Foi executada auditoria com pandas sobre os CSVs de classificacao, qualidade, inventario, deduplicacao e amostras reais do PostgreSQL do pipeline MCMV.

- Registros revisados nos snapshots: 754
- Linhas `confidence=low` apos revisao: 0
- Tabelas corrigidas: 6
- Correcao aplicada: 6 tabelas `bb_2013_06_junho_pmcmv_18062013_tab_*` foram alinhadas para `separador_|` (separador pipe), com status de qualidade `treated` e perfil `separador_pipe`.
- Erro principal encontrado: `tab_arquivos_dados` estava como `dados_sem_utilidade`, mas a amostra real do banco contem valores delimitados por pipe; portanto, deve ser tratada como tabela estruturavel.
- Evidencias geradas com pandas:
  - `docs/evidencias/revisao-classificacao-issue-96/auditoria_classificacao_completa_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_classificacao_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_qualidade_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_inventario_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_classificacao_db_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/amostras_pipe_db_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/flags_revisao_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/amostras_disponibilidade_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/impacto_sftp_minio_pandas.csv`
- Relatorio oficial gerado:
  - `docs/revisao-classificacao-issue-96.html`
  - `docs/revisao-classificacao-issue-96.pdf`

Observacao: os arquivos brutos `data/table_samples/` nao estao presentes nesta branch. Para as tabelas corrigidas, a auditoria dado-a-dado foi feita via banco PostgreSQL configurado no `.env`, com prints reais salvos em CSV e exibidos no PDF.
