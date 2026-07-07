## Resumo da revisao - issue #96

Foi executada auditoria com pandas sobre os CSVs de classificacao, qualidade, inventario e deduplicacao do pipeline MCMV.

- Registros revisados nos snapshots: 754
- Linhas `confidence=low` apos revisao: 0
- Tabelas corrigidas: 5
- Correcao aplicada: 5 tabelas `bb_2013_06_junho_pmcmv_18062013_tab_*` foram alinhadas para `separador_|` (separador pipe), com status de qualidade `treated` e perfil `separador_pipe`.
- Evidencias geradas com pandas:
  - `docs/evidencias/revisao-classificacao-issue-96/auditoria_classificacao_completa_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_classificacao_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_qualidade_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/flags_revisao_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/amostras_disponibilidade_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/impacto_sftp_minio_pandas.csv`
- Relatorio oficial gerado:
  - `docs/revisao-classificacao-issue-96.html`
  - `docs/revisao-classificacao-issue-96.pdf`

Observacao: os arquivos brutos `data/table_samples/` e os JSONs `diff_sftp_minio*.json` nao estao presentes nesta branch. Portanto, a auditoria dado-a-dado dos CSVs brutos e o impacto detalhado SFTP/MinIO precisam ser reexecutados quando as amostras/diffs forem disponibilizados ou quando o modo DB estiver acessivel. As evidencias atuais usam snapshots, inventario, dedup, relatorio de tratamento e amostra versionada em `data/exemplos_por_categoria.md`.
