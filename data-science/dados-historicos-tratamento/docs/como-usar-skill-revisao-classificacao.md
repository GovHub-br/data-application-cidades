# Como Usar a Skill de Revisao de Classificacao

Skill versionada no repositorio:

```text
data-science/dados-historicos-tratamento/skills/mcmv-review-table-classification/
```

## Ativar no Codex

Para usar na conversa, peça explicitamente:

```text
Use a skill mcmv-review-table-classification para revisar dado a dado as classificacoes MCMV e gerar relatorio oficial com evidencias.
```

Se precisar instalar/sincronizar a skill local do repo no Codex:

```bash
mkdir -p ~/.codex/skills
cp -R data-science/dados-historicos-tratamento/skills/mcmv-review-table-classification ~/.codex/skills/
```

## Regerar Evidencias da Issue #96

Execute a partir da raiz do repositorio `data-application-cidades`:

```bash
python3 -m venv .review-venv
.review-venv/bin/pip install pandas matplotlib
.review-venv/bin/python data-science/dados-historicos-tratamento/scripts/revisao_classificacao_db_evidencias.py
.review-venv/bin/python data-science/dados-historicos-tratamento/scripts/revisao_issue96_evidencias.py
.review-venv/bin/python data-science/dados-historicos-tratamento/scripts/render_issue96_profissional.py
```

Para gerar o PDF com Chrome/Chromium:

```bash
google-chrome --headless --disable-gpu --no-sandbox --no-pdf-header-footer --print-to-pdf=data-science/dados-historicos-tratamento/docs/revisao-classificacao-issue-96.pdf data-science/dados-historicos-tratamento/docs/revisao-classificacao-issue-96.html
```

## Saidas Principais

- `docs/revisao-classificacao-issue-96.html`
- `docs/revisao-classificacao-issue-96.pdf`
- `docs/resposta-issue-96.md`
- `docs/evidencias/revisao-classificacao-issue-96/*.csv`
- `docs/assets/revisao-classificacao-issue-96/*.png`

Nota: se `.env` estiver preenchido, o primeiro script consulta o PostgreSQL e gera prints reais das amostras pipe antes/depois da correcao. Quando `data/table_samples/` e `diff_sftp_minio*.json` estiverem disponiveis na branch, rode novamente para complementar o impacto SFTP/MinIO detalhado.
