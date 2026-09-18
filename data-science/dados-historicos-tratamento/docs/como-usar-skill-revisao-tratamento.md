# Como Usar a Skill de Revisao de Tratamento

Skill versionada no repositorio:

```text
data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment/
```

## Ativar no Codex

Peça explicitamente:

```text
Use a skill mcmv-review-data-treatment para revisar dado a dado o tratamento das tabelas MCMV e gerar evidencias oficiais.
```

## Instalar/Sincronizar Localmente

```bash
mkdir -p ~/.codex/skills
cp -R data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment ~/.codex/skills/
```

## Gerar Evidencias e Relatorio

Execute a partir da raiz de `data-application-cidades`:

```bash
.review-venv/bin/python data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment/scripts/audit_treatment_quality.py --root data-science/dados-historicos-tratamento
.review-venv/bin/python data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment/scripts/render_treatment_review_report.py --root data-science/dados-historicos-tratamento
```

## Gerar Evidencias do Banco

Quando o `.env` do pipeline estiver preenchido e o banco estiver acessivel, gere evidencias linha a linha das discordancias:

```bash
cd data-science/dados-historicos-tratamento
/home/juan-pablo/CIDADES/data-application-cidades/.review-venv/bin/python scripts/revisao_tratamento_db_evidencias.py
cd /home/juan-pablo/CIDADES/data-application-cidades
.review-venv/bin/python data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment/scripts/render_treatment_review_report.py --root data-science/dados-historicos-tratamento
```

Saidas padrao:

- `docs/evidencias/revisao-tratamento-dados/auditoria_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/flags_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/amostras_tratadas_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/impacto_sftp_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/resumo_db_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/discordancias_tratamento_db_pandas.csv`
- `docs/revisao-tratamento-dados.html`
- `docs/revisao-tratamento-dados.pdf`
- `docs/resposta-revisao-tratamento-dados.md`

Nota: a skill registra limitacoes quando `data/treated_tables/`, `data/table_samples/` ou `diff_sftp_minio*.json` nao estiverem disponiveis na branch.
