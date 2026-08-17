# Issue #119 — Ajuste das Frentes Faltantes na Silver

## Resumo

Foi revisado o status das frentes que ainda estavam sem dados na primeira
padronizacao da camada `mcmv_silver`: `SUB50/FNHIS` e `Pro-Moradia`.

## Resultado

| Frente | Status apos ajuste | Observacao |
|---|---|---|
| SUB50/FNHIS | Modelo silver ajustado | Fontes localizadas no inventario MinIO e esperadas em `__dados_brutos`. |
| Pro-Moradia | Permanece como lacuna | Nenhuma fonte confiavel encontrada nos inventarios locais ou nomes conhecidos. |

## SUB50/FNHIS

O modelo `silver_mcmv_sub50_base` deixou de ser apenas placeholder e passou a
materializar as fontes abaixo quando elas existirem no schema `__dados_brutos`:

- `novo_mcmv_fnhis_sub_50_propostas_apresentadas`
- `novo_mcmv_fnhis_sub_50_propostas_selecionadas`

As evidencias inventariadas apontam:

- 7.121 propostas apresentadas.
- 1.207 propostas selecionadas.
- 8.328 registros esperados no total das fontes FNHIS/SUB50.

Campos padronizados na silver:

- `programa`
- `frente_mcmv`
- `grupo_linha`
- `linha_mcmv`
- `contrato`
- `codigo_empreendimento`
- `codigo_ibge_municipio`
- `municipio`
- `uf`
- `responsavel_tipo`
- `responsavel_id`
- `responsavel_nome`
- `quantidade_uh`
- `valor_contratado`
- `valor_desembolsado`
- `status_operacional`
- `dt_referencia`
- `dt_contratacao`
- `dt_ultima_atualizacao`
- `dt_silver`

## Pro-Moradia

Pro-Moradia permanece sem dado materializado. Foram procurados nomes e variacoes
como:

- `pro_moradia`
- `promoradia`
- `pro moradia`
- `moradia`

Nenhuma fonte confiavel foi localizada nos inventarios disponiveis. O modelo
`silver_mcmv_pro_moradia_base` segue como contrato vazio para preservar a
estrutura da silver ate a fonte oficial ser identificada.

## Validacao

Validacoes executadas localmente:

- `dbt parse --profiles-dir .`: passou.
- `git diff --check`: passou.

Validacao pendente:

- `dbt run --select silver_mcmv_sub50_base`: pendente por timeout de conexao com
  o Postgres `10.0.0.50:5432` a partir deste terminal.

## Arquivos alterados

- `models/mcmv_silver_dbt/silver/sub50/silver_mcmv_sub50_base.sql`
- `models/mcmv_silver_dbt/silver/schema.yml`
- `models/docs/evidencias/issue-119-mcmv-silver-frentes.csv`
- `models/docs/evidencias/issue-119-mcmv-silver-fontes-validacao.csv`
- `models/docs/issue-119-padrao-silver-marts-dashboard.md`
- `models/docs/entregas/issue-119-entrega.md`
