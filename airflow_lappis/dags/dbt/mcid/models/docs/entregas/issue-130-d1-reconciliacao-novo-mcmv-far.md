# Issue #130 — D1: Reconciliacao SFTP x Novo MCMV e Mapeamento do novo_mcmv_far

## Resumo

Decisao D1 (opcao B): manter as duas fontes de empreendimento MCMV — o SFTP
(interface Ministeriocidades, legado) e o `novo_mcmv_*` (SharePoint/GFAR, Novo
MCMV) — e reconciliar a sobreposicao. Este documento registra a analise de
sobreposicao e o mapeamento de colunas do `novo_mcmv_far` para o contrato comum
do modelo historico/snapshot.

## Analise de Sobreposicao

### Cobertura temporal

| Fonte | FAR | FDS |
|---|---|---|
| SFTP (`int040`/`int054`, `int059`) | 2019-12 .. 2024-11 | 2019-12 .. 2026-06 |
| `novo_mcmv_*` (SharePoint) | 2024-05 .. 2026-02 | 2025-12 .. 2026-01 |

### Sobreposicao de APF

| Frente | SFTP | novo_mcmv | Interseccao |
|---|---|---|---|
| FAR | 4.462 (CAIXA) + 202 (BB) | 822 | **0** |
| FDS | 876 | 335 | **304** (~91% do novo) |

## Conclusao da Reconciliacao

- **FAR — disjunto (continuacao temporal).** `int040`/`int054` (legado, 2019-2024)
  e `novo_mcmv_far_*` (Novo MCMV/GFAR, 2024+) sao programas diferentes, sem APF
  em comum. `UNION` puro, sem dedup. A transicao ocorre em ~2024 (legado termina
  em 2024-11; Novo comeca em 2024-05).
- **FDS — sobreposto.** `int059` e `novo_mcmv_fds_*` compartilham 304 APFs.
  Preferir `int059` (serie historica completa 2019-2026) sobre `novo_mcmv_fds`
  (snapshot corrente, sem serie); o `novo_mcmv_fds` so adiciona ~31 APFs
  exclusivos.

## Estrutura da Fonte novo_mcmv_far

O estado corrente do empreendimento FAR do Novo MCMV esta dividido em dois
arquivos (CSV em `raw/sharepoint/`), com JOIN 1:1 em `nu_apf` (822 = 822 = 822):

| Arquivo | Linhas | Papel |
|---|---|---|
| `novo_mcmv_far_cad_pj_mensal.csv` | 822 | cadastro (identidade, municipio, UH, valores, datas) |
| `novo_mcmv_far_obra_mensal.csv` | 822 | status da obra (percentual, situacao, entregues, entrega) |

Ambos sao snapshot unico (`dt_movimento = 2026-02-01`).

## Mapeamento novo_mcmv_far -> Contrato Comum

| Contrato | `cad_pj_mensal` | `obra_mensal` | Nota |
|---|---|---|---|
| `frente_mcmv` | `'FAR'` (literal) | | |
| `linha_mcmv` | `'Novo MCMV FAR'` (literal) | | distingue do legado |
| `agente_financeiro` | `no_agente_financeiro` | | ex.: `CAIXA` |
| `apf` / `codigo_empreendimento` | `nu_apf` | | |
| `nome_empreendimento` | `no_empreendimento` | | |
| `codigo_ibge_municipio` | `co_municipio_ibge` | | int (7 digitos) |
| `municipio` | `no_municipio` | | |
| `uf` | `sg_uf` | | |
| `responsavel_id` | `nu_cnpj_construtora` | | construtora (aprox.) |
| `responsavel_nome` | `no_construtora` | | |
| `quantidade_uh` | `nu_qt_uh` | | int |
| `quantidade_uh_entregues` | | `qt_uh_alienada` | ver issue de qualidade |
| `valor_contratado` | `vr_total_investimento` | | formato BR |
| `valor_desembolsado` | — | — | gap: so em `financeiro_mensal` |
| `percentual_execucao_fisica` | | `pc_obra_realizada` | formato BR |
| `status_operacional` | | `co_situacao_obra` | codigo (1,2,3,5,11,16) |
| `dt_contratacao` | `dt_contratacao` | | DATE (auto-parsed) |
| `dt_inicio_obra` | `dt_inicio_obra` | | DD/MM/YYYY (string) |
| `dt_entrega` | | `dt_entrega_do_empreendimento` | ver issue de qualidade |
| `dt_referencia` | `dt_movimento` | | nao ha data no filename |
| `dt_movimento` | `dt_movimento` | | `2026-02-01` |

## Issues de Qualidade a Tratar no SQL

1. **`None` literal (nao NULL).** O CSV foi gerado de DataFrame Python e gravou
   `None` como string. Ocorre em `qt_uh_concluidas`, `qt_uh_alienada`,
   `dt_entrega_do_empreendimento`, `dt_paralisacao`, etc. Tratar com
   `nullif(nullif(trim(x), ''), 'None')`.
2. **Data em formato brasileiro.** `dt_inicio_obra = '29/05/2024'` (DD/MM/YYYY)
   e `dt_contratacao` ja vem como DATE (ISO). Tratar com
   `try_cast(x as date)` + fallback `strptime(x, '%d/%m/%Y')`.
3. **`co_situacao_obra` e codigo.** Frequencia: `11` (354), `2` (201), `1` (164),
   `3` (85), `16` (11), `5` (7). Falta a tabela de dominio codigo -> descricao;
   manter o codigo ate definir o lookup.
4. **`valor_desembolsado` ausente** no grao empreendimento — so em
   `financeiro_mensal` (6.069 liberacoes); exigiria agregacao por APF.

## Decisao Estrutural

- `novo_mcmv_far` e **snapshot corrente unico** (nao ha serie mensal em
  `cad_pj`/`obra`; apenas `consolidado` tem 209 meses e `financeiro_mensal` 157).
- Portanto, entra no **snapshot corrente** (`snapshot_mcmv_empreendimentos_atual`,
  D2), nao no modelo historico.
- `dt_referencia = dt_movimento` (nao filename); `fonte_tabela = 'novo_mcmv_far'`.
- Leitura via `read_csv_auto` (CSV em `raw/sharepoint/`), nao parquet/`staging/`.

## Merge no Snapshot Corrente (D1, opcao B)

```text
FAR corrente = [legado SFTP int040/int054 (ultimo mes)]
               UNION
               [Novo MCMV FAR (cad_pj_mensal JOIN obra_mensal ON nu_apf)]
```

Disjuntos (0 APF em comum) -> `UNION` puro, sem dedup.

## Observacao

Nao incluir credenciais MinIO em commit. Usar `.env`.
