# Cobertura documental dos modelos MCID

Critério de documentação dos modelos dbt que entram nas recipes do
OpenMetadata, e o que foi conferido pela última vez.

## Critério de aprovação

Um modelo entra nas recipes de Postgres somente quando atende a todos os itens:

1. possui entrada em um `schema.yml` dbt `version: 2`;
2. possui descrição substantiva do modelo, com pelo menos 80 caracteres;
3. possui tags de camada, domínio e `mcid`;
4. possui `meta.openmetadata` com tier e certificação coerentes com a camada;
5. **prata e ouro**: as colunas documentadas no YAML coincidem exatamente com a
   projeção SQL final, e 100% delas têm descrição não vazia;
6. **bronze**: descrição no nível do MODELO e teste de layout
   `sem_drift_de_colunas` — ver "Por que a bronze é diferente" abaixo;
7. não possui placeholders como `TODO`, `TBD`, `a definir` ou `sem descrição`.

## Por que a bronze é diferente

A bronze é espelho textual do parquet da origem: uma linha por linha recebida,
sem recorte nem tipagem. O que muda com o tempo ali não é o significado de cada
coluna, é o LAYOUT — a origem acrescenta, remove ou renomeia coluna entre
remessas. Por isso o contrato que a bronze declara é a lista de
`sem_drift_de_colunas`: mudou na origem, atualizar a lista é o ato de aceitar a
mudança, e o teste falha enquanto ninguém aceitar.

Documentar coluna a coluna na bronze duplicaria, no vocabulário da origem, o
que a prata já documenta no vocabulário do domínio — e envelheceria a cada
remessa. O catálogo publica a bronze para que a LINHAGEM alcance o início do
caminho, não para consumo (ver `dbt/mcid/governance/schemas.yml`); quem consome
lê prata e ouro, e é lá que a descrição por coluna tem leitor.

O cabeçalho de `models/conjuntura_dbt/bronze/schema.yml` registra a mesma regra
pelo lado do conteúdo: a descrição documenta O DADO — que fenômeno a origem
mede, o que é uma linha, em que unidade e de que período —, não por onde o dado
passou.

## Como reconferir

O item 5 é o que silenciosamente apodrece: o bloco `columns:` existe, parece
completo, e cobre só parte da projeção do SQL. Foi assim que a
`ouro_pro_moradia_ficha_contrato` ficou com 21 das 59 colunas documentadas sem
que nada acusasse.

Contar entradas do YAML não detecta isso — é preciso comparar com a projeção
final de cada model. O script `scripts/governance/conferir_colunas.py` faz essa
comparação e imprime, por model, o que está no SQL e não no YAML (e vice-versa).

```
python scripts/governance/conferir_colunas.py
```

## Estado conferido

### 2026-10-06 — perguntas do MCID (Rural, FNHIS Sub-50, Pró-Moradia)

Modelos novos das perguntas da oficina com o MCID e das réplicas do Rural
(`models/docs/entregas/perguntas-mcid-rural-fnhis-pro-moradia.md`), conferidos com
`conferir_colunas.py` antes do primeiro `dbt build`:

| Produto | Camada | Modelos | Colunas |
|---|---|---:|---:|
| `empreendimento_rural` | prata | 4 | 60/60 |
| `empreendimento_rural` | ouro | 5 | 112/112 |
| `fnhis` | prata | 1 | 12/12 |
| `fnhis` | ouro | 5 | 99/99 |
| `pro_moradia` | ouro | 5 | 114/114 |
| **Total** | | **20** | **397/397** |

Bronze: `bronze_sftp_gehis_andamento_obra`, `bronze_sftp_portaria_162_2018_rural` e
`bronze_shpt_fnhis_sub50_termos_compromisso_serie`, com descrição de modelo e
`sem_drift_de_colunas`. Seeds `perguntas_mcid/*` documentados em `seeds/perguntas_mcid/schema.yml`.
`ouro_rural_obra_situacao_mensal` e `ouro_fnhis_motivos_nao_enquadramento` saem como
INDETERMINADO no script (UNION em CTE); conferidos na mão: 8 e 13 colunas.

### 2026-09-30 — `pro_moradia` e `fnhis`

| Produto | Camada | Modelos | Colunas |
|---|---|---:|---:|
| `pro_moradia` | prata | 5 | 78/78 |
| `pro_moradia` | ouro | 4 | 102/102 |
| `fnhis` | prata | 4 | 91/91 |
| `fnhis` | ouro | 3 | 89/89 |
| **Total** | | **16** | **360/360** |

178 dessas descrições foram escritas nessa data: os dois produtos tinham bloco
`columns:` em todos os models, mas ele cobria só parte da projeção — 190 das 360
colunas estavam sem descrição no catálogo.

Bronze dos dois produtos (14 models em `pro_moradia_dbt`, 3 em `fnhis_dbt`):
descrição de modelo e `sem_drift_de_colunas` presentes, conforme o item 6.

### 2026-07-19 — auditoria do layout anterior

Registro histórico: a auditoria abaixo é anterior à reorganização dos schemas
físicos. Ela descreve `empreendimento_far`, `entidades_fds` e
`conjuntura_gold`, que não existem mais — os models passaram a materializar por
CAMADA, em `bronze`, `prata` e `ouro`. Os números não valem para o estado atual
e estão aqui só como marco.

| Schema físico | Modelos auditados | Aprovados | Colunas documentadas |
|---|---:|---:|---:|
| `conjuntura` | 11 | 11 | 99/99 |
| `conjuntura` | 19 | 19 | 224/224 |
| `conjuntura_gold` | 20 | 20 | 200/200 |
| `empreendimento_far` | 13 | 13 | 308/308 |
| `entidades_fds` | 11 | 11 | 384/384 |
| `metadata` | 1 | 0 | 0/7 |
| **Total** | **75** | **74** | **1.215/1.222** |

Naquele momento o critério era "100% das colunas" para toda camada, inclusive a
bronze. A regra do item 6 acima substitui isso e é a que as bronzes do repo
seguem hoje.

## Modelo excluído

`metadata.models_metadata` continua fora das três recipes porque não possui
entrada em `schema.yml`, descrição de modelo, tags, `meta.openmetadata` nem
documentação para suas sete colunas: `schema_name`, `table_name`,
`database_name`, `materialization`, `description`, `dt_transform` e `run_id`.

## Pendente

- Reconferir `far_dbt`, `fds_dbt`, `rural_dbt`, `conjuntura_dbt` e
  `mcmv_historico_dbt` com `conferir_colunas.py`: o item 5 nunca foi verificado
  neles sob o layout atual, e foi exatamente o que estava quebrado em
  `pro_moradia` e `fnhis`.
- As allowlists de modelos aprovados vivem em `postgres_metadata.yaml`,
  `postgres_profiler.yaml` e `postgres_classifier.yaml` e devem permanecer
  idênticas entre si.
