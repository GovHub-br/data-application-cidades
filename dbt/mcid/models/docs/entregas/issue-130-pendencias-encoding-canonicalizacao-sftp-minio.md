# Issue #130 — Pendencias: Encoding e Canonicalizacao das Bases SFTP no MinIO

## Resumo

Durante a exploracao dos dados historicos (issues #118 e #130), comparamos as
amostras locais (`data-science/dados-historicos-tratamento/data/sftp_tratado`)
com as bases ja presentes no MinIO (`data-lake-mcid`). Conclusao: o MinIO ja e
mais completo e deve ser a fonte de dados. Ficaram duas correcoes para
implementar posteriormente — encoding quebrado no `staging/` e ausencia de
canonicalizacao (`gefus_*` / `_canonicas.csv`).

## Evidencia: MinIO mais completo que as amostras locais

| Interface | Frente | Snapshots locais (raw) | Snapshots MinIO raw |
|---|---|---|---|
| int057 | PNHR BB empreendimentos | 26 | 64 |
| int065 | PNHR CAIXA empreendimentos | 20 | 58 |
| int040 | FAR CAIXA empreendimentos | 1 | 63 |
| int054 | FAR BB empreendimentos | 23 | 59 |
| int059 | FDS CAIXA empreendimentos | 1 | 72 |
| pmcmv_faixa3 | Classe Media / Faixa 3 | ~ | 62 |
| pmcmv_reformas | Reforma Casa Brasil | ~ | 43 |
| pmcmv_cidades | MCMV Cidades | ~ | 7 |

Fatos:

- O MinIO `raw/sftp/fabrica/GEFUS/ANTERIORES/` preserva a serie mensal completa
  (2019-12 a 2024) em arquivos originais `.txt/.xlsx/.csv`
  (ex.: `INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS_20191230.TXT`).
- O MinIO `staging/sftp/` ja tem parquet tratado (~1.500 tabelas).
- As amostras locais (`table_samples`) sao 200 linhas por tabela e estao
  incompletas (ex.: int040 e int059 tem 1 snapshot local contra 63/72 no MinIO).

## Decisao

1. Dados -> MinIO (`raw/` + `staging/`), fonte autorizada.
2. Tratamento/canonicalizacao -> reaproveitar o pipeline local
   (`dados-historicos-tratamento`) rodando contra o `raw/` completo do MinIO,
   nao contra as amostras locais.

## Pendencias a implementar

### P1 — Corrigir encoding (mojibake) no `staging/`

O tratamento "fabrica" do MinIO gera nomes de coluna corrompidos. Exemplos
reais observados em
`staging/sftp/fabrica/GEFUS/202601_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.parquet`:

| Observado | Esperado |
|---|---|
| `idata_de_movimento` | `data_de_movimento` |
| `municapio` | `municipio` |
| `ca3digo_ibge_do_municapio` | `codigo_ibge_do_municipio` |
| `situaaao_do_empreendimento` | `situacao_do_empreendimento` |
| `data_de_contrataaao` | `data_de_contratacao` |
| `observaaaes` | `observacoes` |
| `logradouro_do_ima3vel` | `logradouro_do_imovel` |
| `naomero_do_ima3vel` | `numero_do_imovel` |
| `complemento_do_endereao_do_ima3vel` | `complemento_do_endereco_do_imovel` |
| `quantidade_de_uhs_..._de_referaancia` | `..._de_referencia` |

A correcao e sistematica (acentos/caracteres especiais viram sequencias `a3`,
`aa`, `aaao`, `ao`), tipico de dupla decodificacao de encoding. Se os modelos
dbt lerem do `staging/` com esses nomes, quebram as referencias de coluna.

Observacao: o mojibake atinge tabelas cujas colunas de origem tem acentos
(ex.: SNH dados prioritarios — `situacao`, `municipio`, `numero do imovel`).
As tabelas de interface/empreendimento (`int040`/`int054`/`int059`, GEAVO) usam
nomes ASCII (`nu_apf`, `cod_municipio_ibge`, `situacao_obra`) e estao limpas no
`staging/`.

Acoes:

- [ ] Inventariar quais tabelas do `staging/` estao com mojibake.
- [ ] Definir funcao de normalizacao de nomes de coluna (mapa de substituicoes).
- [ ] Regenerar `staging/` a partir do `raw/` com encoding correto (preferido),
      ou corrigir in-place.

### P2 — Adicionar canonicalizacao (`gefus_*` / `_canonicas.csv`)

O MinIO nao tem a canonicalizacao por hash que o pipeline local produz
(`gefus_*` com sufixo `_000N` + mapeamento `table_name -> canonical_table` em
`_canonicas.csv`). Isso e necessario para:

- deduplicar snapshots de conteudo identico;
- estabilizar a chave de empreendimento ao longo da serie (grao APF x mes);
- alimentar a serie historica do reloginho (#130) e a futura analise preditiva
  de paralisacao/atraso de obra.

Acoes:

- [ ] Rodar a canonicalizacao sobre os snapshots completos do MinIO (63/72 por
      interface), nao sobre as amostras locais.
- [ ] Gerar `_canonicas.csv` global (tabela -> tabela canonica).

### P3 — Temporalidade via `dt_movimento`

A serie historica dos empreendimentos vive na coluna `dt_movimento` (um
snapshot por mes), nao no nome do arquivo. Qualquer modelo historico deve usar
`dt_movimento` como chave temporal; a canonicalizacao perde a data no nome do
arquivo (usa sufixo `_000N`).

## Fronts que dependem do SFTP historico

As frentes abaixo ainda nao estao completas na silver (leem do `raw` Postgres
`novo_mcmv_*` em vez do `staging/sftp`):

| Frente | Interfaces SFTP disponiveis | Snapshots MinIO raw |
|---|---|---|
| FAR | int040 (CAIXA), int054 (BB) | 63 + 59 |
| Entidades/FDS | int059 (CAIXA) | 72 |
| Rural/PNHR | int057 (BB), int065 (CAIXA) | 64 + 58 |

## Observacao

Nao incluir credenciais MinIO (`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`) em commit.
Usar `.env`.
