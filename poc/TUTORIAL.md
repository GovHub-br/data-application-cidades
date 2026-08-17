# Tutorial: dia a dia do desenvolvedor na arquitetura bronze-em-parquet

Este documento não descreve uma ferramenta nova — descreve **o que muda no trabalho de quem
mantém o pipeline** depois que a bronze passa a ser parquet tipado no MinIO (ver
[`README.md`](README.md) para a validação e [`../plan`](.) para a decisão). Ingestão (SFTP →
`raw/`) e mascaramento continuam exatamente como hoje; o que muda começa depois disso.

## O cenário

Chegam em `raw/` três arquivos novos, de uma fonte que nunca foi ingerida antes, em três
formatos diferentes, e que **se relacionam entre si** — cada um descreve um aspecto da mesma
obra e serão unidos na silver por `codigo_obra`:

```
raw/2026/02/OBRAS_CADASTRO_M20260201.xlsx     — endereço, responsável, data de início
raw/2026/02/OBRAS_FINANCEIRO_M20260201.csv    — valores empenhados/pagos
raw/2026/02/OBRAS_STATUS_M20260201.txt        — situação da obra, % de execução
```

Isso é deliberadamente o caso mais chato: três formatos, três encodings/delimitadores
potencialmente diferentes, e uma junção na camada seguinte. É o que aparece de verdade no
lake do MCid (ver `CAIXA_AF_GEHIS_ANDAMENTO_OBRA` no README).

---

## Passo 0 — Ingestão (inalterado)

A DAG de SFTP grava os três arquivos em `raw/`; o mascaramento roda in-place. O desenvolvedor
não faz nada aqui — é o mesmo pipeline de hoje.

## Passo 1 — Rodar o manifesto

```bash
python scripts/gerar_manifesto.py
```

Isso varre `raw/` (ou só os objetos novos, se o script for chamado com um prefixo/lote) e
grava uma linha por arquivo em `_manifesto_bronze`, com o que o DuckDB **não** sabe descobrir
sozinho: encoding, delimitador, se o header é utilizável, quantas colunas tem. Para o `.xlsx`
o manifesto só registra que o schema virá do próprio arquivo (`read_xlsx` lê a planilha
diretamente, sem heurística de encoding/delimitador).

O desenvolvedor consulta o resultado para ver que **três famílias novas** apareceram:

```sql
select familia, formato, count(*), encoding, delimitador, header_ok
from manifesto._manifesto_bronze
where familia like 'obras_%'
group by 1, 2, 4, 5, 6;
```

```
 familia            | formato | count | encoding | delimitador | header_ok
---------------------+---------+-------+----------+-------------+-----------
 obras_cadastro_m    | xlsx    |     1 |          |             | t
 obras_financeiro_m  | csv     |     1 | utf-8    | ;           | t
 obras_status_m      | txt     |     1 | cp1252   | \t          | t
```

Nenhuma das três precisou de intervenção — é o caso feliz. Se alguma tivesse
`tem_bytes_c1 = true` ou `header_ok = false`, seria aqui que o desenvolvedor saberia, antes
de escrever qualquer SQL.

## Passo 2 — Declarar o schema (o único artefato humano)

Para cada família nova, o desenvolvedor escreve um YAML curto em `dbt_poc/tipos/`. É a
mesma decisão que hoje se toma implicitamente ao escrever um model de bronze no Postgres —
só que agora é declarativa e vale para toda a família (todos os arquivos futuros com esse
layout), não por arquivo.

`dbt_poc/tipos/obras_cadastro_m.yml`:
```yaml
familia: obras_cadastro_m
tabela: obras_cadastro
colunas:
  codigo_obra: {tipo: texto}
  municipio: {tipo: texto}
  endereco: {tipo: texto}
  responsavel: {tipo: texto}
  dt_inicio: {tipo: data_br}
```

`dbt_poc/tipos/obras_financeiro_m.yml`:
```yaml
familia: obras_financeiro_m
tabela: obras_financeiro
particao: anomes
colunas:
  anomes: {tipo: integer}
  codigo_obra: {tipo: texto}
  vl_empenhado: {tipo: valor_br}
  vl_pago: {tipo: valor_br}
```

`dbt_poc/tipos/obras_status_m.yml`:
```yaml
familia: obras_status_m
tabela: obras_status
particao: anomes
colunas:
  anomes: {tipo: integer}
  codigo_obra: {tipo: texto}
  situacao: {tipo: texto}
  pct_execucao: {tipo: valor_br}
```

Não há CREATE TABLE, não há migração, não há model boilerplate. `tipo` vem do vocabulário já
existente em `EXPRESSOES` (`scripts/gerar_models_bronze.py`): `texto`, `integer`, `bigint`,
`numeric`, `data_br`, `valor_br`, `apf`, `sn_bool`... Se uma coluna divergir de nome entre
arquivos futuros da mesma família (ex.: `dt_inicio` vira `data_inicio` num extrato mais
novo), a correção é **uma linha** (`alternativas: [data_inicio]`), não um novo model.

## Passo 3 — Gerar os models de bronze

```bash
python scripts/gerar_models_bronze.py --familia obras_cadastro_m
python scripts/gerar_models_bronze.py --familia obras_financeiro_m
python scripts/gerar_models_bronze.py --familia obras_status_m
```

O codegen lê o manifesto (quais arquivos, com que encoding/delimitador cada um) e o YAML (a
semântica), e escreve `dbt_poc/models/bronze/bronze_obras_*.sql`. O desenvolvedor não edita
esse SQL — ele é regenerado sempre que chegam arquivos novos na família. Para
`obras_financeiro_m` (CSV) o gerado é o padrão já validado no README (`read_csv` com
`union_by_name=true`, `null_padding=true`); para `obras_cadastro_m` (XLSX) o `fonte` do model
troca `read_csv(...)` por `read_xlsx(...)` — mesma estrutura de `config`/`select`, sem
encoding/delimitador para declarar.

## Passo 4 — Escrever a bronze

```bash
cd dbt_poc && dbt run --select bronze_obras_cadastro bronze_obras_financeiro bronze_obras_status
```

Três parquets tipados aparecem em `s3://.../bronze/obras_cadastro/`,
`.../obras_financeiro/anomes=202602/`, `.../obras_status/anomes=202602/` — os dois últimos
particionados, pronto para o próximo mês só escrever a partição nova (ver
`resultados/incremental.md`).

## Passo 5 — Expor a bronze ao Postgres

```bash
python scripts/gerar_views_bronze.py --familia obras_cadastro_m
python scripts/gerar_views_bronze.py --familia obras_financeiro_m
python scripts/gerar_views_bronze.py --familia obras_status_m
```

Gera `bronze.obras_cadastro`, `bronze.obras_financeiro`, `bronze.obras_status`: três views
de 0 bytes, com os tipos já corretos no catálogo do Postgres (lidos do footer do parquet).
O desenvolvedor não escreve `CREATE VIEW` à mão.

## Passo 6 — A junção é um model silver comum

É aqui que a arquitetura para de aparecer. O desenvolvedor abre o projeto **dbt-postgres**
de sempre (o `mcid`, ou aqui `dbt_silver/`) e escreve um model igual a qualquer outro:

`models/silver/silver_obras.sql`:
```sql
{{ config(materialized='table') }}

with cadastro as (
    select * from {{ source('bronze', 'obras_cadastro') }}
),
financeiro as (
    select * from {{ source('bronze', 'obras_financeiro') }}
),
status as (
    select * from {{ source('bronze', 'obras_status') }}
)

select
    c.codigo_obra,
    c.municipio,
    c.endereco,
    c.responsavel,
    c.dt_inicio,
    f.anomes,
    f.vl_empenhado,
    f.vl_pago,
    s.situacao,
    s.pct_execucao,
    case
        when s.situacao ilike '%conclu%' then true
        else false
    end as concluida
from cadastro c
left join financeiro f using (codigo_obra)
left join status s using (codigo_obra, anomes)
```

`source('bronze', 'obras_cadastro')` etc. entram em `models/silver/sources.yml`, no mesmo
padrão do exemplo já existente (`dbt_silver/models/silver/sources.yml`). Repare no que este
model **não** tem: nenhum `s3://`, nenhum encoding, nenhum `read_csv`. O `left join` entre
três fontes que vieram de CSV, XLSX e TXT é um `join` comum — a bronze já absorveu a
diferença de formato antes daqui.

## Passo 7 — Rodar a silver

```bash
cd dbt_silver && dbt run --select silver_obras
```

Dialeto Postgres normal. Se `silver_obras` precisasse de um UDF Postgres
(`parse_date_br`, por exemplo) sobre uma coluna que **ainda está como texto**, funcionaria —
a restrição medida (UDFs Postgres falham dentro de uma query que toca a view do pg_duckdb)
só vale para expressões aplicadas *sobre a própria view*, não sobre o resultado já
materializado de outro CTE Postgres puro. Na prática, como a tipagem já aconteceu na
bronze, a silver quase nunca vai precisar disso.

## Passo 8 — Verificação

```sql
select count(*) from bronze.obras_cadastro;
select count(*) from bronze.obras_financeiro;
select count(*) from bronze.obras_status;
select count(*) from silver.silver_obras;          -- <= count(obras_cadastro), por causa do left join
select codigo_obra, count(*) from silver.silver_obras group by 1 having count(*) > 1;  -- não deve haver linha
```

---

## O que muda em relação ao fluxo de hoje

| | hoje | proposta |
|---|---|---|
| detectar encoding/delimitador | `raw_para_staging.py`, por execução | `gerar_manifesto.py`, por família (uma vez, reaproveitado) |
| declarar tipos | implícito no `CAST` de cada model bronze Postgres | YAML explícito por família, em `dbt_poc/tipos/` |
| escrever a bronze | `staging_para_db.py` cria 1 tabela VARCHAR por **arquivo** | `dbt run` escreve 1 parquet tipado por **família**, particionado |
| ler a bronze do Postgres | `SELECT ... FROM sftp."<arquivo>"` (uma tabela por arquivo) | `SELECT ... FROM bronze.<familia>` (uma view por família) |
| escrever a silver | dbt-postgres, `source()` aponta pra tabela VARCHAR | dbt-postgres, `source()` aponta pra view tipada — **o SQL da silver não muda de forma** |
| chegam arquivos novos na mesma família | novo arquivo = nova tabela `sftp.*` | manifesto detecta, `dbt run` sobrescreve só a partição nova (passo 4) |
| chega uma família nova | novo model bronze Postgres, escrito à mão, com `CAST` explícito coluna a coluna | YAML de ~5-10 linhas + `dbt run` (passos 2-5) |

## O que o desenvolvedor nunca escreve à mão

- a lista de colunas do `CREATE VIEW` (gerada do footer do parquet)
- o `read_csv`/`read_xlsx` com encoding e delimitador por arquivo (gerado do manifesto)
- o agrupamento por (encoding, delimitador, header_ok) dentro da família
- SQL para arquivos com header corrompido (o manifesto marca `header_ok=false`; o codegen
  troca para `header=false` + nomes do YAML, automaticamente)

## O que o desenvolvedor decide

- o YAML de tipos por família (o único artefato novo no fluxo)
- a lógica de negócio da silver (joins, dedup, categorização) — **igual a hoje**
- o que fazer com uma família que caiu no bloqueador de encoding (bytes C1): não há
  automação para isso ainda: fica registrada em `_manifesto_bronze.legivel_duckdb = false`
  para decisão manual (ver README, seção do bloqueador)
