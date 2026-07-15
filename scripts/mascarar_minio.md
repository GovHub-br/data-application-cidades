# `mascarar_minio.py` — Mascaramento de PII no data lake (MinIO)

Mascara dados pessoais de pessoa física (PII) na camada `raw/` do data lake no MinIO,
**sobrescrevendo o objeto no lugar**. O objetivo é que o raw deixe de conter PII.

O `raw/` guarda os arquivos exatamente como chegam do SFTP (CSV, TXT, XLSX), em formatos,
delimitadores e encodings heterogêneos. Este script percorre cada objeto, detecta as colunas
sensíveis pelo header e mascara os valores, preservando byte a byte todo o resto.

---

## 1. O que é mascarado

A detecção é por **nome de coluna** (header), reaplicando os padrões do mapeamento feito no
schema `sftp` do Postgres.

| Categoria | Técnica | Exemplos de coluna |
|---|---|---|
| **CPF** | HMAC-SHA256 determinístico | `NU_CPF_CGC_MUTUARIO`, `NU_CPF_BENEFICIARIO`, `txt_cpf` |
| **NIS / PIS / PASEP / NIT** | HMAC-SHA256 determinístico | `NU_PIS`, `NU_PIS_PASEP`, `NU_NIS` |
| **Nome de PF** | Redação (`***`) | `NO_MUTUARIO`, `NO_BENEFICIARIO`, `nomeMutuario` |
| **Data de nascimento** | Redação (`***`) | `DT_NASCIMENTO`, `DT_NASCIMENTO_MUTUARIO` |
| **Endereço** | Redação (`***`) — *condicional* | `NO_LOGRADOURO_IMOVEL`, `NO_BAIRRO_IMOVEL`, `NO_COMPLEMENTO_IMOVEL` |
| **CEP** | Redação (`***`) — *condicional* | `ED_CEP_IMOVEL_GARANTIA`, `NU_CEP`, `CO_CEP_IMOVEL` |

### Regras importantes

- **HMAC determinístico**: o mesmo CPF/PIS gera sempre o mesmo token em qualquer arquivo.
  Isso remove o dado real mas preserva a capacidade de **cruzar bases e contar pessoas
  distintas**. É irreversível sem o segredo. (Ver seção "Por que HMAC e não hash simples".)
- **CEP/endereço são condicionais**: só são mascarados quando o arquivo tem **algum
  indicador de PF na mesma linha** (CPF, NIS, nascimento ou nome de PF). Em bases de
  PJ/empreendimento (ex. `Base_PJ_FGTS_*`, arquivos de empreendimentos), o CEP é do imóvel
  e **é preservado**.
- **Não é mascarado**: CNPJ/CGC (é PJ), `razao_social_*`, `nome_empreendimento`,
  `email_entidade`, município/UF (geografia grossa), e colunas de papel que não são nome
  (`CO_SEXO_BENEFICIARIO`, `VR_PARCELA_MENSAL_BENEFICIARIO`, flags como `TITULAR-COM-PDC`).
- **Valores vazios continuam vazios** (não viram token nem `***`).

---

## 2. Como funciona (arquitetura)

Para cada objeto em `raw/`:

1. **Amostra** dos primeiros 64 KB (via HTTP Range) → detecta **encoding** (utf-8 ou cp1252)
   e **delimitador/dialeto** (`;`, `|`, `\t`, `,`) — via `lake_utils.py`.
2. **Download** para um tempfile (em `MASKING_TMPDIR`, não em `/tmp`).
3. **Mascaramento**:
   - **CSV/TXT**: streaming linha a linha (memória constante mesmo em arquivos de GBs).
   - **XLSX**: pré-scan barato do header em modo `read_only`; só carrega o workbook completo
     na RAM se houver PII a mascarar.
4. **Verificação round-trip** (CSV/TXT): confere que nº de linhas e nº de colunas do header
   foram preservados. Se divergir → status `error`, **não sobrescreve**.
5. **Sobrescreve** o objeto em `raw/` (só com `--apply`) e marca a tag `masked=true`.
6. **Auditoria**: registra o resultado no parquet e na tabela de controle do Postgres.

### Transporte byte-preserving (por que não corrompe acentos)

O arquivo é lido e reescrito em **latin-1** (mapeia todos os 256 bytes 1:1), então qualquer
coluna **não mascarada** é gravada byte-idêntica, independentemente do encoding real do
arquivo. Só as colunas-alvo são trocadas por texto ASCII (token/`***`). O encoding real é
detectado apenas para interpretar corretamente os **nomes** das colunas no matching.

### Idempotência (não re-mascara)

Objetos já mascarados recebem a tag `masked=true` e são pulados em execuções seguintes —
evita o **duplo-HMAC** (aplicar HMAC sobre um token já gerado). Use `--force` para reprocessar.

---

## 3. Configuração (`.env`)

| Variável | Obrigatória | Descrição |
|---|---|---|
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` | sim | Conexão MinIO |
| `DB_DW_HOST_MCID`, `DB_DW_PORT_MCID`, `DB_DW_USER_MCID`, `DB_DW_PASSWORD_MCID`, `DB_DW_DBNAME_MCID` | sim | Postgres (tabela de controle) |
| `MASKING_HMAC_SECRET` | **sim** | Segredo do HMAC. Sem ela o script recusa rodar. |
| `MASKING_TMPDIR` | recomendado | Dir de temporários. **Não use `/tmp`** se for tmpfs (RAM). Precisa ~5 GB livres p/ os arquivos de ~2 GB. |
| `SFTP_SCHEMA` | não (default `sftp`) | Schema da tabela de controle |
| `MASKING_PREFIX` | não (default `raw/`) | Prefixo a varrer |
| `MASKING_TOKEN_LEN` | não (default `16`) | Tamanho do token HMAC (hex chars) |
| `MASKING_REDACTION` | não (default `***`) | Valor da redação |

> ⚠️ **`MASKING_HMAC_SECRET` nunca deve ser alterado depois do primeiro mascaramento.**
> Trocar o segredo muda todos os tokens e quebra os joins entre execuções. Ele também é o
> que garante a irreversibilidade — trate-o como credencial (não commitar, não vazar).

### Dependências
```bash
VIRTUAL_ENV=.venv uv pip install "pyarrow>=15.0.0" boto3
# já usados pelo projeto: pandas, psycopg2, openpyxl, python-dotenv
```

O script importa `lake_utils.py` (módulo compartilhado com `raw_para_staging.py`: detecção de
encoding/dialeto, normalização de header, hash e helpers S3), então rode-o com `scripts/`
acessível (`.venv/bin/python scripts/mascarar_minio.py`).

---

## 4. Uso

Por padrão roda em **DRY-RUN**: não sobrescreve `raw/`, grava a prévia em `masked_dryrun/`
e gera auditoria. Use `--apply` para efetivar.

```bash
# dry-run de tudo
.venv/bin/python scripts/mascarar_minio.py

# dry-run fatiado por formato
.venv/bin/python scripts/mascarar_minio.py --only-ext xlsx
.venv/bin/python scripts/mascarar_minio.py --only-ext csv
.venv/bin/python scripts/mascarar_minio.py --only-ext txt --max-size-mb 50

# um arquivo específico (reprocessa mesmo se já mascarado)
.venv/bin/python scripts/mascarar_minio.py --pattern PMCMV_FAIXA3 --force

# EFETIVAR (sobrescreve raw/) — recomendado fatiar, menores primeiro
.venv/bin/python scripts/mascarar_minio.py --apply --only-ext xlsx
.venv/bin/python scripts/mascarar_minio.py --apply --only-ext csv
.venv/bin/python scripts/mascarar_minio.py --apply --only-ext txt --max-size-mb 50
.venv/bin/python scripts/mascarar_minio.py --apply --only-ext txt   # inclui os ~2 GB
```

### Flags

| Flag | Efeito |
|---|---|
| `--apply` | Sobrescreve `raw/` no lugar. Sem ela = dry-run (prévia em `masked_dryrun/`). |
| `--force` | Reprocessa objetos já com tag `masked=true`. |
| `--limit N` | Processa no máximo N objetos. |
| `--pattern STR` | Só objetos cuja key contém `STR`. |
| `--only-ext csv,txt` | Restringe às extensões dadas. |
| `--max-size-mb N` | Pula objetos maiores que N MB (fatiar: pequenos primeiro). |
| `--prefix P` | Prefixo a varrer (default `raw/`). |

---

## 5. Status de saída (por objeto)

| Status | Significado |
|---|---|
| `masked` | Mascarado e sobrescrito em `raw/` (só com `--apply`). |
| `dry_run` | Tinha PII; prévia gravada em `masked_dryrun/` (sem `--apply`). |
| `skipped_no_pii` | Sem coluna sensível (bases de empreendimento/PJ). Esperado. |
| `skipped_no_header` | Não é tabular / sem delimitador detectável. |
| `skipped_already` | Já mascarado (tag `masked=true`). |
| `skipped_unsupported` | Formato não suportado (`.xls`, `.zip`) — **tratar à parte**. |
| `error` | Falhou; **o objeto NÃO foi sobrescrito**. Investigar e re-rodar com `--pattern`. |

Ao final, o log imprime a contagem por status. **`error` deve ser 0.**

---

## 6. Auditoria e linhagem

**Parquet** por execução, em `audit/masking/execution_id=<uuid>/part-0.parquet` no MinIO
(+ cópia local `scripts/auditoria_mascaramento_<uuid>.parquet`). Colunas:
`execution_id, timestamp, bucket, file, file_name, file_format, encoding, delimiter,
has_pf_indicator, has_formulas, masked_columns, registros_total, registros_alterados,
hash_before, hash_after, status, error_message, duration_s`.

**Tabela de controle** `sftp._masking_log` no Postgres (criada automaticamente), com
`UNIQUE(minio_key, source_hash)`. Consulta útil:
```sql
SELECT status, count(*), sum(registros_alterados)
FROM sftp._masking_log
GROUP BY status;
```

---

## 7. Formatos e limitações conhecidas

- **CSV / TXT**: totalmente suportados (streaming). Delimitador e encoding detectados por
  arquivo; headers com quebra de linha dentro de aspas são tratados.
- **XLSX**: suportado. **Fórmulas**: o `openpyxl` preserva a fórmula mas perde o *valor
  em cache* ao salvar — leitores programáticos (pandas/`data_only`) podem ver `None` nessas
  células até o arquivo ser reaberto e resalvo num Excel real. Sinalizado por `has_formulas`
  na auditoria + warning no log.
- **`.xls` (Excel legado) e `.zip`**: `skipped_unsupported` — precisam de tratamento manual
  (são poucos e pequenos).
- **`~$*.xlsx`**: arquivos de lock temporário do Excel — ignorados automaticamente.

---

## 8. Por que HMAC e não hash simples

Um hash simples (`sha256(cpf)`) de um CPF é **reversível na prática**: o espaço de CPFs é
pequeno (~10¹¹, e menos ainda com dígito verificador), então dá para testar todos os
candidatos numa GPU em minutos e casar os hashes ("ataque de dicionário"). O **HMAC** mistura
um **segredo** no cálculo — sem a chave, essa busca exaustiva é inviável. E, sendo
determinístico, mantém o mesmo token para o mesmo CPF entre bases, preservando joins.
É o padrão correto para identificadores estruturados de baixa entropia (CPF, PIS, telefone).

---

## 9. Recomendações operacionais

- **Sempre rode dry-run antes do `--apply`** e confira a auditoria + amostras.
- **Fatiar o `--apply`** por formato/tamanho (menores primeiro); se algo falhar no meio,
  o que já foi feito não se perde (idempotência por tag).
- Em máquina com **pouca RAM**, garanta `MASKING_TMPDIR` num disco com espaço e rode os
  lotes grandes (`txt` sem `--max-size-mb`) fora de horário de uso pesado. O footprint de
  RAM do script é pequeno (streaming), mas os arquivos grandes têm I/O intenso.
- `--apply` é **destrutivo e irreversível** (os bytes originais com PII são perdidos).
  A verificação round-trip roda antes de cada sobrescrita como salvaguarda.
```
