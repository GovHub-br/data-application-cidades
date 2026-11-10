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
| **Nome de PF** | Redação (`***`) | `NO_MUTUARIO`, `NO_BENEFICIARIO`, `nomeMutuario`, `NO_ENTREVISTADOR_FAM` |
| **Documento** | Redação (`***`) | `NU_IDENTIDADE_PESSOA`, `NU_TITULO_ELEITOR_PESSOA`, `CO_MATRICULA_CERTIDAO` |
| **Endereço** | Redação (`***`) — *condicional* | `NO_LOGRADOURO_IMOVEL`, `NO_BAIRRO_IMOVEL`, `NO_COMPLEMENTO_IMOVEL` |
| **CEP** | Redação (`***`) — *condicional* | `ED_CEP_IMOVEL_GARANTIA`, `NU_CEP`, `CO_CEP_IMOVEL` |
| **Texto livre** | Redação (`***`) — *condicional* | `DS_OBS_ENTREVISTADOR_FAM`, `DS_REFERENCIA_LOCAL_FAM`, `Observações Gerais` |

### Regras importantes

- **HMAC determinístico**: o mesmo CPF/PIS gera sempre o mesmo token em qualquer arquivo.
  Isso remove o dado real mas preserva a capacidade de **cruzar bases e contar pessoas
  distintas**. É irreversível sem o segredo. (Ver seção "Por que HMAC e não hash simples".)
- **CEP, endereço e texto livre são condicionais**: só são mascarados quando o arquivo tem
  **algum indicador de PF na mesma linha** (CPF, NIS, nascimento ou nome de PF). Em bases
  de PJ/empreendimento (ex. `Base_PJ_FGTS_*`, arquivos de empreendimentos), o CEP é do
  imóvel e a observação é da obra: **são preservados**. Texto livre é observação
  (`obs`, `observacao`) e ponto de referência do domicílio; `MES_REFERENCIA` e afins não
  entram.
- **Não é mascarado**: CNPJ/CGC (é PJ), `razao_social_*`, `nome_empreendimento`,
  `email_entidade`, município/UF (geografia grossa), e colunas de papel que não são nome
  (`CO_SEXO_BENEFICIARIO`, `VR_PARCELA_MENSAL_BENEFICIARIO`, flags como `TITULAR-COM-PDC`).
- **Atributos de perfil preservados**: data de nascimento, sexo, raça/cor, etnia,
  deficiência e PCD (`DT_NASCIMENTO`, `CO_RACA_COR_PESSOA`, `IC_BENEF_PCD`...) não
  identificam a pessoa sozinhos e respondem perguntas de perfil dos beneficiários. Não são
  mascarados, mas contam como indicador de PF para a regra de CEP/endereço. A camada ouro
  publica só agregados.
- **Valores vazios continuam vazios** (não viram token nem `***`).

---

## 2. Como funciona (arquitetura)

Para cada objeto em `raw/`:

1. **Amostra** dos primeiros 64 KB (via HTTP Range) → detecta **encoding** (utf-8 ou cp1252)
   e **delimitador/dialeto** (`;`, `|`, `\t`, `,`) — via `lake_utils.py`.
2. **Leitura**:
   - **CSV/TXT**: direto do MinIO, em streaming, sem tocar o disco. A saída sobe em
     multipart para a mesma key; o objeto só é trocado depois da verificação, e qualquer
     falha aborta o upload e mantém o original.
   - **XLSX/MDB**: download para um tempfile (em `MASKING_TMPDIR`, não em `/tmp`).
3. **Mascaramento**:
   - **CSV/TXT**: linha a linha (memória constante mesmo em arquivos de dezenas de GB).
   - **XLSX**: pré-scan barato do header em modo `read_only`; só carrega o workbook completo
     na RAM se houver PII a mascarar.
   - **MDB/ACCDB**: **somente verificação** — ver "Bases Access (.mdb)" abaixo.
4. **Verificação round-trip** (CSV/TXT): a saída é reparseada em lotes de 10 mil linhas,
   que precisam voltar idênticas ao que foi escrito. Se divergir → status `error`, **não
   sobrescreve**.
5. **Sobrescreve** o objeto em `raw/` (só com `--apply`) e marca a tag `masked=true`.
6. **Auditoria**: registra o resultado no parquet e na tabela de controle do Postgres.

### CSV/TXT grande: modo paralelo

O mascaramento de CSV/TXT é limitado por CPU, e um processo Python usa um núcleo só. Arquivo
de 1 GB ou mais, **sem aspas** e com PII é dividido em pedaços de ~2 GB, cortados logo após
um `\n`, e cada pedaço é mascarado num processo:

- Sem aspas, todo `\n` fecha um registro, então qualquer fim de linha é um corte seguro.
  Com aspas um campo pode conter quebra de linha; por isso, se algum pedaço tiver uma aspa,
  tudo é descartado e o arquivo segue pelo modo sequencial.
- Cada pedaço vira um objeto em `tmp/masking/`, com um `.json` ao lado marcando que está
  completo. No fim, o MinIO concatena os pedaços na key original sem baixar nada
  (`upload_part_copy`) e os temporários são apagados.
- Se a execução cair, os pedaços prontos ficam: rodar de novo refaz só os que faltam.
- Durante a junção o bucket guarda original, pedaços e objeto novo ao mesmo tempo: são uns
  2x o tamanho do arquivo a mais por alguns minutos.

No CadÚnico pessoa (37 GB) o modo sequencial levaria ~9 h numa VM de 6 núcleos; o paralelo
levou 1h40.

### Organização do código

`mascarar_minio.py` percorre o `raw/`, decide o que fazer com cada objeto e registra o
resultado. O trabalho mora em `scripts/mascaramento/`:

| Módulo | O que tem |
|---|---|
| `config.py` | Variáveis de ambiente, limites e configuração do log |
| `regras.py` | Padrões das colunas sensíveis, `classificar()`, HMAC e redação |
| `tabular.py` | CSV/TXT linha a linha: em disco (`mascarar_csv`) e direto do MinIO |
| `paralelo.py` | O modo paralelo acima |
| `xlsx.py` | Reescrita das células sensíveis direto no XML do XLSX |
| `mdb.py` | Verificação do Access |
| `auditoria.py` | `lake._masking_log` e o parquet de auditoria |

Para mudar **o que** é mascarado, mexa em `regras.py`; o resto não muda.

### Transporte byte-preserving (por que não corrompe acentos)

O arquivo é lido e reescrito em **latin-1** (mapeia todos os 256 bytes 1:1), então qualquer
coluna **não mascarada** é gravada byte-idêntica, independentemente do encoding real do
arquivo. Só as colunas-alvo são trocadas por texto ASCII (token/`***`). O encoding real é
detectado apenas para interpretar corretamente os **nomes** das colunas no matching.

### Bases Access (.mdb) — só verificação, nunca reescrita

O lake tem **128 arquivos `.mdb` (~105 GB)**. Eles são lidos via **`mdbtools`** (binário do
sistema), que é **read-only**: não existe `mdb-import`. Reescrever um Access no Linux só seria
possível via Java (Jackcess/UCanAccess).

Por isso, para `.mdb` o script **apenas verifica** se há coluna sensível em alguma tabela:

| Resultado | Status |
|---|---|
| Nenhuma coluna sensível | `skipped_no_pii` |
| **Alguma coluna sensível** | **`mdb_mascara_na_staging`** — nada é gravado no `raw/` |

Sem poder reescrever o arquivo, marcar como `masked` seria mentira. O `.mdb` continua com PII
no `raw/`; quem protege as camadas seguintes é o `raw_para_staging.py`, que passa o CSV
exportado de cada tabela por `mascarar_csv()` (mesmas regras, mesmo HMAC) antes de gravar o
parquet. Para tirar a PII do
`raw/` também, é preciso converter e descartar o `.mdb`, ou reescrevê-lo via Java (Jackcess).

**Correção (out/2026):** a amostra abaixo não pegou o `CCI_CCA`. As tabelas `cci_analitico` e
`cca_analitico` têm `DT_Nascimento` e `IM_CEP` de pessoa física, e chegaram sem máscara à
staging e ao banco até o `raw_para_staging.py` passar a mascarar o `.mdb`.

**Por que não têm PII** (verificado em amostra das 4 famílias): `MCidades_AO_1/2/3` (96 arqs),
`CCI_CCA` (30), `AF` (1) e `CCI_CCA_ATUAL` (1) são dados de empreendimento/obra e analítico
agregado. O mais próximo de PII é `Gênero`, `Entidades.CGC` (PJ) e
`tab_empreendimentos.txt_logradouro` — endereço do **empreendimento**, preservado corretamente
pela regra condicional (só mascara CEP/endereço com indicador de PF na mesma tabela).

### Idempotência (não re-mascara)

Objetos já mascarados recebem a tag `masked=true` e são pulados em execuções seguintes —
evita o **duplo-HMAC** (aplicar HMAC sobre um token já gerado). Use `--force` para reprocessar.

Objeto sem a tag cujo conteúdo é igual a um mascaramento anterior (ex.: copiado na
reorganização do lake, que não leva as tags) sai como `skipped_already` e **ganha a tag de
volta**, para a execução seguinte pulá-lo sem reler o arquivo.

Com `--apply`, os objetos liberados sem reescrita (`skipped_no_pii`, `skipped_no_header`,
`skipped_unsupported`) recebem a tag `triagem=<status>`. É ela, junto com `masked=true`, que
libera o objeto para a staging.

---

## 3. Configuração (`.env`)

| Variável | Obrigatória | Descrição |
|---|---|---|
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` | sim | Conexão MinIO |
| `DB_DW_HOST_MCID`, `DB_DW_PORT_MCID`, `DB_DW_USER_MCID`, `DB_DW_PASSWORD_MCID`, `DB_DW_DBNAME_MCID` | sim | Postgres (tabela de controle) |
| `MASKING_HMAC_SECRET` | **sim** | Segredo do HMAC. Sem ela o script recusa rodar. |
| `MASKING_TMPDIR` | recomendado | Dir de temporários do XLSX/MDB (CSV/TXT não usa disco). **Não use `/tmp`** se for tmpfs (RAM). |
| `LAKE_SCHEMA` | não (default `sftp`) | Schema da tabela de controle |
| `MASKING_PREFIX` | não (default `raw/`) | Prefixo a varrer |
| `MASKING_TOKEN_LEN` | não (default `16`) | Tamanho do token HMAC (hex chars) |
| `MASKING_REDACTION` | não (default `***`) | Valor da redação |
| `MASKING_PROCESSOS` | não (default núcleos − 1) | Processos do modo paralelo; `1` desliga |
| `MASKING_PARALELO_MIN_MB` | não (default `1024`) | Tamanho mínimo para usar o modo paralelo |
| `MASKING_PEDACO_MB` | não (default `2048`) | Tamanho de cada pedaço (máx. 5 GB, limite do S3) |

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
| `skipped_no_pii` | Sem coluna sensível (bases de empreendimento/PJ). Esperado — mas ver **Arquivos sem cabeçalho** abaixo. |
| `skipped_no_header` | Não é tabular / sem delimitador detectável. |
| `skipped_already` | Já mascarado (tag `masked=true`, ou conteúdo igual a um mascaramento anterior). |
| `mdb_mascara_na_staging` | `.mdb` com PII: fica como está no `raw/` e é mascarado na conversão. |
| `skipped_unsupported` | Formato não suportado (`.xls`, `.zip`) — **tratar à parte**. |
| `error` | Falhou; **o objeto NÃO foi sobrescrito**. Investigar e re-rodar com `--pattern`. |

Ao final, o log imprime a contagem por status. **`error` deve ser 0.**

---


## 6. Arquivos sem cabeçalho

O mascaramento acha as colunas pelo **nome**. Num arquivo cuja primeira linha já é dado,
não há o que casar: ele sairia como `skipped_no_pii` — "sem PII" — com a PII intacta.

Isso foi medido, não suposto: dos **2.405** arquivos de texto do `raw/`, **2.378 têm
cabeçalho de verdade** e apenas **6** começam com dado. Desses 6, **5 não têm CPF nem NIS**.
Sobra um:

| arquivo | achado |
|---|---|
| `CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112.TXT` | col 2 valida como CPF em 298/300; col 3 como NIS em 248/248 |

Com um arquivo e um layout conhecido, a solução não é heurística — é declaração. O mapa
`COLUNAS_POR_POSICAO` (em `mascaramento/regras.py`) dá as posições à mão, por key exata:

```python
COLUNAS_POR_POSICAO = {
    "raw/.../CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112.TXT": {2: "cpf", 3: "nis"},
}
```

Funciona porque o passo que reescreve **já é posicional** (`idx_action` em
`_mascarar_fluxo`, de `mascaramento/tabular.py`); a busca por nome existe só para produzir esses índices. O override os
fornece direto. Mesmo HMAC, mesma auditoria, mesma tag de idempotência — e o token de um CPF
é idêntico ao que o caminho por nome geraria, então o join com as outras bases continua de pé.

Estar no mapa também significa **"este arquivo não tem linha de cabeçalho"**: nenhuma linha é
consumida antes do laço, então a linha 0 é mascarada como dado. Sem isso o CPF da primeira
linha vazaria mesmo com os índices certos. O round-trip compara linha a linha, então essa
linha entra na conferência como qualquer outra.

Nada é inferido em tempo de execução: arquivo fora do mapa segue exatamente o caminho de hoje.

## 7. Auditoria e linhagem

**Parquet** por execução, em `audit/masking/execution_id=<uuid>/part-0.parquet` no MinIO
(+ cópia local `scripts/auditoria_mascaramento_<uuid>.parquet`). Colunas:
`execution_id, timestamp, bucket, file, file_name, file_format, encoding, delimiter,
has_pf_indicator, has_formulas, masked_columns, registros_total, registros_alterados,
hash_before, hash_after, status, error_message, duration_s`.

**Tabela de controle** `lake._masking_log` no Postgres (criada automaticamente), com
`UNIQUE(minio_key, source_hash)`. Consulta útil:
```sql
SELECT status, count(*), sum(registros_alterados)
FROM lake._masking_log
GROUP BY status;
```

---

## 8. Formatos e limitações conhecidas

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

## 9. Por que HMAC e não hash simples

Um hash simples (`sha256(cpf)`) de um CPF é **reversível na prática**: o espaço de CPFs é
pequeno (~10¹¹, e menos ainda com dígito verificador), então dá para testar todos os
candidatos numa GPU em minutos e casar os hashes ("ataque de dicionário"). O **HMAC** mistura
um **segredo** no cálculo — sem a chave, essa busca exaustiva é inviável. E, sendo
determinístico, mantém o mesmo token para o mesmo CPF entre bases, preservando joins.
É o padrão correto para identificadores estruturados de baixa entropia (CPF, PIS, telefone).

---

## 10. Recomendações operacionais

- **Sempre rode dry-run antes do `--apply`** e confira a auditoria + amostras.
- **Fatiar o `--apply`** por formato/tamanho (menores primeiro); se algo falhar no meio,
  o que já foi feito não se perde (idempotência por tag).
- Garanta `MASKING_TMPDIR` num disco com espaço para os XLSX. CSV/TXT não usa disco e
  ocupa pouca RAM (partes de 64 MB), mas os arquivos grandes têm rede intensa: rode-os
  fora de horário de uso pesado.
- `--apply` é **destrutivo e irreversível** (os bytes originais com PII são perdidos).
  A verificação round-trip roda antes de cada sobrescrita como salvaguarda.
```
