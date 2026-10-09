# Aprendizados do refactor da ingestão

O que o refactor da ingestão do cidades (PR #206) ensinou, para quem mantém o
pipeline e para quem vai repetir a migração em outro projeto do GovHub. As
decisões de arquitetura e os porquês estão em [diagnostico.md](diagnostico.md);
as classes, em [diagrama-classes.md](diagrama-classes.md); o antes e depois de
cada fonte, em [comparacao-migracao.md](comparacao-migracao.md). Aqui fica o que
não cabe nesses três: o que deu certo, o que deu errado e o que fazer da próxima
vez.

A skill `govhub-ingestion-refactor` (repositório GovHub-skills, `01-govhub/`)
transforma estes aprendizados em roteiro para o Claude Code.

## 1. A arquitetura em uma página

```
fonte ──(provider do Airflow: HttpHook, S3Hook, SFTPHook, ImapHook)──┐
                                                                    │ Extractor (Strategy + Factory)
                                                                    ▼
                                                    preparos (Unpack, MaskPii)   ← no worker, antes do pouso
                                                                    ▼
raw/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/  + _SUCCESS (manifesto)   ← imutável
                                                                    │ Converter (Template Method + Factory)
                                                                    ▼
staging/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/*.parquet  + latest/   ← só texto, cabeçalho original
                                                                    │ dbt: select * from {{ fonte_lake(...) }}
                                                                    ▼
bronze (overwrite | append | merge) → prata → ouro
```

- A DAG não tem lógica. Ela declara um `DatasetSpec` (de onde, como converter,
  como carregar) e liga `steps.extract_to_raw >> steps.convert_to_staging`. Todo o
  resto mora em `plugins/ingestion`.
- O que muda de uma fonte para outra é configuração: `ExtractorConfig`,
  `ConverterConfig`, `RemoteFiles`, `Field`, `prepare`. Classe com nome de fonte
  (`FgvDadosExtractor`, `IbgeV3Converter`) é sinal de que falta uma opção genérica.
- Conexão e credencial são Airflow Connection. Variable só para o que não é
  conexão (a chave da Alpha Vantage, por exemplo), e lida dentro da task, nunca no
  parse da DAG.

## 2. Providers primeiro

Antes de escrever cliente HTTP, SFTP ou S3, procurar o hook do provider oficial.

| Necessidade | O que usamos | O que evitamos |
|---|---|---|
| API, arquivo por HTTP | `HttpHook` (provider `http`) | `requests` solto com retry caseiro |
| Object storage | `S3Hook` (provider `amazon`) | cliente boto montado à mão |
| SFTP | `SFTPHook` (provider `sftp`), download em janelas (`readv`) | `paramiko` direto; o prefetch dele trava sob limitação de banda |
| E-mail | `ImapHook.download_mail_attachments` (provider `imap`) | `ImapAttachmentToS3Operator`, que só leva o 1º anexo e em memória |

- Operators de transferência (`*ToS3Operator`) ficam de fora: escondem o pouso,
  não escrevem manifesto e não deixam a raw imutável.
- Fonte pública sem credencial usa `base_url` no `ExtractorConfig` (Connection em
  memória); ninguém precisa cadastrar Connection para baixar um XLSX público.
- Fora de uma task, o SDK do Airflow 3 só acha Connection por variável de
  ambiente ou backend de segredos. Script de validação precisa de
  `AIRFLOW_CONN_<ID>`.

## 3. Como o trabalho foi conduzido (e por quê funcionou)

- **Uma fase por vez, com plano aprovado e parada no fim.** Decisão de produto
  (modo de carga, o que vira dataset, o que sai) é de quem é dono do dado:
  pergunta, não suposição.
- **TDD, um commit por ciclo verde.** Toda DAG mudada tem teste. A checagem roda
  sem atalho: black nos arquivos tocados, ruff, mypy e pytest, parando no primeiro
  erro. `| tail` engole código de saída; ver o `exit` de verdade.
- **Validar contra a fonte real** no Airflow local, gravando só no prefixo `tests/`
  do MinIO, e comparar com a staging antiga arquivo a arquivo (colunas, linhas e
  conteúdo com `EXCEPT ALL` nos dois sentidos).
- **Testes proporcionais ao risco.** Fonte lenta (o SFTP entrega ~0,5 MB/s) se
  valida por amostra: a entrega mais antiga e a mais recente de cada família, e
  o código provado byte a byte contra o legado onde for o caso.
- **Não rodar dbt contra o banco compartilhado** (homolog) sem pedido: ele
  reescreve tabelas que outras pessoas usam. Macros se testam num projeto
  dbt-duckdb temporário (`tests/ingestion/dbt`).
- **Diagnosticar sobre `origin/main`.** Branch local velha já fez a análise partir
  de código de 600 commits atrás. E não supor fonte: checar no código o que é
  extraído hoje antes de dizer que algo não existe.

## 4. Armadilhas por fonte

### APIs HTTP
- O SGS do BACEN ignora `ultimos` em query string (devolve a série inteira), exige
  `dataInicial` nas séries diárias (HTTP 406) e às vezes responde 200 com uma
  página HTML. O `api` trata HTML/XML como erro retentável.
- O Power BI serve JSON como `text/plain`: rejeitar só HTML/XML, não exigir
  `application/json`.
- IBGE API v3: uma classificação por chamada (duas dão HTTP 500); o valor vem com
  ponto decimal, e tirar o ponto na tipagem corrompe o número.
- Filtro OData (Olinda) vai no endpoint, com `%20`, não em `params`.

### SFTP
- `.zip` que é gzip, e `.xlsx` que é zip: detectar formato pelo conteúdo (bytes
  mágicos e `[Content_Types].xml`), nunca pela extensão.
- A mesma entrega aparece como `X.TXT`, `X.TXT.zip` e `X.zip`, em `GEFUS/` e em
  `ANTERIORES/`: a identidade é o nome sem pasta e sem as extensões encadeadas.
- Pacotes mensais com várias famílias (`202310.zip`) trazem meses que não existem
  soltos. Entram como `bundles` e saem pelo `Unpack(members=família)`; o manifesto
  registra a origem mesmo quando nada dela pousa, para não baixar de novo.
- Fora: `_substituido` (versão superada), `.filepart` (upload em curso), `~$`
  (temporário do Excel), `.7z` vazios.
- Linha com campos a mais (a INT039 manda 2 em 1 milhão) deslocava o CEP para
  uma coluna sem máscara: linha desalinhada é redigida inteira, e a conversão a
  descarta e conta (`bad_rows="skip"`).

### Conversão
- Cabeçalho em branco, repetido ou só com espaço vira `column_<n>`/`<nome>_2`; a
  staging guarda o cabeçalho original e o dbt (`normalizar_colunas`) entrega à
  prata os nomes de antes. A paridade do macro com a função Python é testada
  caractere a caractere.
- Encoding e delimitador detectados por arquivo (`"auto"`) quando a fonte varia;
  byte inválido fora da amostra vira U+FFFD, não derruba a conversão.
- No DuckDB, `::numeric` é `DECIMAL(18,3)`: comparar como `double`.

## 5. Modo de carga: a decisão que mais pesa

| Como a fonte entrega | Modo | Exemplo |
|---|---|---|
| Série inteira a cada execução | `overwrite` | INCC, FipeZap, ABECIP |
| Janela parcial que se acumula, com revisões | `merge` + `keys` | IBGE v3, Infomoney |
| Uma entrega nova por arquivo (retratos, remessas) | `append` | SFTP inteiro |

- Extração incremental e `overwrite` não combinam: a última ingestão traz só o
  que chegou de novo (e a primeira, o histórico inteiro). Fonte de retratos é
  `append`, e a bronze que quer só o último filtra com `arquivo_mais_recente`.
- O `latest/` guarda a partição na pasta (`latest/<data>/<hora>/`) para o
  `filename` da bronze continuar trazendo a data da ingestão (`lake_dt_ingest`).

## 6. Ambiente e ferramentas

- O hook de pre-commit formata o repositório inteiro e falha: commitar com
  `--no-verify` e rodar o lint só nos arquivos tocados.
- O `dbt-core` chama `load_dotenv()` no import e vaza o `.env` nos testes: uma
  fixture autouse limpa as variáveis do storage.
- No venv local, o upgrade do Airflow no lugar apaga `airflow/__init__.py`;
  reinstalar com `--force-reinstall`.
- O zsh não faz word splitting de `$VAR`; passar a lista de arquivos explícita.

## 7. Virada e limpeza (depois do merge)

1. Criar as Connections (`sftp_mcid`, `minio_lake`, as das APIs) e o
   `MASKING_HMAC_SECRET` no worker, com o mesmo segredo de antes, para os tokens
   de CPF/NIS não mudarem.
2. Reservar disco no `lake_tmp` para o maior arquivo duas vezes (original e cópia
   mascarada; o CadÚnico pessoa tem ~34 GB).
3. Rodar as DAGs de ingestão antes do primeiro `dbt run`; no SFTP, a primeira
   carga leva cerca de 30 h.
4. Só depois de conferir as pratas, apagar a `raw/sftp` e a `staging/sftp` antigas.
   O `CCI_CCA` e as bases AO/AF do GEAVO só existem lá (a pasta pede outra senha),
   e os `.mdb` do `CCI_CCA` têm dado pessoal sem máscara.
