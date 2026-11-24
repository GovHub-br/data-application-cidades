# Contratos de dados (ODCS)

Um contrato diz o que se espera de cada dado que a ingestão recebe: onde ele pousa no lake, como reconhecer a entrega, que leitor o converte e quais colunas ele tem (ADR-0006). A ingestão confere cada entrega contra o contrato antes de deixá-la entrar na staging. O que não bate fica em quarentena, e o que não tem contrato fica na raw como `uncataloged`.

## Organização

Uma pasta por fonte, a mesma que abre o caminho no lake (`raw/<fonte>/...`). Dentro dela, um arquivo por grupo de dados:

```
contracts/
├── sftp/          remessas da CAIXA no SFTP do MCid (GEFUS, GEAVO, CadÚnico)
│   └── subsidiada/  linha subsidiada: FDS (INT059), Rural (INT055/057/065) e dados prioritários da SNH
├── sharepoint/    planilhas do SharePoint do MCid (inclui o Canal FGTS)
├── ibge/          agregados da API v3
├── bacen/         SGS e Olinda
├── fgv/  abecip/  fipe/  novo_caged/  mrv/  infomoney/  tesouro/
```

- O arquivo é `contracts/<fonte>/<grupo>.odcs.yaml`, e o `id` do contrato é `<fonte>_<grupo>`. Numa subpasta por linha, o `id` leva a linha: `contracts/sftp/subsidiada/fds.odcs.yaml` é `sftp_subsidiada_fds`.
- No SFTP ficam os dados essenciais do MCMV, e ele vai concentrar a maior parte dos contratos (FGTS, linha subsidiada etc.). Quando a pasta crescer, ela pode ganhar subpastas por linha, como `sftp/fgts/` e `sftp/subsidiada/`: a ingestão e os geradores leem as subpastas.

## Campos de cada dado (`customProperties`)

| Campo | O que é |
|---|---|
| `path` | Pasta do dado na staging, **sem** `load_id=` (ex.: `staging/fgv/icst/*.parquet`) |
| `file_pattern` | Regex do nome do arquivo entregue (ex.: `icst\.csv`) |
| `load_type` | `full` (cada carga traz o dado inteiro) ou `incremental` |
| `max_row_variation` | Variação de linhas aceita entre cargas `full`; acima disso, quarentena |
| `reader` | Leitor de `plugins/datalake/readers.py` que dá forma de tabela ao arquivo |
| `dag` | DAG que ingere o dado |
| `dbt_tables` | Tabelas do dbt que leem o dado |

As colunas esperadas estão em `properties`. Uma coluna renomeada na origem pode ser aceita por `aliases`.

## Fluxo

1. Escreva ou ajuste o contrato aqui.
2. Rode `uv run python scripts/contratos/gerar_sources.py`, que gera `dbt/mcid/models/fontes/<id>.yml`. O `tests/test_contratos.py` falha se o gerado estiver desatualizado.
3. Para o SFTP e o SharePoint, `scripts/contratos/gerar_contratos.py` cria o rascunho a partir dos cabeçalhos da staging.
