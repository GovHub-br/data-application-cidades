# Varredura — sufixo ".0" em colunas de código (eixo histórico)

Change: `testes-data-quality-dbt`, tarefa 1.2.

## O defeito

Um identificador inteiro passou, a montante, por `int -> float -> str`:
`"3550308"` -> `3550308.0` -> `"3550308.0"`. O bronze é cópia fiel, então o
resíduo chega intacto. Num join com uma base que traz o mesmo código sem sufixo
(`"3550308"`), nada casa — silenciosamente.

Valores **numéricos de verdade** (`vlr_*`, `vr_*`, `valor_*`, `pc_*`,
`percentual_*`) também têm o sufixo, mas **não são alvo**: os parsers de
`macros/parse_hist_numeric.sql` (`parse_hist_double`) já absorvem `"2113.0"`
como número. O alvo é só coluna de **identificador que deve seguir texto**
(código IBGE, código de empreendimento, CNPJ, APF, id de registro).

## Escopo da varredura

Varridas **todas as 53 tabelas de bronze** do `cidades.duckdb` (schemas
`bronze` e `dados_historicos`), toda coluna `VARCHAR`.

**Resultado do escopo:** o `.0` só existe nas bronzes do eixo histórico
materializadas via DuckDB a partir do dump `dados_historicos/*.parquet` — as 13
famílias `bronze_dhist_*` / `bronze_sftp_*` / `bronze_shpt_*` + `bronze_dhist_snh_entregas_evento_*`.
As bronzes dos colegas (`bronze_far_*`, `bronze_fds_*`, `bronze_rural_*`,
schema `bronze`) **não têm nenhum `.0`** — inclusive em `apf`, `cep`,
`cod_municipio_ibge`, `cnpj_proponente`: aquele pipeline preservou os tipos.
As interfaces SFTP/GEFUS (`INT040/054/057/059/065`: `nu_apf`,
`cod_municipio_ibge`, `cnpj_proponente`) também estão limpas.

## Método

Consulta ao arquivo local `cidades.duckdb` (modo A, target `staging_duckdb`),
schema `dados_historicos`, tabelas `bronze_dhist_*` / `bronze_sftp_*` / `bronze_shpt_*` e
`bronze_dhist_snh_entregas_evento_*`:

```sql
select count(*) filter (
  where regexp_full_match(trim(cast("<coluna>" as varchar)), '^-?[0-9]+\.0+$')
) as n_com_sufixo
from dados_historicos."<tabela>";
```

Colunas de identificador filtradas por nome (`codigo*`, `cod_*`, `cnpj`,
`ibge`, `apf`, `idregistro`, `id_mcmv`), excluindo prefixos monetários.

## Resultado — colunas de identificador COM sufixo ".0"

| tabela | coluna | linhas c/ ".0" | preenchidas |
|---|---|---:|---:|
| `bronze_dhist_serie_entrada_bb` | `codigo_empreendimento_bb` | 15.717 | 17.815 |
| `bronze_dhist_serie_entrada_bb` | `codigo_do_ibge` | 13.769 | 17.985 |
| `bronze_dhist_serie_entrada_bb` | `codigo_do_empreendimento` | 171 | 171 |
| `bronze_dhist_serie_entrada_bb` | `numero_da_operacao` | 171 | 171 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `codmunicibge` | 9.061 | 282.203 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `codapf` | 9.061 | 282.203 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `idregistro` | 9.061 | 282.203 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `id_mcmv` | 28.974 | 271.761 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `cnpj` | 76.472 | 282.175 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `inumero_cnpj` | 55.056 | 726.868 |
| `bronze_dhist_empreendimento_snh_caixa` | `codigo_ibge_do_municipio` | 58.432 | 287.391 |
| `bronze_dhist_empreendimento_snh_caixa` | `cep_do_imovel` | ~5.030 dist. | 236.812 |
| `bronze_dhist_empreendimento_snh_caixa` | `numero_do_imovel` | ~808 dist. | 236.812 |
| `bronze_dhist_serie_bases_relatorio_executivo` | `ano` | 6 dist. (~40k linhas) | 271.761 |

Amostras (`entrada_bb`): `codigo_empreendimento_bb` = `['1452531.0', '491473.0', ...]`;
`codigo_do_ibge` = `['353470.0', '350160.0', ..., '520870', '330455']` (mistura).
`cep_do_imovel` = `['29000000.0', '76907566.0', '0.0', ...]`; `ano` = `['2012', '2012.0', ...]`.

## Resultado — colunas de identificador LIMPAS (sem ".0")

- **Interfaces SFTP/GEFUS** (`INT040/054/057/059/065`): `nu_apf`,
  `cod_municipio_ibge` / `co_municipio_ibge`, `cnpj_proponente`,
  `nu_cnpj_entidade`, todos os `cod_*` de domínio — **0 ocorrências**. Esse
  pipeline preservou os tipos.
- `bronze_dhist_serie_bext`: `icodigo_empreendimento`,
  `icodigo_municipio_ibge_sem_dv` — limpas.
- `bronze_dhist_serie_min_cidades`: `cod_contrato`, `cod_municipio`,
  `cod_empreendimento`, `cnpj_*` — limpas.
- `bronze_dhist_serie_bases_relatorio_executivo`: a geração posterior
  traz `cod_munic_ibge` e `cod_apf` **limpas** (convivem com as antigas
  `codmunicibge`/`codapf` sujas na mesma tabela empilhada).
- `bronze_dhist_empreendimento_snh_bb`: `apf`, `codigo_ibge_do_municipio`
  — limpas (só o agente CAIXA tem o problema).
- `bronze_dhist_snh_entregas_evento_{bb,caixa}`: `apf` — limpa.

## Encaminhamento

- **Bronze**: teste `sem_sufixo_float_texto` em `warn` (aplicado em
  `models/mcmv_historico_dbt/bronze/schema.yml`). Não corrige — é cópia fiel;
  `warn` porque é defeito de origem conhecido, não regressão.
- **Silver**: macro `strip_float_text` na harmonização + o mesmo teste em
  `error` na coluna já limpa.

### Onde a limpeza é aplicada (todos os consumidores silver das bronzes sujas)

| silver | coluna(s) limpa(s) | fonte suja |
|---|---|---|
| `prata_dhist_serie_executiva` | `chave_natural`, `codigo_ibge_municipio` (antes do `regexp_replace(\D)`), `responsavel_id` | `codapf`, `codmunicibge`, `cnpj`, `inumero_cnpj` de `bases_relatorio_executivo`; `codigo_empreendimento_bb`, `codigo_do_ibge` de `entrada_bb` |
| `prata_{far,fds,rural}_historico_empreendimento` (braço SNH, `corpos_silver.sql`) | `codigo_ibge_municipio` | `snh_caixa.codigo_ibge_do_municipio` |
| `prata_dhist_snh_apf_mes` (reloginho) | `codigo_ibge_municipio` | `snh_caixa.codigo_ibge_do_municipio` |

Braços SFTP/GEFUS dos silvers de frente (`nu_apf`, `cod_municipio_ibge`,
`cnpj_proponente`) e `prata_dhist_snh_entregas_mes` não precisam de limpeza
(fontes já limpas).

### Colunas sujas sem consumidor silver (só teste `warn` no bronze)

`snh_caixa.cep_do_imovel`, `snh_caixa.numero_do_imovel`,
`bases_relatorio_executivo.ano`, `idregistro`, `id_mcmv`. Não são projetadas em
nenhuma silver hoje; se um dia forem promovidas, aplicar `strip_float_text` no
ponto de projeção.
