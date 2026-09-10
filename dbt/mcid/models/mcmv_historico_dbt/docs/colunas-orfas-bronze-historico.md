# Colunas órfãs das bronzes de empreendimento (eixo histórico)

Change: `colunas-orfas-bronze-historico`. Entregável das tasks 1.1–1.3 (auditoria
+ enums) e 8.3 (inventário final). Base: varredura de 2026-09-08 sobre
`/mnt/data/duckdb/cidades.duckdb` (build local modo A).

`coalesce_present(bronze, [aliases])` é uma **allowlist**: coluna do bronze fora
da lista de aliases é descartada sem `warn`. Este doc inventaria o que está órfão
e com sinal de negócio nas 7 bronzes de empreendimento
(`bronze_sftp_empreendimento_int040/054/057/059/065` +
`_snh_bb/_snh_caixa`).

## 1.1 Inventário — órfãs com sinal semântico

"Órfã com sinal" = nome casa
`^(uh|qt|qtd|qtde|quantidade)_ | ^(vr|valor)_ | ^dt_ | situacao | pendencia |
retomad | paralis | entidade | aporte` **e** não aparece em nenhum `.sql` de
`models/mcmv_historico_dbt/` nem `macros/historico/`.

### INT040 (FAR CAIXA — 239.022 linhas)

| coluna | fill | destino nesta change |
|---|---|---|
| `dt_ultima_liberacao_recurso` | 93,9% | **Bloco C** → `dt_ultima_liberacao` |
| `desc_situacao_contrato` | 100% | **Bloco A** → `desc_situacao_contrato` (cru) |
| `situacao_retomada` | 4,9% (292 APF) | **Bloco A** → `sinal_retomada` |
| `dt_primeira_entrega` | 67,2% | **Bloco C** → `dt_primeira_entrega` (hoje só via espinha SNH) |
| `cod_situacao_contrato_pj` | 100% | ignorar — código espelho de `desc_situacao_contrato` |
| `cod_motivo_ociosidade` | **0% real** (só literal 'NULL') | ignorar — fonte vazia |
| `tipo_aporte` | 96,4% | ignorar — não é sinal de paralisação/retomada nesta iteração |
| `vr_contrapartida_1` / `vlr_operacao` | 100% | fora de escopo — mart financeiro |

### INT054 (FAR BB — 11.339 linhas)

Mesmo conjunto do INT040, fills menores. `situacao_retomada` 5,8% (22 APF).
`desc_situacao_contrato` / `cod_motivo_ociosidade` 0% neste lote.
`vr_emprestimo_far` 100% → mart financeiro.

### INT057 (Rural BB — 61.993 linhas)

| coluna | fill | destino |
|---|---|---|
| `dt_ultima_liberacao` | 98,1% | **Bloco C** → `dt_ultima_liberacao` |
| `dt_efetiva_conclusao` | 86,0% | **já consumida** (→ `dt_conclusao_obra`); Bloco C expõe cópia nomeada |
| `no_entidade_organizadora` / `nu_cnpj_entidade` | 100% | **dim** |
| `vr_atec` `vr_ts` `vr_custo_originacao` `vr_taxa_administracao` `vr_taxa_risco_credito` `vr_contrapartida` `vr_cisterna` `vr_efluentes` `vr_atec_ts_cisterna` `vr_tx_adm_cisterna` `vr_edificacao` `vr_operacao` `vr_diferencial_juros` `vr_diif_juros` | 12–100% | fora de escopo — mart financeiro |
| `pz_obra` `pc_execucao_financeira_atec` `pc_execucao_financeira_ts` | 100% | ignorar nesta iteração |

### INT059 (FDS / Entidades — 39.346 linhas)

| coluna | fill | destino |
|---|---|---|
| `dt_assinatura_projeto` | 19,1% | **Bloco C** → `dt_assinatura_projeto` |
| `nu_apf_vinculacao` | 100% | **dim** (só expor; não altera resolução de fase) |
| `dsc_tipologia` / `tipo_de_unidade_do_empreendimento` | 44,8% | **dim** |
| `regime_construcao` / `cod_regime_execucao` | 86,8% / 12,4% | **dim** |
| `modalidade_requalificacao` | 92,9% | **dim** |
| `situacao_empreendimento` | 30,6% (NORMAL / INVADIDO / ABANDONO PELA EO / …) | ignorar nesta iteração — eixo de ocupação, não de obra; registrar no dicionário |
| `dt_maxima_liberacao` | 91,9% | ignorar — `dt_ultima_liberacao` (efetiva) é o marco de Bloco C; máxima = prazo-limite |
| `valor_aporte_adicional` | 74,0% | fora de escopo — mart financeiro |
| `gps_latitude_grau/minuto/segundo` + `gps_longitude_*` | **0% real** (literal 'NULL'; só `gps_hemisferio` às vezes "HEMISFERIO SUL") | **ignorar — fonte vazia** (deviation vs design D4/OQ4: não há GPS a converter) |
| `vr_projeto` `vr_obra` `vr_emprestimo_original` `vr_liberado_sisfin` `valor_aditamento_*` `quantidade_uh_adaptadas` | 0–100% | fora de escopo / vazio |

### INT065 (Rural CAIXA — 473.976 linhas)

| coluna | fill | destino |
|---|---|---|
| `dt_ultima_liberacao` | 100% | **Bloco C** → `dt_ultima_liberacao` |
| `dt_efetiva_conclusao` | 91,6% | **já consumida** (→ `dt_conclusao_obra`) |
| `no_entidade_organizadora` / `nu_cnpj_entidade` | 99,8% / 98,6% | **dim** |
| `dsc_situacao_obra` | 12,5% | ignorar — redundante com `no_situacao_obra` (consumida) |
| `pz_construcao` | 100% | ignorar nesta iteração |
| `vr_atec` … `vr_subsidio_fgts` `vr_emprestimo` `vr_contrapartida` `vr_cisterna` … | 100% | fora de escopo — mart financeiro |

### SNH BB (21.896 linhas) / SNH CAIXA (287.401 linhas)

| coluna | fill BB / CAIXA | destino |
|---|---|---|
| `detalhamento_da_situacao_do_empreendimento` | 99,9% / 87,2% | **Bloco A** → `sinal_retomada` (só `A RETOMAR%`); resto = NULL nesta iteração |
| `quantidade_de_uhs_{contratadas,entregues,vigentes,distratadas}_do_ano_de_referencia` | 100% / 82–87% | **Bloco B** → `fluxo_ano` (`_ano`) |
| `quantidade_de_uhs_*_em_janeiro_do_ano_de_referencia` | 100% / 82–87% | **Bloco B** → `fluxo_ano` (`_jan`) |
| `valor_desembolsado_do_ano_de_referencia` | 1,1% / 75,3% | **Bloco B** → `fluxo_ano` (`valor_desembolsado_ano`, ressalva de fill) |
| `situacao_do_empreendimento_em_janeiro_do_ano_de_referencia` | 100% / 82,7% | ignorar — baseline de status, não usado nesta iteração |
| `detalhamento_*_em_janeiro_do_ano` | 99,8% / 75,0% | ignorar — baseline |
| `classificacao_dos_paralisados` (só CAIXA) | — / 0,8% | **Bloco A** → `motivo_paralisacao` |
| `valor_aporte_adicional` | 3,2% / 94,3% | fora de escopo — mart financeiro |
| `latitude_do_imovel` / `longitude_do_imovel` | 11,9% / 59,7% | **dim** (decimal PT-BR; sentinela `0`/`0` → NULL) |
| `bairro_do_imovel` `logradouro_do_imovel` `complemento_*` | — / 82% | **dim** (endereço) |
| `mcmv_ogu_27/28/30/31_*` (só CAIXA) | 1,6–17,3% | **ignorar** — redundantes com a família `quantidade_de_uhs_*_do_ano` |

## 1.2 Enum de `sinal_retomada`

Valores brutos observados:

- **SFTP** `situacao_retomada` (INT040/054): **único valor** não-nulo →
  `OBRA_RETOMADA_COM_SUPL_APORTE`.
- **SNH** `detalhamento_da_situacao_do_empreendimento` casando `A RETOMAR%`
  (BB tem caixa mista):
  - `A RETOMAR` / `A Retomar`
  - `A RETOMAR - APORTE SUPLEMENTAR`
  - `A RETOMAR - AO/MCI/ASSINATURA`
  - `A RETOMAR - SOLUÇÃO LOCAL`
  - `A RETOMAR - APORTE ENTE PÚBLICO`
  - `A RETOMAR - EM ANÁLISE`
  - `A RETOMAR - EM CONSTRUÇÃO DA PROPOSTA`

Enum canônico proposto (distinção mínima retomada × a-retomar, + subtipo):

| `valor_bruto` | `sinal_retomada` | `origem` |
|---|---|---|
| `OBRA_RETOMADA_COM_SUPL_APORTE` | `retomada_aporte_suplementar` | sftp |
| `A RETOMAR` | `a_retomar` | snh |
| `A RETOMAR - APORTE SUPLEMENTAR` | `a_retomar_aporte_suplementar` | snh |
| `A RETOMAR - AO/MCI/ASSINATURA` | `a_retomar_ao_mci_assinatura` | snh |
| `A RETOMAR - SOLUÇÃO LOCAL` | `a_retomar_solucao_local` | snh |
| `A RETOMAR - APORTE ENTE PÚBLICO` | `a_retomar_aporte_ente_publico` | snh |
| `A RETOMAR - EM ANÁLISE` | `a_retomar_em_analise` | snh |
| `A RETOMAR - EM CONSTRUÇÃO DA PROPOSTA` | `a_retomar_em_construcao_proposta` | snh |

Cobertura: FAR 292+22 APF (SFTP `situacao_retomada`, desde 2019-12); SNH BB 76 /
CAIXA 277 APF (`detalhamento_*`, 2024-06+). O braço SNH só passa o
`detalhamento_*` para a resolução de retomada quando casa `A RETOMAR%` (senão
`NULL` — os demais valores do `detalhamento` são rescisão/desimobilização/
ocupação, não retomada).

## 1.3 Enum de `motivo_paralisacao`

- **INT040 `cod_motivo_ociosidade`**: **0% real** (só `None` / literal `'NULL'`)
  → não contribui. Vai para `colunas_bronze_ignoradas`.
- **SNH CAIXA `classificacao_dos_paralisados`** (0,8% fill, ~2.600 linhas):
  domínio de classificação da obra paralisada.

Enum canônico proposto:

| `valor_bruto` | `motivo_paralisacao` |
|---|---|
| `EM ANDAMENTO` | `em_andamento` |
| `A ENTREGAR` | `a_entregar` |
| `A RETOMAR - APORTE SUPLEMENTAR` | `a_retomar_aporte_suplementar` |
| `A RETOMAR - EM ANÁLISE` | `a_retomar_em_analise` |
| `A RETOMAR - AO/MCI/ASSINATURA` | `a_retomar_ao_mci_assinatura` |
| `A RETOMAR - SOLUÇÃO LOCAL` | `a_retomar_solucao_local` |
| `A RETOMAR - APORTE ENTE PÚBLICO` | `a_retomar_aporte_ente_publico` |
| `A RETOMAR - EM CONSTRUÇÃO DA PROPOSTA` | `a_retomar_em_construcao_proposta` |
| `A RETOMAR` | `a_retomar` |
| `OCUPADO, EM LEGALIZAÇÃO` | `ocupado_em_legalizacao` |
| `RESCISÃO OPERAÇÃO` / `RESCISÃO DA OPERAÇÃO` | `rescisao_operacao` |
| `INDICATIVO DESIMOBILIZAÇÃO` | `indicativo_desimobilizacao` |
| `DESIMOBILIZAÇÃO ASSINADA` | `desimobilizacao_assinada` |
| `DESIMOBILIZAÇÃO PARA VENDA AUTORIZADA` | `desimobilizacao_venda_autorizada` |
| `OCUPAÇÃO IRREGULAR` / `OCUPADO` | `ocupacao_irregular` |
| `EXTRAPOLA TETO` | `extrapola_teto` |
| `REDUÇÃO DE META` | `reducao_de_meta` |
| `ENCERRAMENTO DA OPERAÇÃO` | `encerramento_operacao` |
| `SUPLEMENTAÇÃO DE OBRA NÃO INCIDENTE` | `suplementacao_obra_nao_incidente` |
| `DOAÇÃO COM INFRA A SER EXECUTADA PELO FAR` | `doacao_infra_far` |

Fora do seed → `nao_mapeada` (teste `dentro_do_dominio` `warn`).

## 1.4 Decisões (Open Questions do design)

- **OQ1 — enum de `sinal_retomada`**: subtipo no próprio enum (acima), não em
  coluna separada. `retomada_*` vs `a_retomar_*` dá o corte binário por prefixo.
- **OQ2 — `dim` em `silver/` ou `gold/`**: `silver/` (deriva de silvers/bronzes
  históricas; `prata_*` / `bronze_*` / `ouro_*` é o padrão de nome do eixo). Alias
  `dim_empreendimento_historico`, schema `mcmv_historico`.
- **OQ3 — `desc_situacao_contrato`**: cru, sem mapa canônico (voto do design).
- **OQ4 — precedência de coordenadas**: **moot** — `gps_*` do INT059 é 0% real.
  `latitude`/`longitude` da dim vêm só de `latitude_do_imovel` /
  `longitude_do_imovel` (SNH), parse PT-BR, sentinela `0` → NULL.
- **OQ5 — guardrail cobre `serie_executiva`/`obra_mensal`**: depois. Esta
  iteração = as 7 de empreendimento.
- **OQ6 — `nu_apf_vinculacao`**: só expor na dim.

## Deviations vs. proposal/design

1. `cod_motivo_ociosidade` (INT040) é **0% real**, não 7% — INT040 não
   contribui para `motivo_paralisacao`; só a SNH CAIXA
   `classificacao_dos_paralisados`.
2. `situacao_retomada` tem **um único valor** (`OBRA_RETOMADA_COM_SUPL_APORTE`),
   não um domínio — `sinal_retomada` do braço SFTP é efetivamente um marcador
   binário.
3. `gps_*_grau/minuto/segundo` (INT059) é **0% real** — não há conversão a
   fazer (task 5.2 simplifica: só parse do decimal SNH).
4. `dt_efetiva_conclusao` (INT057/065) **já é consumida** como `dt_conclusao_obra`
   no Rural — a coluna nomeada de Bloco C é cópia explícita (redundância aceita
   para simetria do contrato) ou omitida (decidir na task 3.1).

## Cobertura por frente das colunas novas (build local 2026-09-08)

### Blocos A/C no contrato `&contrato_empreendimento` (3 silvers, ao fim)

| coluna | FAR | Entidades (FDS) | Rural | fonte |
|---|---|---|---|---|
| `sinal_retomada` | 3,9% (314 APF) | 0,4% | 0,4% | INT040/054 `situacao_retomada` + SNH `detalhamento` `A RETOMAR%` |
| `motivo_paralisacao` | 0,1% | 1,0% | 0,2% | SNH CAIXA `classificacao_dos_paralisados` |
| `desc_situacao_contrato` | 72,7% (91% c/ LOCF) | — | — | INT040 |
| `dt_ultima_liberacao` | 71,7% (91% c/ LOCF) | — | 76,8% (98% c/ LOCF) | INT040/054/057/065 |
| `dt_primeira_entrega` (+`_fonte`) | 51,7% | — | — | INT040/054 |
| `dt_assinatura_projeto` | — | 16,6% | — | INT059 |

LOCF na cauda (`silver_tail`, `preenchido` CTE): os 7 campos A/C recebem
`last_value(... ignore nulls)` do `(frente, apf)` junto de `valor_contratado` —
senão o `gold_snapshot` (última linha = SNH pós-2024-11) ficava ~0%.

### Fluxo YTD (ex-modelo `silver_historico_empreendimento_fluxo_ano`, dissolvido) — Bloco B

270.858 linhas · grão `(frente_mcmv, apf, dt_referencia)` único · cobertura
2024-06 → 2026-03 · FAR 81.578 / Rural 176.709 / Entidades 12.571.
Fills: `_ano` contratadas/vigentes/distratadas 100%, entregues 95%,
`valor_desembolsado_ano` 80% (CAIXA ~75%, BB ~1% — ressalva no schema.yml).
275 quedas intra-ano (0,1%, `acumulado_nao_regride` `warn`).

### `dim_empreendimento_historico`

17.545 linhas = `ouro_dhist_snapshot_empreendimento_atual` exato (chaves idênticas nos
2 sentidos). Fills: `no_entidade_organizadora`/`nu_cnpj_entidade` Rural ~86%
(FAR/Entidades 0% — INT059 não tem a coluna); `latitude`/`longitude` ~45% (SNH
decimal); `bairro`/`cep`/`logradouro` ~85–91%; `dsc_tipologia` ~29%;
`nu_apf_vinculacao` ~5% (FDS); `portaria_selecao` ~2%.

### Propagação nos golds de estado

- `ouro_dhist_snapshot_empreendimento_atual` +5 colunas ao fim (`sinal_retomada`,
  `motivo_paralisacao`, `desc_situacao_contrato`, `dt_ultima_liberacao`,
  `dt_primeira_entrega`). FAR desc 81% / dul 80%, Rural dul 86%,
  `sinal_retomada` 301 `retomada_*` + ~350 `a_retomar_*`.
- `ouro_dhist_marco_empreendimento` +3 (`dt_primeira_entrega` agora
  `least(silver_direto, espinha)` + `_fonte`; novo marco `dt_ultima_liberacao`
  + `_fonte` + `_dt_snapshot`). `dt_primeira_entrega` fill 67% (era via espinha).

## Guardrail `bronze_colunas_nao_mapeadas`

`macros/data_quality/bronze_colunas_nao_mapeadas.sql`. `warn` nas 7 bronzes.
Rodada final: **PASS=8 WARN=0** — toda coluna com sinal semântico está
consumida ou em `colunas_bronze_ignoradas`.

A lista de aliases consumidos vive em `historico_bronze_aliases_consumidas()`
(o `graph` do dbt não expõe `compiled_code` no contexto de teste). Regerar
quando um mapeamento bronze→silver mudar:

```python
# raiz do repo, com o eixo já compilado:
#   .venv/bin/dbt compile --target staging_duckdb --select mcmv_historico_dbt
import duckdb, glob, re
con = duckdb.connect('/mnt/data/duckdb/cidades.duckdb', read_only=True)
bronzes = ['int040','int054','int057','int059','int065','snh_bb','snh_caixa']
pat = re.compile(r'^(uh|qt|qtd|qtde|quantidade)_|^(vr|valor)_|^dt_|situacao|pendencia|retomad|paralis|entidade|aporte')
files = []
for d in ['models/mcmv_historico_dbt/silver','models/mcmv_historico_dbt/gold',
          'models/mcmv_historico_dbt/bronze','models/indicadores_mcmv_dbt/silver',
          'models/indicadores_mcmv_dbt/gold']:
    files += glob.glob(f'dbt/mcid/target/compiled/mcid/{d}/*.sql')
corpus = "".join(open(f).read().lower() for f in files)
toks = set(re.findall(r'[a-z_][a-z0-9_]+', corpus))
consumed = set()
for b in bronzes:
    for (c,) in con.execute(f"select column_name from information_schema.columns where table_name='bronze_sftp_empreendimento_{b}'").fetchall():
        if pat.search(c.lower()) and c.lower() in toks:
            consumed.add(c.lower())
print(sorted(consumed))
```

## Lista final de "ignoradas" (`colunas_bronze_ignoradas.csv`)

Ver o seed. Resumo por motivo:
- **fonte vazia (literal 'NULL' / 0% real)**: `cod_pendencia_entrega` (todas),
  `cod_motivo_ociosidade` (int040/054), `gps_*_grau/minuto/segundo` (int059).
- **metadados de interface**: `de_interface`, `de_descricao`, `de_pendencia_campo`,
  `nu_linha`, `co_validacao_mdr`, `nu_contrato_emprendimento`, `_source_*`.
- **redundante com coluna consumida**: `cod_situacao_contrato_pj` (↔
  `desc_situacao_contrato`), `dsc_situacao_obra` int065 (↔ `no_situacao_obra`),
  `mcmv_ogu_2*` snh_caixa (↔ família `quantidade_de_uhs_*_do_ano`),
  `dt_maxima_liberacao` int059 (↔ `dt_ultima_liberacao`).
- **breakdown financeiro (mart financeiro futuro, não-goal)**: todas as `vr_*` /
  `contrapartida_*` não consumidas.
- **fora de escopo desta iteração (candidatas ao dicionário)**:
  `situacao_empreendimento` int059, `tipo_aporte`, `pz_obra`, `pz_construcao`.
