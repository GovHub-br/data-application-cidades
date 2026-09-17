# Auditoria de grão — série executiva histórica (pré-2019)

Change: `auditar-grao-serie-executiva-historica`. Medido em
`/mnt/data/duckdb/cidades.duckdb` (build local `staging_duckdb`) em 2026-09-07,
sobre as 4 bronzes por família de `familias_serie_executiva()`.

Este doc fecha as tasks 1.1–1.7 e as Open Questions 1–4 do `design.md`.

---

## 1. `bases_relatorio_executivo`

### Fonte e cobertura

- Bronze: **1.011.883 linhas**, **61 `source_file`**, `dt_referencia`
  2012-07 → 2018-08 (44 meses úteis).
- Vários `source_file` por mês-calendário: snapshots progressivos dentro do mês
  (ex.: set/2012 tem arquivos de `10_09_12`, `15_09_12`, `26092012`,
  `30092012` e um `_v3`) **e** reenvios `_v2`/`_v3` do mesmo dia
  (`...30042015.parquet` + `...30042015_v2.parquet`, ambos
  `report_date_parsed = 2015-04-30`).

### Grão real da fonte

`(cod_apf, faixa, faixa_divisao, município, versão-de-arquivo)`. Cada linha é um
**item de movimento de unidades** dentro de um APF: a coluna `uh` / `unidades`
carrega valores **assinados** (`+N` contratação, `−N` distrato). O arquivo
mensal **restata o razão inteiro** de itens do APF — o `−N` de um distrato
antigo reaparece em todos os arquivos futuros.

Exemplo (APF `35773528`, Taubaté/SP): `+48` isolado até out/2014; a partir de
nov/2014 o arquivo passa a trazer `+48` **e** `−48` na mesma faixa em todos os
meses → estoque real do APF = `48 + (−48) = 0` de nov/2014 em diante. A dedup
atual (`row_number() … rn = 1`) mantém **uma** dessas linhas (`+48`), reportando
48 UH para um APF que não tem nenhuma.

### `+N` / `−N`: movimento, **não** artefato de reenvio (Open Question 2)

No arquivo mais recente (`ago2018`, 29.259 linhas, 26.017 APFs):

| classe de grupo APF×faixa_divisao com >1 linha | grupos |
|---|---|
| **linha exata duplicada** (todas as medidas idênticas; difere só em `"295.0"` vs `"295"` e no `hash_linha`) | **2.747** |
| par de **movimento** `+N` / `−N` | 403 |
| outro | 0 |

- **87% dos grupos multi-linha são reingestão byte-a-byte da mesma linha de
  negócio** (o extrator a montante emitiu uma cópia com float-string e outra com
  int-string). `SUM` cego **dobra** essas linhas: soma bruta de `uh` no `ago2018`
  = 3.308.166; após `SELECT DISTINCT` no conteúdo de negócio = 3.001.280
  (**−9,3%**, ~307 k UH).
- Os 13% restantes são pares `+N`/`−N` genuínos (distrato). Nenhum APF fica com
  `SUM(uh) < 0` (0 de 26.017 no `ago2018`); 403 ficam em 0.

**Decisão (D2 refinada):** antes do net de movimento é obrigatório um passo de
**dedup de conteúdo** — `GROUP BY conteudo_hash` (md5 das colunas de negócio já
tipadas, que o modelo já calcula) — para eliminar a reingestão. Só então
`SUM` no grão de consumo neteia os `+N`/`−N`. Não há descarte de par: o `SUM`
pós-dedup-de-conteúdo já resolve os dois casos (dupe some no `DISTINCT`,
movimento neteia na soma).

### Natureza da série (Open Question 3): `estoque`

Estoque restado mês a mês (não deltas). Transições mês-a-mês do estoque por APF
(dedup de conteúdo + `SUM`): 761.010 transições — 86% iguais, 7% sobem, 7%
descem. As quedas são distratos/correções reais, não sinal de série de fluxo.
`natureza_serie = 'estoque'`.

### Colunas de UH: aninhadas parcialmente (task 1.6)

Aninhamento real da fonte (confirmado após a correção de grão):

- `uh_entregues ⊆ uh_contratadas`, `uh_concluidas ⊆ uh_contratadas`,
  `uh_comercializadas ⊆ uh_contratadas`, `uh_em_obras ⊆ uh_contratadas`.
- **`uh_comercializadas` NÃO é subconjunto de `uh_entregues`** — comercializada
  = UH vendida/reservada ao beneficiário, o que ANTECEDE a entrega física. 24%
  das linhas têm `uh_comercializadas > uh_entregues` e isso é característica da
  fonte, não artefato de grão: APFs de linha única já vêm assim (ex.: APF
  `30073748`, 2014-10, linha única: contr 528 / entr 455 / com 456 / conc 528).
- `uh_em_obras` é medida paralela (obra em andamento), não aninhada com
  entregues/concluídas.

Após a correção de grão: `uh_entregues > uh_contratadas` cai de ~7,3 k linhas
para 2.472; `uh_concluidas > uh_contratadas` de 4.753 para 1. O teste
`quantidade_nao_excede_referencia` (`warn`, referência = `uh_contratadas`)
trava regressão nas 4 colunas.

### Grão de consumo

Empreendimento (`chave_natural = cod_apf`, `dt_referencia`). A decomposição por
faixa (Open Question 1) **não** vai para coluna estruturada nesta change: no
`ago2018` **nenhum** APF tem faixas aditivas legítimas (todo grupo multi-linha
é dupe ou movimento na mesma faixa). O corte OGU/FGTS do gold passa a derivar
de `linha_ogu_fgts` recalculado sobre os **subsídios somados do APF**, não da
faixa da linha sobrevivente.

---

## 2. `min_cidades`

- Bronze: **3.903.362 linhas**, 71 `source_file`, `dt_referencia` 2014-10 →
  2016-07 (20 meses com grão por contrato; relatórios agregados 2011–2013 sem
  grão são descartados no `util`).
- **3.880.310 linhas com `cod_contrato`**; exatamente **1 linha por
  `cod_contrato` por mês** (123.609 linhas = 123.609 contratos em 2014-10).
- `vlr_financiamento` ~R$ 90 k/linha, constante mês a mês para o mesmo contrato
  (ex.: contrato `329208408` = 90.277,47 em todos os 20 meses). `qtd_uh`
  majoritariamente nulo.
- **Grão = contrato PF individual** (igual a `bext`). Série de **estoque**
  (contagem acumulada de contratos: 123 k → 254 k ao longo da janela).
- Reenvio dentro do mês existe (2015-01 tem 283.679 linhas / 143.304 contratos
  ≈ 2×) → dedup por `source_file`/`report_date_parsed` necessária.

**Decisão (D3 confirmada):** `grao_familia` de `min_cidades` passa de
`empreendimento` para **`contrato`**. `natureza_serie = 'estoque'`.
`soma_nao_cruza_familia` impede somar com `bases_relatorio_executivo`
(empreendimento).

---

## 3. `bext`

- Bronze: **5.658.694 linhas**, `dt_referencia` 2012-04 → 2018-08 (21 meses).
- `avg(iqde_uh) ≈ 2,4` UH/linha; ~250 k–300 k linhas/mês, crescendo no tempo →
  **estoque** por **contrato PF** (`chave_natural = icodigo_empreendimento`,
  que aqui identifica o contrato).
- **Mesmo padrão `+N`/`−N` de `bases_relatorio_executivo`**: contrato
  `293611265` traz `+1` e `−1` em **todos** os 21 meses (PF de 1 UH distratada).
  A dedup atual (`rn = 1`, desempate por valor negativo olha só colunas de
  **valor**, não `uh`) mantém a linha `+1` → reporta 1 UH para um contrato
  distratado.
- Já marcado `grao_familia = 'contrato'`.

**Decisão:** `bext` recebe o **mesmo** tratamento de dedup-de-conteúdo + `SUM`
no grão contrato que `bases_relatorio_executivo` — não basta `rn = 1`.
`natureza_serie = 'estoque'`.

---

## 4. `entrada_bb`

- Bronze: **~18 k linhas**, 22 meses. Grão = **empreendimento BB**
  (`codigo_empreendimento_bb`). ~410 linhas/mês.
- Colunas de UH: `qde_unidades` (contratadas), `unid_disponiveis`,
  `unid_fin_pf`. Sem `entregues`/`concluidas`/`em_obras` reais.
- É a **entrada** de novos empreendimentos na carteira BB → série de **fluxo**
  (`natureza_serie = 'fluxo'`), não estoque acumulado.
- Silver hoje tem 14 grupos `(chave_natural, dt_referencia)` com 2–3 linhas —
  reenvio; dedup de conteúdo resolve.

**Decisão:** `grao_familia = 'empreendimento'` (mantém), `natureza_serie =
'fluxo'`. Não somar `entrada_bb` (fluxo) com as demais (estoque) no tempo.

---

## 5. Fechamento das Open Questions (task 1.7)

| # | Questão | Decisão |
|---|---|---|
| 1 | Decomposição por faixa em coluna estruturada? | **Não.** Nenhum APF tem faixas aditivas legítimas no período; multi-linha = dupe ou movimento. Total do APF só. `linha_ogu_fgts` recalculado sobre subsídios somados. |
| 2 | `+N`/`−N` = movimento ou artefato? | **Movimento** (distrato). Mas precedido de **dedup de conteúdo** (`conteudo_hash`) que remove a reingestão byte-a-byte (87% dos grupos multi-linha). |
| 3 | `natureza_serie` de `bext`/`min_cidades` | **`estoque`** (valor por contrato constante entre meses; contagem acumulada cresce). |
| 4 | `min_cidades` × `bases_relatorio_executivo` no período sobreposto | Grãos diferentes (contrato PF vs empreendimento). `grao_familia` + `soma_nao_cruza_familia` + nota "não somar entre famílias" cobrem; teste explícito adicionado. |

## 6. Resumo das decisões para a silver

| Família | grão de consumo | `grao_familia` | `natureza_serie` | tratamento de dedup |
|---|---|---|---|---|
| `bases_relatorio_executivo` | empreendimento (APF) | `empreendimento` | `estoque` | dedup conteúdo → `SUM` por (APF, mês) |
| `min_cidades` | contrato PF | `contrato` **(muda)** | `estoque` | dedup conteúdo → 1 linha por (contrato, mês) |
| `bext` | contrato PF | `contrato` | `estoque` | dedup conteúdo → `SUM` por (contrato, mês) |
| `entrada_bb` | empreendimento BB | `empreendimento` | `fluxo` **(muda)** | dedup conteúdo → 1 linha por (emp, mês) |

### Magnitude do impacto (bases_relatorio_executivo, 2018-08, nacional)

| métrica | silver antes (`rn=1`) | soma bruta | dedup-conteúdo + SUM (implementado) |
|---|---|---|---|
| `uh_contratadas` | 3.005.577 | 3.308.166 | 3.001.213 |
| linhas | 25.913 | 29.259 | 25.913 (1/APF) |

O total nacional muda pouco (~0,15%); o ganho é **por linha**: faixa, subsídio
e `linha_ogu_fgts` passam a representar o APF inteiro (soma das faixas dedupadas)
e não uma linha de movimento sorteada. Efeitos medidos na silver inteira
(`bases_relatorio_executivo`):

| checagem | antes | depois |
|---|---|---|
| `uh_contratadas < 0` | 4.881 | 0 |
| `uh_entregues > uh_contratadas` | ~7.300 | 2.472 |
| `uh_concluidas > uh_contratadas` | 4.753 | 1 |
| `uh_comercializadas > uh_entregues` | 186.345 | 186.345 (característica da fonte, ver acima) |
