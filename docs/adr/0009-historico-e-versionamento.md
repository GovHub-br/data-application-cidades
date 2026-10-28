# 0009. Histórico e versionamento

- **Status:** Proposto
- **Data:** 2026-10-19
- **Relacionados:**
  - ADR-0002 (prata com todos os períodos, ouro escolhe o vigente), ADR-0005 (uma
  chave por carga), ADR-0006 (`padrao_arquivo`), ADR-0007 (colunas técnicas),
  ADR-0008 (materialização)
  - Issues #118 (estratégia de histórico) e #66
  - `dbt/mcid/models/docs/entregas/issue-118-entrega-final.md`,
  `dbt/mcid/models/mcmv_historico_dbt/`

## Contexto

A issue #118 definiu a estratégia de histórico com um piloto
(`historico_mcmv_serie_temporal_snapshot`): preservar os arquivos na raw e na
staging, rastrear a origem por `snapshot_date` e `source_file`, criar `hash_linha`
para detectar mudança, versionar no estilo SCD2 (`dt_valid_from`, `dt_valid_to`,
`is_current`), reprocessar sem sobrescrever e reter por tempo indeterminado. Os
modelos de prata do eixo histórico usam esses campos em 56 pontos.

Desde então, três decisões mudaram o terreno:

- a raw guarda cada carga numa chave própria e nunca é sobrescrita (ADR-0005);
- a bronze guarda todas as cargas, e a prata, todos os períodos de referência
  (ADR-0002 R3 e R4);
- a escolha do período vigente é regra da ouro (ADR-0002 R5).

Com isso o histórico já fica preservado pela própria estrutura das camadas. A
pergunta passa a ser o que ainda precisa de versionamento explícito.

## Decisão

### 1. Três tempos, sempre separados

| Tempo | Coluna | Pergunta que responde | De onde vem |
|---|---|---|---|
| Período de referência | `dt_referencia` | A que mês ou data o dado se refere | Do próprio dado ou do nome do arquivo (abaixo); tipado na prata |
| Carga | `carga`, `_ingested_at` | Quando o dado chegou | Da chave do lake (ADR-0005) e da conversão |
| Materialização | `_built_at` | Quando a tabela foi construída | Do dbt (ADR-0007) |

Uma análise temporal usa o período de referência. A carga serve para auditoria e
para escolher a versão de um período. Nenhum modelo mistura os dois.

Quando o período não vem no dado, ele é extraído do nome do arquivo por um grupo de
captura no `padrao_arquivo` do contrato da fonte (ADR-0006). Isso substitui o
`dt_movimento` usado em alguns pontos, que o eixo histórico já mostrou ser menos
confiável que o nome do arquivo.

### 2. A raw é a fonte de verdade do histórico

A raw é imutável e retida por tempo indeterminado (ADR-0005), como a #118 já
recomendava. Reprocessar qualquer período é reconstruir as camadas a partir dela.

### 3. Prata: um retrato por período

A prata guarda uma linha por `(chave, dt_referencia)`.

- **Fonte de carga completa:** cada período é o retrato que a fonte mandou para
  ele.
- **Reenvio do mesmo período:** a prata fica com a carga mais recente daquele
  período (deduplicação técnica, ADR-0002 R4). As versões anteriores continuam na
  bronze e na raw, para auditoria.
- **`_row_hash`** (hash das colunas de negócio) é coluna técnica opcional na
  prata, para comparar um período com o anterior sem comparar coluna por coluna.

### 4. SCD2 só onde o produto pede

Validade com início e fim (`dt_valid_from`, `dt_valid_to`, `is_current`) não é o
padrão. Ela entra na **ouro**, calculada a partir dos retratos da prata, quando o
produto precisa responder "qual era o estado vigente em tal data", como a situação
de um empreendimento ao longo do tempo.

### 5. Metadados de governança saem da linha

`estrategia_versionamento` e `regra_retencao` deixam de ser colunas repetidas em
cada linha e passam para o `meta` do modelo no dbt, de onde vão para o catálogo
(ADR-0012). `id_historico_snapshot` e `snapshot_date` são substituídos pela chave
`(chave, dt_referencia)` e pelas colunas técnicas do ADR-0007.

## Alternativas consideradas

**SCD2 em toda prata (piloto da #118).** Cada prata carregaria lógica de abertura e
fechamento de versões, o que contraria a prata genérica do ADR-0002. Como os
retratos por período já estão guardados, a validade pode ser calculada na ouro
quando for necessária.

**Guardar só as mudanças, por hash de linha.** Economiza espaço, mas complica toda
consulta de "como estava em tal mês". Fica disponível como estratégia de
materialização (ADR-0008) se o volume exigir, sem mudar o que a prata representa.

**`dbt snapshot`.** O histórico passaria a depender da frequência de execução: um
dia sem rodar é um estado que nunca existiu no snapshot. A raw já guarda toda carga,
então o snapshot seria uma segunda cópia menos completa.

## Trade-offs

| Ganha | Paga |
|---|---|
| Histórico preservado sem lógica extra em cada modelo | Prata maior nas fontes de carga completa |
| Reprocessar qualquer período a partir da raw | Reprocessamento grande custa tempo (ADR-0008) |
| Análise temporal sempre pelo período de referência, nunca pela data de carga | Toda fonte precisa ter o período definido, no dado ou no padrão de nome |
| SCD2 só onde alguém vai usar | Produtos que precisam de validade calculam na ouro |

## Consequências

- Toda prata tem `dt_referencia` não nulo e um teste de unicidade em
  `(chave, dt_referencia)` (`unique_combinacao`).
- Os modelos do eixo histórico migram dos campos do piloto da #118 para este
  padrão, junto com a migração do domínio no ADR-0002.
- O piloto (`mcmv_historico_dbt/piloto/`) fica como referência histórica e não é
  base para modelos novos.

## Plano de migração

1. Acrescentar `padrao_arquivo` com grupo de captura do período aos contratos das
   fontes de arquivo (ADR-0006).
2. Em cada domínio, quando migrar pelo ADR-0002: garantir `dt_referencia` na prata,
   a deduplicação do reenvio e o teste de unicidade.
3. No eixo histórico: substituir os campos SCD2 da prata por retratos por período
   e, onde algum painel usa `is_current`, calcular a validade na ouro.

## Validação operacional

- Toda prata tem `dt_referencia` preenchido e única por `(chave, dt_referencia)`.
- Nenhum modelo filtra ou agrega por `_ingested_at` ou `carga` para análise
  temporal (revisão de PR).
- Reconstruir um período antigo a partir da raw devolve os mesmos números da ouro
  daquele período.

## Questões em aberto

Nenhuma.
