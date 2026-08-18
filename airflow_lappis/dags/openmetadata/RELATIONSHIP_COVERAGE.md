# Cobertura de linhagem e relações semânticas do MCID

Auditoria realizada em 2026-07-23 sobre as 74 tabelas aprovadas nas recipes de
Postgres do OpenMetadata. A fonte de verdade legível por máquina está em
`semantic_relationships/mcid.yaml`.

## Escopo e segurança

| Item | Resultado |
|---|---:|
| Tabelas | 74 |
| Colunas documentadas | 1.215/1.215 |
| Modelos com descrição dbt | 74/74 |
| Constraints PK/FK/UNIQUE no PostgreSQL | 0 |
| Índices no escopo | 0 |
| Arestas dbt `ref()` após correção | 65 |
| Arestas dbt `source()` após correção | 34 |
| Cláusulas JOIN inventariadas | 53 em 19 modelos |
| Relações observadas validadas | 18 |
| Contratos `relationships` do dbt | 6 |
| Relações candidatas | 3 |
| Grupos de pesquisa | 14 |
| Colunas com suspeita de PII por metadados | 71 |

A exploração do banco foi executada em transação somente leitura. Foram
consultados metadados, contagens, nulidade, cardinalidade e sobreposição de
chaves normalizadas. Nenhuma amostra, valor bruto, hash reutilizável, APF, CNPJ,
nome, endereço ou coordenada foi retornado ou persistido.

## Como interpretar as relações

- **Linhagem dbt** indica fluxo real de transformação entre `source()`, `ref()`
  e o modelo produzido. Ela aparece na aba **Lineage** do OpenMetadata.
- **Contrato dbt** é um teste `relationships` que passou no banco atual. Ele
  comprova ausência de órfãos no recorte testado, mas não cria uma FK física.
- **Relação observada** foi comprovada por cobertura/cardinalidade agregada. Ela
  pode ser usada como aresta semântica, respeitando a normalização e o recorte.
- **Relação candidata** ajuda descoberta e recuperação, mas não autoriza fundir
  entidades automaticamente.
- **Campo de pesquisa** serve para filtro, busca exata, fuzzy ou espacial. Igualdade
  nesse campo, sozinha, não é identidade de entidade.

Relações semânticas e candidatas não são publicadas como lineage, justamente
para o GraphRAG não confundir associação com dependência de transformação.

## Correções de linhagem dbt

Quatro modelos Silver liam uma tabela Bronze local por `source()`, escondendo
a dependência real. Foram trocados para `ref()` sem alterar a relação física ou
a lógica SQL:

| Silver | Upstream local |
|---|---|
| `silver_fgv_icst` | `bronze_fgv_icst` |
| `silver_fgv_incc_m` | `bronze_fgv_incc_m` |
| `silver_ibge_sinapi` | `bronze_ibge_sinapi` |
| `silver_imob_infomoney` | `bronze_imob_infomoney` |

O manifest regenerado passou a apontar os quatro upstreams como
`model.mcid.bronze_*`, permitindo que o OpenMetadata apresente
Bronze → Silver corretamente.

Sete relações físicas de `conjuntura_bronze` não possuem modelo SQL local e
continuam corretamente como `source()`: novos financiamentos ABECIP,
financiamentos habitacionais ABECIP/SBPE, ABRAMAT, balanços de empresas, CBIC,
FGTS e ticket médio de empresas.

## Contratos dbt habilitados e validados

Todos os testes abaixo passaram em 2026-07-23:

| Origem | Destino | Chave |
|---|---|---|
| `fds_obra_mensal` | `fds_cadastro_pj` | `apf` |
| `fds_trabalho_social` | `fds_cadastro_pj` | `apf` |
| `fds_evolucao_financeira` | `fds_empreendimento` | `apf` |
| `fds_ficha_empreendimento` | `fds_empreendimento` | `apf` |
| `fds_evolucao_financeira_chart` | `fds_ficha_empreendimento` | `apf` |
| `silver_ibge_pnadc_ocupados_construcao` | `silver_ibge_pnadc_rendimento_construcao` | `periodo` |

Também passaram `unique`/`not_null` para as chaves-pai FDS aplicáveis, os dois
períodos PNAD e `data_referencia` de ICST, INCC-M, SINAPI e IMOB.

Não foi aplicado `relationships` ao bronze financeiro FDS inteiro: ele contém
operações legadas fora do recorte Novo MCMV. A relação válida usa a raiz de
seis dígitos dentro do modelo Silver e permanece documentada como transformação
condicional.

## Relações observadas principais

### FAR

- `cadastro_pj.apf` cobre os 823 APFs observados em `empreendimento`,
  `ficha_empreendimento`, `obra_mensal` e
  `execucao_fisica_financeira_chart`.
- As séries `evolucao_financeira` e `evolucao_financeira_chart` formam o
  subconjunto de 651 APFs que possui evolução financeira.
- Todos os 823 `cadastro_pj.id_proposta` aparecem em
  `consolidado.id_proposta`; o consolidado possui universo maior e repetição,
  portanto o modelo reduz o pai com `row_number()` antes do JOIN.
- Todos os APFs cadastrais aparecem na base ampla
  `dados_prioritarios_caixa`, mas ela contém APFs fora do recorte e repetição.

### FDS

- `fds_cadastro_pj`, `fds_empreendimento`, `fds_ficha_empreendimento` e
  `fds_obra_mensal` compartilham os mesmos 343 APFs no recorte atual.
- `fds_trabalho_social` contém 89 APFs, todos presentes no cadastro.
- As séries financeiras contêm 111 APFs, todos presentes no empreendimento.
- `fds_panorama_entidade.cnpj_eo` é a referência única para 172 CNPJs de EO e
  cobre os CNPJs válidos usados pelo empreendimento e pela ficha.
- `fds_cadastro_pj.cod_ibge` e `fds_empreendimento.cod_ibge` compartilham os
  220 códigos municipais observados.

As 18 relações observadas, com cardinalidade, contagens e cobertura em cada
ponta, estão completas no catálogo YAML.

## Relações candidatas

1. `fds_int_059_caixa_pj.apf` ↔ `fds_cadastro_pj.apf`: há 304 APFs em comum;
   o universo da INT 059 é mais amplo e existe uma divergência no recorte, por
   isso não é FK obrigatória.
2. `dados_prioritarios_caixa.apf` ↔
   `fds_dados_prioritarios_entregas.apf`: há 3.491 APFs em comum, cobrindo
   78,6% das chaves FAR e 30,3% das chaves FDS dessas bases amplas. É uma aresta
   transversal relevante para pesquisa, mas precisa de APF/localização/programa
   para confirmar identidade.
3. `fds_cadastro_pj.eo_cnpj` ↔ `fds_panorama_entidade.cnpj_eo`: a ligação é
   forte apenas após normalizar CNPJ válido; o bronze possui CNPJs com 13
   dígitos e sentinelas.

## Grupos de ligação e pesquisa

| Grupo | Membros | Uso recomendado | Cautela |
|---|---:|---|---|
| APF | 22 | busca exata, resolução e JOIN | não igualar APF completo à raiz de 6 dígitos sem transformação |
| Rótulo APF | 5 | recuperação textual | nunca usar o texto concatenado como chave |
| CNPJ | 19 | resolução de organização por papel | proteger o valor e não misturar EO, proponente, tomador e construtora |
| ID da proposta | 3 | busca exata por programa | FAR e FDS não mostraram universo compartilhado |
| Código IBGE | 6 | ligação territorial | preferível ao nome do município |
| UF | 18 | filtro/facet | não identifica empreendimento |
| Município | 13 | filtro e fuzzy search | ambíguo sem Código IBGE/UF |
| Nome do empreendimento | 10 | candidato de resolução | exigir APF ou localização corroborando |
| Nome de organização | 14 | recuperação por papel | pode conter nome/razão social |
| Endereço | 9 | pesquisa restrita | mascarar; não enviar bruto a embeddings |
| Geolocalização | 32 | candidato espacial | arredondar/agrupar conforme autorização |
| Datas | 244 | linha do tempo e filtro | igualdade de data não indica identidade |
| Códigos de domínio | 71 | filtro/facet com contexto | códigos iguais podem ter domínios diferentes |
| Nomes genéricos | 21 | fuzzy search após revisão | nunca criar JOIN automático |

## Inventário dos JOINs

As 53 cláusulas estão estruturadas em `model_join_logic` no catálogo:

- 30 JOINs em 10 modelos de conjuntura;
- 17 JOINs em 6 modelos FAR;
- 6 JOINs em 3 modelos FDS.

O inventário preserva os predicados exatos, incluindo chaves compostas
`(ano, trimestre)`, `(apf, mês)`, JOINs por `periodo`, relações de UF com a
dimensão IBGE e a transformação `LEFT(apf, 6)` para as séries financeiras.
JOINs entre CTEs do mesmo modelo são documentados para explicar grão e fanout,
mas não viram arestas artificiais entre tabelas.

## Risco de qualidade bloqueando contratos FAR

Há duplicação exata já materializada em duas fontes:

| Relação | Linhas | Linhas distintas completas | Chave distinta |
|---|---:|---:|---:|
| `empreendimento_far.dados_prioritarios_caixa` | 8.886 | 4.443 | 4.443 APFs |
| `__dados_brutos.api_ibge_uf` | 54 | 27 | 27 siglas |

O fanout propagou para `empreendimento` e `ficha_empreendimento` (2× por APF),
`evolucao_financeira` (2× por APF/mês) e
`evolucao_financeira_chart` (até 8× por APF/mês).

Por isso, os vínculos FAR estão documentados, mas os testes fortes de
unicidade/FK do FAR não foram habilitados. A correção deve deduplicar
`dados_prioritarios_caixa` por APF usando a versão mais recente e reduzir a
dimensão IBGE a uma linha por sigla, seguida de reconstrução controlada dos
modelos dependentes.

## Publicação no OpenMetadata

A task `sync_mcid_semantic_relationships`, executada depois de
`dbt_metadata`, cria duas Custom Properties de Table:

- `mcidSemanticRelationships`: Markdown com chaves, relações, JOINs, evidência
  agregada e cautelas por tabela;
- `mcidRelatedTables`: referências navegáveis para as outras tabelas citadas.

O sincronizador é idempotente, preserva outras chaves de `extension`, falha se
uma tabela/coluna do catálogo não existir e publica explicitamente listas
vazias para evitar metadado obsoleto. O catálogo não contém valores de negócio.

Na interface, abra uma tabela do serviço `Cidades`, acesse **Custom
Properties** e confira essas duas propriedades. Use a aba **Lineage** somente
para dependências dbt e a aba **Glossary Terms/Tags** para APF, CNPJ, Código
IBGE, UF, MCMV, FAR e FDS.
