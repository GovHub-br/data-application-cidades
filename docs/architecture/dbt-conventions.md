# Convenções dbt do data-application-cidades

## Propósito

Este documento define o contrato de organização do projeto dbt. Convenções
existentes que não o atendam são migradas progressivamente, com aliases ou
compatibilidade de consumidores quando necessário.

## Produtos de dados

Cada diretório de primeiro nível em `dbt/mcid/models/` representa um produto
de dados independente:

| Produto | Escopo |
|---|---|
| `conjuntura_dbt` | Séries contínuas e edições trimestrais do boletim habitacional. |
| `empreendimento_far_dbt` | Empreendimentos do Fundo de Arrendamento Residencial. |
| `entidades_dbt` | Operações e entidades do Fundo de Desenvolvimento Social. |
| `metadata` | Metadados técnicos internos do projeto. |

Os produtos compartilham macros e infraestrutura, mas não devem referenciar
internamente tabelas de outro produto sem declarar a dependência, o consumidor
e o contrato no YAML.

## Camadas

Cada produto usa as camadas `bronze`, `silver` e `gold`:

- **Bronze**: cópia fiel e textual do parquet de staging; não aplica regra de
  negócio nem é camada de consumo.
- **Silver**: tratamento, tipagem, padronização e conceitos reutilizáveis.
- **Gold**: modelos curados para produto, relatório, dashboard ou consumo
  autorizado.

Modelos de qualidade ficam em `qualidade/` enquanto forem artefatos técnicos;
modelos Gold de qualidade usam o prefixo `gold_qualidade_`.

Todas as camadas são materializadas como `table` até nova decisão
arquitetural. Materialized views são proibidas neste projeto por incidente de
estabilidade registrado em 2026-08-29.

## Nomes de modelos

Os novos modelos devem usar `snake_case`, sem acentos, e um dos prefixos:

| Prefixo | Uso |
|---|---|
| `br_<fonte>__<entidade>` | Bronze fiel da fonte. |
| `int_<dominio>__<transformacao>` | Etapa intermediária com responsabilidade única. |
| `dim_<entidade>` | Dimensão reutilizável. |
| `fct_<evento>` | Fato reutilizável. |
| `rpt_<produto>__<assunto>` | Saída específica para boletim, relatório ou dashboard. |
| `gold_qualidade_<assunto>` | Medida histórica de qualidade. |

Os nomes atuais com `*_continuo_*` são compatibilidade transitória do produto
Conjuntura. Uma renomeação de model só ocorre com mapeamento de `ref`, alias
físico e conferência de consumidores.

## Macros e testes

Macros são organizadas por responsabilidade:

```text
macros/
  quality/       contratos e medições de qualidade
  security/      classificação e proteção de dados
  parsing/       normalização e conversão de formatos
  governance/    metadados e publicação controlada
  warehouse/     UDFs e particularidades do Postgres
```

- Macros públicas usam verbo e objeto: `parse_valor_siafi`,
  `fonte_lake`, `perfil_completude`.
- Macros auxiliares começam com `_`.
- Testes genéricos seguem `test_<regra>` e recebem `model` como primeiro
  argumento.
- Regras comuns da Silver serão concentradas em
  `macros/quality/silver_contract.sql`; regras de domínio continuam em testes
  singulares legíveis.

### Contrato Silver

Cada modelo Silver adotará progressivamente o teste genérico
`silver_contract`, configurado no respectivo YAML. O contrato pode declarar:

- layout completo (`expected_columns`) e aceitação ou não de novas colunas;
- campos estruturais e valores obrigatórios;
- chave de granularidade, frescor e completude mínima;
- tipo físico, domínio categórico, padrão textual, faixa numérica e ausência
  de espaços nas extremidades.

O teste devolve somente o identificador da regra e uma descrição genérica da
falha. Nunca devolve chave de negócio, valor de coluna ou linha de origem.
`sem_coluna_sensivel` é separado e bloqueante; o contrato de qualidade pode
começar como alerta enquanto cada domínio é estabilizado.

Quando duas fontes representam a mesma série, `silver_reconciliation` compara
os agregados configurados por chave, medida e tolerância. A regra só deve ser
ativada após documentar equivalência de período, universo e unidade entre as
fontes; diferenças de recorte não são falhas de qualidade.

O contrato não substitui os controles de metadados: descrições formam o
dicionário de dados, `governance/schemas.yml` compõe o catálogo, e o manifesto
dbt fornece a linhagem. A mudança histórica de colunas é mantida por
`gold_qualidade_schema` e `gold_qualidade_schema_drift`, sem registrar nomes
de campos sensíveis.

### Great Expectations e catálogo

`make gx-silver` executa as expectativas estruturais em todos os modelos
Silver e traduz as regras específicas já declaradas em `silver_contract`.
O resultado persistido não inclui valores inesperados ou linhas de dados.

`make openmetadata-catalog` executa `dbt docs generate` em diretório
temporário privado e guarda apenas a projeção semântica permitida. Quando uma
descrição YAML ainda não foi curada, aplica uma descrição determinística
baseada no nome técnico e marca `documentation_status: derived_convention`.
Esse estado é uma pendência de curadoria, não uma autorização para publicar
exemplos, mapeamentos ou SQL compilado.

`make openmetadata-sync OPENMETADATA_ARGS=--confirmar` só deve ser usado após
preencher as variáveis OpenMetadata no `.env`. Ele envia tabelas Silver/Gold e
linhagem entre entidades elegíveis; Bronze, Raw, artefatos dbt completos e
linhagem coluna a coluna ficam fora do envio por segurança.

### Edições do boletim

O congelamento é feito por **snapshot do dbt** (`make conjuntura-congelar`),
não por script. São 21 snapshots em `conjuntura_continuo_snapshots`, um por
quadro do boletim, com chave `edicao` + rótulo da linha e `strategy='check'`.

Existiu um `scripts/conjuntura/congelar_edicao.py` que copiava a safra para um
schema próprio. Foi removido em 2026-08-31: mantinha um segundo mecanismo de
safra em paralelo ao snapshot, e dois mecanismos divergem. O snapshot resolve
melhor porque guarda o histórico das revisões (`dbt_valid_from`/`dbt_valid_to`),
não apenas o retrato de uma data.

## Materialização e referências

- Um model só lê outro model por `ref()` e uma origem por `source()`.
- Consultas diretas a schema externo exigem uma `source` declarada ou uma
  exceção documentada no model.
- `select *` é permitido apenas na Bronze fiel; Silver e Gold devem enumerar
  colunas ou usar macro que explicite a exclusão permitida.

## Metadados obrigatórios

Todo model persistido terá descrição e metadados de produto, classificação,
frequência, granularidade e termos de glossário. Estes metadados são a fonte
de verdade para OpenMetadata, documentação e consumidores autorizados.
