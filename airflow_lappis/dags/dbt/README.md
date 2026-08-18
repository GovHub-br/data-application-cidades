# Guia de documentacao dos projetos dbt

Este guia define o padrao para documentar modelos, colunas, testes, tags e
metadados nos arquivos `schema.yml` dos projetos `ipea`, `mcid` e `mir`.

> **Versao usada neste repositorio:** o `requirements.txt` fixa
> `dbt-postgres==1.7.13`. Os exemplos principais abaixo sao compativeis com essa
> versao. A documentacao mais recente do dbt usa algumas estruturas diferentes;
> veja [Compatibilidade entre versoes](#compatibilidade-entre-versoes).

## Onde fica a documentacao

Coloque o `schema.yml` perto dos arquivos SQL que ele documenta:

```text
models/
  empreendimento_far_dbt/
    bronze/
      consolidado.sql
      cadastro_pj.sql
      schema.yml
```

O nome `schema.yml` e uma convencao do repositorio, nao uma exigencia do dbt.
Qualquer arquivo `.yml` dentro dos caminhos do projeto pode declarar
propriedades. Neste repositorio, use sempre `schema.yml` (extensao `.yml`) para
facilitar busca e revisao.

O `name` do modelo deve ser exatamente o nome do arquivo SQL sem `.sql`:
`consolidado.sql` vira `name: consolidado`.

## Modelo recomendado

```yaml
version: 2

models:
  - name: consolidado
    description: >
      Uma linha por proposta do GFAR. Consolida dados cadastrais, contratuais e
      de localizacao usados no acompanhamento dos empreendimentos FAR.
    config:
      tags:
        - bronze
        - empreendimento_far
        - mcid
    meta:
      openmetadata:
        tier: "Tier.Tier3"
        tags:
          - "Certification.Bronze"
    columns:
      - name: id_proposta
        description: Identificador unico da proposta no GFAR.
        tests:
          - not_null
          - unique

      - name: uf
        description: Sigla da unidade federativa do empreendimento.
        tests:
          - accepted_values:
              values:
                - AC
                - AL
                - AP
                # ...demais UFs

      - name: criado_em
        description: Data e hora em que o registro foi carregado na camada raw.

    tests:
      - row_count_match:
          source_table: __dados_brutos.novo_mcmv_far_consolidado
          target_table: empreendimento_far.consolidado
```

### Ordem dos campos

Use esta ordem para tornar os arquivos previsiveis:

1. `version: 2` no inicio do arquivo;
2. tipo de recurso, normalmente `models:`;
3. `name` e `description` do modelo;
4. `config` para configuracoes interpretadas pelo dbt;
5. `meta` para integracoes e metadados livres;
6. `columns` com nome, descricao e testes de cada coluna;
7. `tests` no nivel do modelo para regras que envolvem a tabela ou mais de uma
   coluna.

A ordem nao muda o resultado do dbt; ela e uma convencao de legibilidade.

## Indentacao YAML

Use **dois espacos por nivel** e nunca tabulacao. Uma lista comeca com `-` e os
campos do item ficam dois espacos adiante:

```yaml
models:                         # nivel 0
  - name: meu_modelo            # 2 espacos + item da lista
    description: Descricao.     # mesmo objeto de "name"
    columns:
      - name: minha_coluna      # lista dentro de "columns"
        description: Descricao.
        tests:
          - not_null
```

Errado — `tests` deixou de pertencer a coluna:

```yaml
columns:
  - name: minha_coluna
    description: Descricao.
tests:
  - not_null
```

Correto — teste da coluna:

```yaml
columns:
  - name: minha_coluna
    description: Descricao.
    tests:
      - not_null
```

Correto — teste da tabela/modelo:

```yaml
models:
  - name: meu_modelo
    columns:
      - name: minha_coluna
        description: Descricao.
    tests:
      - meu_teste_de_tabela
```

### Textos, aspas, `>` e `|`

- Use texto simples para descricoes curtas.
- Use `>` para uma descricao longa virar um unico paragrafo. Quebras de linha
  do YAML sao convertidas em espacos.
- Use `|` quando as quebras de linha precisam ser preservadas, por exemplo em
  listas Markdown.
- Coloque o texto entre aspas quando houver caracteres que o YAML pode
  interpretar, especialmente `:`, `{}`, `[]`, `#`, valores como `true`, `false`
  e datas.

```yaml
description: >
  Consolida os pagamentos e preserva uma linha por contrato, competencia e
  favorecido.

description: |
  Regras aplicadas:

  - remove registros sem contrato;
  - converte o valor para numeric(15,2);
  - mantem apenas a carga mais recente.

description: "Situacao exibida no formato: codigo - descricao."
```

Descricoes aceitam Markdown. Para textos longos ou reutilizados, use um bloco
`docs` em arquivo `.md` e referencie-o com `description: '{{ doc("nome") }}'`.

## O que documentar

### Modelo

A descricao deve permitir que alguem use a tabela sem precisar ler o SQL.
Informe, quando aplicavel:

- o que uma linha representa (a granularidade);
- finalidade e dominio de negocio;
- principais fontes;
- transformacoes ou filtros que alteram o significado;
- frequencia ou regra de atualizacao;
- limitacoes relevantes.

Evite descricoes circulares como "tabela de contratos". Prefira:
"Uma linha por contrato e competencia; consolida valores programados e pagos do
ComprasGov para acompanhamento da execucao financeira".

### Coluna

Documente todas as colunas expostas pelo `select` final, usando exatamente o
mesmo nome e a mesma grafia. Explique significado, unidade, formato, origem ou
regra de calculo — nao apenas repita o nome.

```yaml
- name: percentual_execucao_fisica
  description: >
    Percentual acumulado da obra, de 0 a 100, referente a medicao mais recente.
```

Para chaves, diga qual entidade identificam e se sao unicas. Para datas, diga
qual evento representam. Para valores, informe moeda/unidade. Para codigos e
status, descreva o dominio ou adicione `accepted_values`.

O campo opcional `data_type` documenta o tipo esperado. No dbt ele se torna
obrigatorio quando um contrato de modelo e habilitado; fora disso, nao substitui
um teste de tipo executavel.

## Tags dbt e metadados: nao sao a mesma coisa

### Tags selecionaveis pelo dbt

Tags operacionais devem ficar em `config.tags`:

```yaml
config:
  tags:
    - bronze
    - empreendimento_far
    - mcid
```

Elas podem selecionar recursos nos comandos:

```bash
dbt run --select tag:bronze
dbt build --select tag:empreendimento_far
dbt test --select tag:mcid
```

Padrao de tags deste repositorio:

- camada: `bronze`, `silver` ou `gold`;
- dominio/produto: por exemplo `empreendimento_far`, `contratos` ou `ted`;
- projeto/orgao: `mcid`, `ipea` ou `mir`.

Use nomes em minusculas e `snake_case`, sem espacos ou acentos. Nao use
`meta.tags` para selecionar modelos: `meta` e um dicionario livre e nao equivale
a configuracao `tags` do dbt.

### Metadados do OpenMetadata

Nos modelos integrados ao OpenMetadata, este repositorio adota:

```yaml
meta:
  openmetadata:
    tier: "Tier.Tier3"
    tags:
      - "Certification.Bronze"
```

Convencao atual:

| Camada | Tier | Tag do OpenMetadata |
| --- | --- | --- |
| Bronze | `Tier.Tier3` | `Certification.Bronze` |
| Silver | `Tier.Tier2` | `Certification.Silver` |
| Gold | `Tier.Tier1` | `Certification.Gold` |

Esses valores pertencem a integracao com OpenMetadata. Eles nao substituem
`config.tags` e nao tornam o modelo selecionavel por `tag:` no dbt.

## Testes como parte da documentacao

Na versao atual do repositorio, use `tests:`. Os quatro testes genericos nativos
do dbt sao:

- `not_null`: nao permite nulos;
- `unique`: nao permite duplicidade;
- `accepted_values`: restringe valores a uma lista;
- `relationships`: valida uma chave contra outro modelo ou fonte.

```yaml
columns:
  - name: apf
    description: Chave do empreendimento, normalizada para oito digitos.
    tests:
      - not_null
      - unique

  - name: id_municipio
    description: Identificador do municipio associado ao empreendimento.
    tests:
      - relationships:
          to: ref('dim_municipio')
          field: id_municipio
```

Testes especificos do projeto podem ficar no nivel da coluna ou do modelo,
conforme a assinatura da macro. Mantenha os argumentos sob o nome do teste:

```yaml
tests:
  - verificacao_tipagem:
      nome_tabela: dados_abertos.parlamentares
      nome_coluna: id_parlamentar
      tipo_esperado: integer
```

Teste nao substitui descricao: `unique` prova unicidade, mas nao explica o que a
chave identifica.

## Outros recursos

O mesmo arquivo de propriedades pode documentar outros tipos de recurso:

```yaml
version: 2

sources:
  - name: dados_brutos
    description: Dados recebidos dos sistemas de origem antes da transformacao.
    schema: __dados_brutos
    tables:
      - name: novo_mcmv_far_consolidado
        description: Extracao consolidada de propostas FAR do sistema GFAR.
        columns:
          - name: id_proposta
            description: Identificador da proposta no sistema de origem.

seeds:
  - name: partidos_map
    description: Mapeamento de siglas partidarias de origem para a sigla canonica.

macros:
  - name: parse_financial_value
    description: Converte valores financeiros textuais para numeric(15,2).
    arguments:
      - name: value
        type: text
        description: Valor textual nos formatos aceitos pela macro.
```

Nao misture `models`, `sources`, `seeds` e `macros` dentro uns dos outros; cada
tipo e uma chave de nivel superior abaixo de `version: 2`.

## Compatibilidade entre versoes

A documentacao atual do dbt usa `data_tests:` como nome preferido. Como este
repositorio fixa dbt 1.7.13, o padrao local continua sendo `tests:` ate a versao
ser atualizada. Arquivos novos nao devem misturar as duas formas.

Tambem houve mudancas recentes para mover `meta` e algumas `tags` para dentro de
`config`. No dbt 1.7, mantenha a forma usada nos exemplos deste guia:

```yaml
config:
  tags:
    - bronze
meta:
  openmetadata:
    tier: "Tier.Tier3"
```

Ao atualizar o dbt, faca a migracao em todo o projeto e valide com `dbt parse`;
nao atualize arquivos isolados copiando a sintaxe da documentacao mais recente.
Em dbt 1.10.5+, os argumentos de testes genericos passam a ser explicitamente
aninhados em `arguments:`.

## Validacao e geracao da documentacao

Execute os comandos a partir da pasta do projeto (`mcid`, `ipea` ou `mir`):

```bash
# valida YAML, Jinja, referencias e propriedades sem executar modelos
dbt parse

# compila o projeto
dbt compile

# executa modelos e testes relacionados
dbt build --select nome_do_modelo

# gera e abre o site local de documentacao
dbt docs generate
dbt docs serve
```

Para gravar as descricoes tambem como comentarios no banco, o dbt oferece
`persist_docs`. Habilite apenas depois de confirmar que o usuario do dbt possui
permissoes para criar comentarios:

```yaml
models:
  nome_do_projeto:
    +persist_docs:
      relation: true
      columns: true
```

## Checklist de revisao

- [ ] O arquivo comeca com `version: 2`.
- [ ] O YAML usa dois espacos e nenhuma tabulacao.
- [ ] Cada `name` corresponde ao recurso ou coluna real.
- [ ] O modelo explica granularidade, finalidade, fontes e regras relevantes.
- [ ] Todas as colunas expostas possuem descricoes uteis.
- [ ] Tags operacionais estao em `config.tags`.
- [ ] Metadados do OpenMetadata estao em `meta.openmetadata`.
- [ ] Chaves e dominios importantes possuem testes.
- [ ] A sintaxe usa `tests:` enquanto o projeto permanecer no dbt 1.7.13.
- [ ] `dbt parse` e `dbt build --select nome_do_modelo` passam antes do merge.

## Referencias oficiais

- [Documentacao de projetos dbt](https://docs.getdbt.com/docs/build/documentation)
- [Propriedade `description`](https://docs.getdbt.com/reference/resource-properties/description)
- [Propriedade `columns`](https://docs.getdbt.com/reference/resource-properties/columns)
- [Data tests](https://docs.getdbt.com/reference/resource-properties/data-tests)
- [Configuracao `tags`](https://docs.getdbt.com/reference/resource-configs/tags)
- [Configuracao `meta`](https://docs.getdbt.com/reference/resource-configs/meta)
- [Propriedade `config`](https://docs.getdbt.com/reference/resource-properties/config)
