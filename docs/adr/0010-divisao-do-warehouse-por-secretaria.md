# 0010. Divisão do warehouse por secretaria

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (onde o `+database` aparece hoje)
- **Relacionados:**
  - ADR-0001 (arquitetura), ADR-0007 (nomenclatura), ADR-0011 (controle de acesso)
  - Deck *GovHub — Arquitetura Medalhão de Referência* seções 8 a 11

## Contexto

O laboratório usa um banco só, `cidades`, com um schema por camada (`bronze`,
`prata`, `ouro`). O piloto no ministério começa pela SNH, mas o desenho precisa
acomodar outras secretarias. O deck apresenta duas propostas:

| | Proposta 1: banco único | Proposta 2: banco por secretaria |
|---|---|---|
| Divisão | Um schema por secretaria | Um banco por secretaria |
| Exploração | Schema `playground` no banco único, com as pratas | Banco central dedicado |
| Acesso | Por schema | Por banco, mais simples de auditar |
| Isolamento | As secretarias dividem o banco | Dados, acessos e processamento próprios |
| Infraestrutura | Um banco e poucos schemas | Mais bancos, um motor de consulta (Trino) e, para mascaramento por coluna, o Apache Ranger |
| Atenção | Playground sem dono claro; lógica repetida entre secretarias | Prata duplicada; definições comuns exigem acordo |

## Decisão

**A divisão por secretaria é por schema, num banco só.**

| Schema | Conteúdo |
|---|---|
| `<sigla da secretaria>` (`snh`, `dph`) | Bronze, prata e ouro dos dados da secretaria |
| `comum` | Bronze e prata de fontes usadas por mais de uma secretaria |

- A camada fica no prefixo da tabela e do arquivo de modelo (`bronze_`, `prata_`,
  `ouro_`), como define o ADR-0007.
- Uma fonte vai para `comum` só quando a segunda secretaria passar a usá-la. Até
  lá, fica no schema da secretaria que a usa.
- `comum` não tem ouro. Todo produto tem uma secretaria dona, e a ouro fica no
  schema dela.
- Não há schema `playground`. A exploração acontece sobre as pratas, com o papel de
  leitura de análise de cada secretaria (ADR-0011). A prata genérica do ADR-0002 já
  é o material de exploração.
- Hoje praticamente todos os domínios são da SNH, então o primeiro passo é mover as
  tabelas dos schemas de camada para `snh`.

Para não fechar a porta de uma divisão por banco no futuro, nenhum modelo fixa o
nome do banco no SQL. Hoje o `+database: cidades` aparece em seis lugares do
`dbt_project.yml`: na raiz dos modelos, em `indicadores_mcmv_dbt`, em
`mcmv_historico_dbt` e nos seeds `conjuntura`, `entidades_fds` e `data_quality`. O
nome passa a vir de variável, declarado uma vez.

## Alternativas consideradas

**Banco por secretaria (proposta 2 do deck).** Isola dados, acessos e
processamento, mas exige um motor de consulta federado (o Postgres não cruza bancos
numa consulta), mais bancos para operar e, para mascaramento por coluna, o Ranger.
Hoje há uma secretaria; o custo não se paga. Volta à mesa se aparecer um destes
sinais:

- consultas de uma secretaria passam a degradar as de outra, medido em
  `lake._dbt_log` e no Postgres;
- o ministério passa a exigir mascaramento por coluna ou por linha no consumo, que
  pede o Ranger, e o Ranger pede o Trino.

**Bronze e prata comuns, só a ouro por secretaria.** Fica mais perto da proposta 1
do deck e evita duplicar fonte. Mas põe numa área sem dono as bases que só uma
secretaria usa, e o acesso à prata deixa de seguir a secretaria. O schema `comum`
resolve a fonte compartilhada sem isso.

**Schema por camada, como hoje.** Não separa secretarias.

**Lakehouse com isolamento por catálogo (Iceberg com Lakekeeper ou Polaris).**
Está no ADR-0001 como evolução futura.

## Trade-offs

| Ganha | Paga |
|---|---|
| O schema diz de quem é o dado, e o acesso segue a secretaria | O schema sozinho não separa bronze de ouro; isso fica no prefixo e nos grants (ADR-0011) |
| Um banco, sem motor federado nem Ranger | Isolamento de processamento entre secretarias não existe |
| Fonte compartilhada sem duplicação, em `comum` | Mover uma fonte para `comum` quando a segunda secretaria chega é uma migração |
| Toda ouro tem dono | Um produto que serve duas secretarias precisa ter uma dona |

## Consequências

- O roteamento de schema do dbt passa a ser por domínio, não por camada (ADR-0007).
- Os grants passam a combinar secretaria e camada (ADR-0011).
- O Superset e o OpenMetadata passam a ver as tabelas em `snh` em vez de `bronze`,
  `prata` e `ouro`.

## Plano de migração

1. Trocar `+database: cidades` por variável.
2. Mover as tabelas para `snh`, pelo plano do ADR-0007.
3. Quando uma segunda secretaria entrar: criar o schema dela, os papéis do
   ADR-0011 e, para cada fonte que ela compartilhar com a SNH, mover a bronze e a
   prata para `comum`.

## Validação operacional

- Todo modelo materializa no schema de uma secretaria ou em `comum`.
- Nenhuma ouro materializa em `comum`.
- Nenhum modelo tem nome de banco escrito no SQL ou no `dbt_project.yml`.

## Questões em aberto

1. **Siglas das secretarias.** A lista oficial de siglas que vira nome de schema
   (`snh` para o piloto já está definida).
