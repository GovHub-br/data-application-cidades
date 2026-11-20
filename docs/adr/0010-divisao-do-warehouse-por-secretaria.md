# 0010. Divisão do warehouse por secretaria

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (onde o `+database` aparece hoje; sem schema `comum`; medição pelo Elementary; questões em aberto resolvidas)
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

- A camada fica no prefixo da tabela e do arquivo de modelo (`bronze_`, `prata_`,
  `ouro_`), como define o ADR-0007.
- A sigla do piloto é `snh`. As das demais secretarias vêm da lista oficial, que o
  ministério vai indicar.
- Não há schema compartilhado. Uma fonte usada por mais de uma secretaria é
  carregada no schema de cada uma, com bronze e prata próprias, e o dado se
  repete. Hoje só a SNH usa o warehouse. Como compartilhar fontes entre
  secretarias fica com o ministério, quando a segunda secretaria chegar.
- Todo produto tem uma secretaria dona, e a ouro fica no schema dela.
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
  nas tabelas do Elementary (ADR-0008) e no Postgres;
- o ministério passa a exigir mascaramento por coluna ou por linha no consumo, que
  pede o Ranger, e o Ranger pede o Trino.

**Bronze e prata comuns, só a ouro por secretaria.** Fica mais perto da proposta 1
do deck e evita duplicar fonte. Mas põe numa área sem dono as bases que só uma
secretaria usa, e o acesso à prata deixa de seguir a secretaria.

**Schema `comum` para fontes de mais de uma secretaria (versão anterior deste
ADR).** Evitaria repetir bronze e prata, mas criaria uma área sem secretaria dona
e uma migração a cada fonte que passasse a ser compartilhada. Com uma secretaria
só, não se paga.

**Schema por camada, como hoje.** Não separa secretarias.

**Lakehouse com isolamento por catálogo (Iceberg com Lakekeeper ou Polaris).**
Está no ADR-0001 como evolução futura.

## Trade-offs

| Ganha | Paga |
|---|---|
| O schema diz de quem é o dado, e o acesso segue a secretaria | O schema sozinho não separa bronze de ouro; isso fica no prefixo e nos grants (ADR-0011) |
| Um banco, sem motor federado nem Ranger | Isolamento de processamento entre secretarias não existe |
| Cada secretaria é dona de tudo que está no seu schema, inclusive bronze e prata | Uma fonte usada por duas secretarias é carregada duas vezes |
| Toda ouro tem dono | Um produto que serve duas secretarias precisa ter uma dona |

## Consequências

- O roteamento de schema do dbt passa a ser por domínio, não por camada (ADR-0007).
- Os grants passam a combinar secretaria e camada (ADR-0011).
- O Superset e o OpenMetadata passam a ver as tabelas em `snh` em vez de `bronze`,
  `prata` e `ouro`.
- No dbt, o nome do modelo é único no projeto. A cópia de uma fonte no schema de
  outra secretaria vai precisar de outro nome de arquivo, com `alias` para o nome
  da tabela. Isso se resolve quando a segunda secretaria chegar.

## Plano de migração

1. Trocar `+database: cidades` por variável.
2. Mover as tabelas para `snh`, pelo plano do ADR-0007.
3. Quando uma segunda secretaria entrar: criar o schema dela e os papéis do
   ADR-0011. A forma de compartilhar as fontes que ela usar em comum com a SNH
   fica com o ministério.

## Validação operacional

- Todo modelo materializa no schema de uma secretaria.
- Nenhum modelo tem nome de banco escrito no SQL ou no `dbt_project.yml`.

## Questões em aberto

Nenhuma.
