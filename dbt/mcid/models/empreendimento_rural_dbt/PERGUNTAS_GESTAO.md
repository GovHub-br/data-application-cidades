# Perguntas de gestão — MCMV Rural

Saída da oficina com a equipe da SNH/DHR. A oficina validou os dois fluxos (político e
político × dado) e mudou a estrutura: o que o pôster tratava como **um** fluxo de 8 etapas
passa a ser **três linhas de programa**, cada uma com **cinco etapas**.

Este documento registra as perguntas que a equipe quer responder com dado, e — para cada
uma — se ela é respondível hoje, com qual tabela, e o que falta. A avaliação é técnica e
foi feita contra o pipeline que existe, não contra o desejável.

---

## A nova estrutura

**Três linhas**

| Linha | O que é | Situação no pipeline |
|---|---|---|
| **Rural (PNHR)** | A linha principal, Novo MCMV Rural + PNHR histórico | Implementada — 12 bronze, 13 silver, 9 gold |
| **FNIS** | ⚠️ *confirmar*: a equipe indicou ser o mesmo que "sub‑50" (propostas de menos de 50 UH) | Não separada no pipeline |
| **Pró‑Moradia** | Linha distinta, com pergunta própria | Ausente do pipeline — nenhuma fonte declarada |

**Cinco etapas por linha**

```
Habilitação → Enquadramento → Seleção → Contratação → Execução
```

### Como as cinco etapas se relacionam com as oito do pôster

Mapeamento proposto, **para validar na próxima conversa** — foi derivado do conteúdo das
etapas, não ditado pela equipe:

| Etapa nova | Etapas do pôster | Observação |
|---|---|---|
| — | 0 Orçamento (OGU), 1 Planejamento | Ficam **acima** das linhas: são do programa inteiro, não de uma linha |
| **Habilitação** | 2 Habilitação das EOs | Correspondência direta |
| **Enquadramento** | 3 Seleção (parte "AF enquadra") + 4 Beneficiários (parte "AF enquadra cada CPF") | O pôster juntava enquadrar e selecionar numa etapa; a oficina separou — e essa separação é substantiva, porque enquadrar é ato do AF e selecionar é ato do MCID |
| **Seleção** | 3 Seleção (parte "MCID hierarquiza / resultado abre 180 dias") | |
| **Contratação** | 5 Contratação e obra (parte contrato/termo/parcelas) | |
| **Execução** | 5 (obra) + 6 Entrega + 7 Pós‑ocupação | Entrega e pós‑ocupação viram fases da execução, não etapas próprias |

---

## Linha Rural (PNHR) — quatro perguntas

A equipe declarou "uma pergunta por etapa", mas foram enunciadas **quatro** perguntas para
**cinco** etapas. A quarta cobre contratação e execução juntas. **Falta uma pergunta**, e
vale decidir na próxima conversa se ela é de contratação (separada da execução) ou se a
quarta se desdobra em duas.

---

### 1. Habilitação
> **O perfil da entidade habilitada, histórico de pedidos, construção etc.**

**Respondível hoje: parcialmente — só a EO que virou contrato.**

| | |
|---|---|
| Onde está o que temos | `silver_cadastro_pj` (`eo_nome`, `eo_cnpj`, investimento, prazo) e, para o legado, `silver_pnhr_caixa` / `silver_pnhr_bb` |
| O que falta | A **base de habilitação do AF** (sistema Atender Habitação, níveis F–A da Portaria 925/2025) não chega à plataforma. Sem ela não existe "entidade habilitada" — existe "entidade contratada" |
| Consequência | A pergunta como enunciada não é respondível: quem se habilitou e **não** contratou é invisível. E `silver_cadastro_pj` cobre ~1,2% da carteira, então até o recorte "contratou" é parcial |
| Para responder | Coletar a base de habilitação (nível, validade, data). É fonte nova, não transformação |

O "histórico de pedidos e construção" da EO existe em parte no legado PNHR (INT065/INT057
trazem o histórico de empreendimentos por entidade), mas esse feed **parou em
2024‑11‑29** — o PNHR é programa encerrado. Para o Novo MCMV Rural não há histórico.

---

### 2. Enquadramento
> **O motivo de enquadramento e não‑enquadramento das entidades / propostas.**

**Respondível hoje: não.**

| | |
|---|---|
| Onde está o que temos | Nada. O pôster já registra "SEM DADO — propostas enquadradas não selecionadas" |
| O que falta | O **motivo** vive na análise do AF (jurídica, técnica, experiência em obras rurais; CadÚnico/CAF, vistoria de campo, teto por UH) e nunca é exportado. Chega à plataforma só o resultado positivo, e só depois de virar contrato |
| Para responder | Fonte nova, com o par (proposta, decisão, motivo). É a pergunta com a maior distância entre o que se quer e o que existe |

Esta é a pergunta mais valiosa e a mais barata de perder: se o motivo de recusa não for
capturado **no momento da análise**, ele não é recuperável depois.

---

### 3. Seleção
> **Abrangência e quantidade de seleções passadas (proposta, protocolo), histórico.**

**Respondível hoje: abrangência sim, histórico não.**

| | |
|---|---|
| Abrangência | `gold_panorama_estadual` e `gold_mapa_nacional` entregam por UF e por região. Município via `gold_ficha_empreendimento.municipio_uf` |
| Protocolo / proposta | `silver_prioritarios_snh` tem `id_operacao_snh`; o pôster registra `NU_PROTOCOLO_SISAD` como chave da etapa. Mas a tabela é a espinha dorsal do que foi **selecionado** — proposta que não avançou não está lá |
| Histórico | **Estruturalmente indisponível.** Ver a seção "O problema do histórico" abaixo |

---

### 4. Contratação **e** Execução
> **Uma comparação de execução financeira e física, histórico por entidade.**

**Respondível hoje: por empreendimento sim, por entidade quase não, histórico não.**

| | |
|---|---|
| Por empreendimento | É a parte mais bem coberta do pipeline: `gold_execucao_fisica_financeira` (físico × financeiro), `gold_evolucao_financeira` (série de desembolso) e `gold_ficha_empreendimento` (execução, ritmo, R$/UH) |
| Por entidade | A EO só existe em `silver_cadastro_pj` (~1,2% da carteira) e no legado PNHR. **Não há como agregar execução por entidade na carteira atual** — falta o vínculo EO ↔ empreendimento para a maioria |
| Histórico | Ver abaixo |

Duas ressalvas de leitura, encontradas ao investigar divergências reais e já corrigidas no
dbt, mas que mudam como a resposta deve ser lida:

- **Físico e financeiro medem em regimes diferentes.** O físico é uma medição corrente com
  data de referência (uma por empreendimento); o financeiro é uma série mensal. Comparar os
  dois exige declarar a data-base do físico — está em `dt_referencia_execucao_fisica`.
- **Desembolso tem duas medidas que não fecham.** `valor_desembolsado` é estoque (posição
  informada pelos prioritários) e `vr_acumulado_liberacoes` é fluxo (soma da série). Quando
  divergem, é a série que está incompleta: o INT055 só traz o que CAIXA e BB reportaram
  por aquela integração, e o feed para em 2025‑03. A diferença está em
  `diferenca_estoque_menos_serie`.

---

## Linha Pró‑Moradia — uma pergunta

> **Conseguir linkar qual proposta está em qual contrato.**

**Respondível hoje: não. A linha não existe no pipeline.**

Nenhuma fonte do Pró‑Moradia está declarada em `scripts/bronze_familias.yml`, e nenhum
model a referencia. Responder exige:

1. Identificar a origem (qual sistema guarda proposta e contrato do Pró‑Moradia).
2. Declarar a família na bronze.
3. Uma silver que estabeleça a chave de ligação proposta ↔ contrato.

A pergunta é de **rastreabilidade**, não de indicador: o que se quer é a correspondência,
não uma métrica sobre ela. Isso simplifica — uma tabela de duas colunas bem definidas
resolve. O trabalho todo está em achar a fonte e definir a chave.

---

## Linha FNIS — sem perguntas declaradas

A oficina identificou a linha mas não enunciou pergunta. Duas coisas a resolver antes:

1. **Confirmar que FNIS = "sub‑50"** (propostas de menos de 50 UH). Se for isso, não é
   fonte nova: é um **recorte** da carteira que já temos, e dá para atender com uma coluna
   de linha na `silver_empreendimento` mais filtros nas gold.
2. Se for fundo distinto com fluxo próprio, é o mesmo trabalho do Pró‑Moradia.

---

## O problema do histórico

**Três das cinco perguntas pedem histórico, e a arquitetura atual não guarda nenhum.**

A bronze é **full refresh**: o `staging_para_bronze.py` dropa e recria cada tabela a partir
do arquivo mais recente da `staging/`. Some a isso que os exports canônicos do SharePoint
(prioritários SNH, trabalho social) são **sobrescritos in place** na origem. O efeito é que
o banco tem sempre a foto de hoje e nenhuma foto de ontem.

Isso não se resolve escrevendo SQL diferente na gold. Nenhuma consulta recupera um estado
que não foi gravado. Enquanto a bronze for full refresh:

- "quantidade de seleções passadas, histórico" → só o ciclo corrente
- "histórico por entidade" → só a posição atual
- "histórico de pedidos" da EO → só o legado PNHR, que congelou em 2024

Três caminhos, do mais barato ao mais completo:

| Caminho | O que envolve | O que passa a responder |
|---|---|---|
| **Guardar o parquet de cada competência** na `staging/` (já acontece para os feeds datados) e carregar a bronze com a competência na chave, em vez de substituir | Mudança no `bronze_familias.yml` e no carregador: append por competência em vez de drop/create | Série mensal de qualquer medida, para o Novo MCMV Rural |
| **Snapshot dbt** (`dbt snapshot`, SCD2) sobre as silver que interessam | Recurso nativo do dbt, arquivos novos em `snapshots/` | Quando cada campo mudou, e o valor anterior |
| **Snapshot na origem**: pedir que os exports canônicos do SharePoint sejam datados em vez de sobrescritos | Conversa com quem gera o export | Histórico dos prioritários SNH e do trabalho social — as duas fontes onde hoje não há saída técnica |

O primeiro caminho é o de melhor retorno e resolve a maior parte. O terceiro é o único que
resolve os snapshots sobrescritos, e é conversa, não código.

---

## Resumo: o que dá para fazer sem fonte nova

| Pergunta | Hoje | Falta |
|---|---|---|
| 1. Perfil da EO habilitada | parcial (só quem contratou) | base de habilitação do AF |
| 2. Motivo de (não) enquadramento | não | fonte nova, capturada na análise |
| 3. Abrangência e histórico de seleções | abrangência sim | histórico (arquitetura) |
| 4. Execução física × financeira | por empreendimento sim | vínculo EO para agregar; histórico |
| 5. Pró‑Moradia: proposta ↔ contrato | não | linha inteira |

**Duas das cinco** (2 e 5) exigem fonte que ninguém coleta hoje — e nesses casos o
gargalo é acordo com CAIXA/AF, não engenharia. **Uma** (4) é entregável já, no grão de
empreendimento. **Duas** (1 e 3) melhoram muito com o snapshot da bronze, que é trabalho
nosso e não depende de terceiros.

---

## Anotações à mão nos pôsteres — a confirmar

As fotos trouxeram marcações que **não consegui ler com segurança**. Registrei o que
identifiquei; o que estiver errado aqui é minha leitura, não a decisão da equipe:

- Post‑it na linha da **CAIXA — Gestora Operacional**, com setas para "ETAPA 1A" e
  "ETAPA 4A" — parece renumeração ou desdobramento de etapas. **Não decifrei o texto.**
- Margem esquerda: "OU 1A", "4A" repetidos em duas linhas de ator.
- Entre as etapas 2 e 3: as palavras **HABILITAÇÃO**, **ENQUADRAMENTO**, **PRIORIZAÇÃO**,
  **CONTRATAÇÃO** escritas soltas — consistente com a nova divisão em cinco etapas.
- "**+ SOCIAL**" e "FASE TRABALHO SOCIAL" perto da etapa 3 — o trabalho social apareceria
  mais cedo do que o pôster mostra (hoje está em pós‑ocupação)?
- Sobre "Enquadra proposta": algo como "**verifica documentos da obra + engenharia +
  social**".
- Riscado sobre "Cadastra‑se no sistema do AF" e a palavra "não se aplica".
- "**INDÍGENA E ASS. REFORMA AGRÁRIA**" na etapa de hierarquização.
- Sobre "Apresenta a proposta": "a proposta deve constar localização, aprovação(?) e
  características gerais dos beneficiários (grupos)".
- Sobre "Assina contrato individual": algo sobre a família ficar com "18%" e menções a
  poupança/folha e BPC. **Não decifrei.**
- Perto da etapa 7: "impacto na pós‑ocupação" e um "30 anos".

Se você me passar essas marcações em texto — ou uma foto de cada trecho, sem rotação —
eu incorporo direto no pôster.
