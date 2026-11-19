---
title: "Relatório Consolidado do Inventário SharePoint — MCMV"
subtitle: "Acervo local para descoberta, triagem e preparação dos dados"
date: "30 de agosto de 2026"
lang: pt-BR
---

# 1. Resumo executivo

O acervo completo baixado do SharePoint foi movido para:

`/home/juan-pablo/CIDADES/sharepoint`

A nova pasta foi validada e inventariada. A pasta antiga, que ocupava 3,73 GB e continha apenas uma parcela do acervo, foi removida definitivamente depois da validação da nova pasta.

| Indicador | Resultado |
|---|---:|
| Tamanho do novo acervo | **38,26 GB** |
| Arquivos físicos | **2.361** |
| Pastas | **83** |
| Itens internos de ZIP catalogados | **474** |
| Total de itens inventariados | **2.917** |
| Candidatos a dados, documentos ou dashboards | **2.415** |
| Tamanho do acervo antigo removido | **3,73 GB** |

# 2. Distribuição por formato

| Formato | Quantidade | Uso esperado |
|---|---:|---|
| CSV | **1.464** | Bases tabulares e exportações de dados |
| XLSX | **342** | Planilhas modernas e fichas de metadados |
| TXT | **209** | Bases textuais e arquivos delimitados |
| ZIP | **192** | Pacotes compactados; conteúdo interno inventariado |
| XLS | **142** | Planilhas em formato legado |
| PPTX | **52** | Apresentações e materiais institucionais |
| PDF | **14** | Documentos, notas e materiais de referência |

# 3. Escopo do inventário

O inventário registra, item a item, os arquivos e pastas do acervo local. Para os arquivos ZIP legíveis, também registra cada item interno sem exigir sua extração permanente. Os registros incluem, conforme disponibilidade:

- origem do item, no filesystem ou dentro de ZIP;
- nome e caminho relativo;
- extensão e categoria inferida;
- tamanho em bytes e data de modificação;
- período, fonte e frente MCMV inferidos pelo nome/caminho;
- pacote de origem e eventuais erros de leitura;
- identificação de candidatos a dados, documentos ou dashboards.

As classificações de fonte, frente e período são inferências para triagem. Elas devem ser confirmadas durante a leitura estrutural e semântica de cada base.

# 4. Conteýo identificado

O acervo combina bases e documentos relacionados ao Minha Casa, Minha Vida, incluindo materiais associados a FAR, Entidades, Rural, FGTS financiado, OGU subsidiado, SUB50/FNHIS, conjuntura, metadados e documentos gerais do programa.

Entre os candidatos tabulares, predominam arquivos associados a FAR, Entidades, FGTS, SUB50/FNHIS e Rural. Também foram encontrados materiais de metadados e apresentações que podem apoiar o mapeamento semântico das bases.

# 5. Artefatos entregues

## 5.1 Relatório geral

**Arquivo:** `sharepoint_local_inventario_mcmv.md`

Apresenta a leitura executiva, resumos por frente, fonte, extensão, período e pacote, além da descrição dos artefatos gerados.

## 5.2 Inventário dos arquivos

**Arquivo:** `sharepoint_local_arquivos.csv`

Contém o inventário dos arquivos e pastas encontrados diretamente no acervo local.

## 5.3 Conteúdo dos ZIPs

**Arquivo:** `sharepoint_local_zip_conteudo.csv`

Contém os itens localizados dentro dos pacotes ZIP, incluindo caminho interno, extensão, tamanho e classificações inferidas.

## 5.4 Dados candidatos

**Arquivo:** `sharepoint_local_candidatos_mcmv.csv`

Reúne os arquivos considerados candidatos para uso em dados, dashboards ou documentação, tanto no filesystem quanto dentro dos pacotes compactados.

# 6. Limites e próxima camada de análise

Esta entrega é um inventário arquivo por arquivo e item por item dentro dos ZIPs. Ela ainda não representa o perfil interno completo de cada CSV, XLS ou XLSX.

A próxima camada deve abrir cada base tabular e registrar:

- nomes e quantidade de colunas;
- quantidade de linhas e abas;
- tipos inferidos;
- períodos efetivamente cobertos;
- valores ausentes e problemas de codificação;
- chaves candidatas;
- arquivos duplicados e versões;
- qualidade e utilidade analítica;
- relação com as camadas raw, staging, silver e gold.

# 7. Preservação e compartilhamento

O PDF resume os resultados para leitura e circulação. Os arquivos CSV que acompanham o relatório são a evidência detalhada e devem ser preservados junto ao PDF para permitir filtros, buscas e análises posteriores.

O pacote de entrega contém este PDF, o relatório geral em Markdown, o inventário de arquivos, o inventário do conteúdo dos ZIPs e a lista de candidatos.
