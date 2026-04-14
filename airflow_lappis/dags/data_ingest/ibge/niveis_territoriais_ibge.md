# IBGE API: Níveis Territoriais, Variáveis, Classificações e Categorias

Entender a estrutura da API do IBGE (SIDRA) é fundamental para puxar os dados corretos. Todo dado na API é estruturado em uma hierarquia de 5 pilares: **Agregado, Variável, Localidade (Nível Territorial), Classificação e Categoria**.

Abaixo, explicamos o que é cada escopo e como eles se aplicam às nossas fontes focadas em Construção Civil e Minha Casa Minha Vida (MCMV).

---

## 1. Glossário: Como a API Estrutura os Dados

1. **Agregado:** É a tabela ou "cubo" de dados de uma pesquisa específica. Imagine como o banco de dados geral. 
   *(Ex: Agregado 5932 - Contas Nacionais Trimestrais).*

2. **Variável:** É a **métrica**, a unidade de medida ou o indicador que você quer saber daquele Agregado.
   *(Ex: "Valor em Reais", "Variação Percentual no Ano", "Número de Pessoas").*

3. **Nível Territorial e Localidade:** O recorte geográfico. O Nível (N) é o tipo de geografia (N1=Brasil, N3=UF, N6=Municípios) e a Localidade é o ID específico geográfico (35=SP).

4. **Classificação:** É a "dimensão" ou filtro de análise. 
   *(Ex: "Setores da Economia", "Faixa de Renda", "Cor ou Raça", "Condição do domicílio").*

5. **Categoria:** É o valor específico dentro de uma classificação.
   *(Ex: Para a classificação "Setores da Economia", as categorias seriam "Construção Civil", "Agropecuária", etc).*

---

## 2. Aplicação nas Pesquisas de Produção e MCMV (Dicionário de Endpoints para as DAGs)

Veja o que significa cada um desses campos nas pesquisas que utilizamos, e como preencher o JSON da nossa variável `IBGE_CONFIGURACOES` para extraí-las.

### 1. Agregado 5932 - PIB da Construção Civil (Contas Nacionais Trimestrais)
* **Nível Territorial (Localidade):** `N1` (Apenas Brasil).
* **Variáveis:** O *que* estamos medindo. Selecionamos `6564|6563|6562|6561`:
  * `6564`: Taxa trimestre contra trimestre imediatamente anterior (%)
  * `6561`: Taxa trimestre contra mesmo trimestre do ano anterior (%)
  * `6562`: Taxa acumulada ao longo do ano (%)
  * `6563`: Taxa acumulada em quatro trimestres (%)
* **Classificação:** `11255` -> Representa a dimensão de análise "Setores e subsetores da economia".
* **Categoria:** `90694` -> O ID específico do setor "Construção". Para puxar todos os setores (Indústria, Agropecuária, etc) em uma única requisição, usamos `"all"`.

**Configuração na DAG:**
```json
{
    "agregado": 5932,
    "variaveis": "6564|6563|6562|6561",
    "tabela": "pib_construcao",
    "periodos": "-20",
    "classificacao_id": 11255,
    "categoria": "all"
}
```

### 2. Agregado 2296 - SINAPI (Custo da Construção Civil)
* **Níveis Territoriais:** `N1` (Brasil), `N2` (Regiões), `N3` (Estados/UF).
* **Variáveis:** Medidas do índice mensal. Selecionamos `48|1196|1197|1198`:
  * `48`: Custo médio m² em moeda corrente (R$/m²)
  * `1196`: Variação percentual mensal (%)
  * `1197`: Variação percentual no ano (%)
  * `1198`: Variação percentual nos últimos 12 meses (%)
* **Classificação/Categoria:** *Não possui.* O agregado inteiro do SINAPI já é um dado puro de custo geral da construção.

**Configuração na DAG:**
```json
{
    "agregado": 2296,
    "variaveis": "48|1196|1197|1198",
    "tabela": "sinapi",
    "periodos": "-20"
}
```

### 3. Agregado 1779 e 1780 - PAIC (Pesquisa Anual da Indústria da Construção)
* **Níveis Territoriais:** Principalmente `N1` (Brasil), e alguns dados em `N3` (UF). Não chega em Municípios.
* **Variáveis:** Métricas financeiras e de equipe. Usaremos `"all"` para baixar todas, que incluem por exemplo o código `449` (Receita bruta), Pessoal Ocupado, Salários, Valor das Incorporações, etc.
* **Classificação:** `12187` (Tipo de Obra) ou `11977` (Porte da Empresa).
* **Categoria:** Usaremos `"all"` para baixar "Edificações Residenciais" (que engloba o MCMV), "Infraestrutura" e "Serviços especializados" juntos para filtrar no DBT ou SQL depois.

**Configuração na DAG:**
```json
{
    "agregado": 1779,
    "variaveis": "all",
    "tabela": "paic_receitas_obras",
    "periodos": "-10",
    "classificacao_id": 12187,
    "categoria": "all"
}
```

### 4. Agregado 6435 - PNAD Contínua (Habitação / Déficit / Condição de Ocupação)
* **Níveis Territoriais:** `N1` (Brasil), `N3` (Estados), `N7` (Regiões Metropolitanas), `N33` (Capitais).
* **Variáveis:** Apenas a `5977` (Total de domicílios particulares permanentes habitados, em milhares).
* **Classificação:** `700` (Condição de posse/ocupação do domicílio - Essencial para mapear Déficit Habitacional e mercado de Aluguel).
* **Categoria:** `"all"` -> Ao passarmos "all", o IBGE nos retornará quantas casas são "Próprias (pagas)", "Próprias (pagando/financiadas)", "Alugadas" ou "Cedidas" em cada Capital/Estado.

**Configuração na DAG:**
```json
{
    "agregado": 6435,
    "variaveis": "5977",
    "tabela": "pnad_condicao_ocupacao",
    "periodos": "-12",
    "classificacao_id": 700,
    "categoria": "all"
}
```

### 5. Agregado 7435 - Rendimento Domiciliar (Público-alvo MCMV por faixa de SM)
* **Níveis Territoriais:** Foca em UFs e Metrópoles.
* **Variáveis:** `10681` (População) e `10682` (Rendimento nominal domiciliar per capita médio).
* **Classificação:** Não especificado no nível mais alto, ou classes de salário usando *all*.
* **Categoria:** Puxa a quebra geral de renda.
*(Para o Censo em vez da PNAD e ir para Nível Municipal, usa-se agregados semelhantes do Censo 2022 listando as famílias de R$1.500 a R$4.400).*

### 6. Déficit Habitacional (Fundação João Pinheiro + IBGE)
* **Níveis Territoriais:** Historicamente `N1`, `N2`, `N3` e `N7` (Grandes centros metropolitanos). Nos anos de Censo, o mapa do déficit desce ao `N6` (Municípios). 
* **Variáveis e Classificações:** O Déficit oficial calculado pela Fundação João Pinheiro não é um único "Agregado" puro do IBGE. Ele cruza os microdados da **PNAD Contínua (Habitação)** e do **Censo**.
* **O que puxar:** Na prática, nas nossas DAGs, para alimentar os cálculos de Déficit usamos os agregados da PNAD acima (como o `6435`) filtrando por "Habitação Precária", "Coabitação familiar" e "Ônus excessivo com aluguel". A lógica de configuração da DAG para déficit será consumir os endpoints de habitação e renda citados nos passos 4 e 5.

---

## 3. Consultando Tudo: O uso do "ALL"

Como a API suporta o parâmetro `all`, quando você não souber o ID exato da variável ou da categoria, você sempre pode fazer:

```text
variaveis="all"&classificacao=ID_CLASSIFICACAO[all]
```

Dessa forma, o IBGE listará num mesmo JSON todos os setores da economia, todas as faixas de salário mínimo ou todos os tipos de habitação possíveis. Você traz todos os dados para o seu Data Lake e faz a limpa estrutural apenas das categorias que importam usando uma ferramenta de modelagem no Data Warehouse.
