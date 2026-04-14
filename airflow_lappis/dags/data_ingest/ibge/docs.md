# Endpoints e Agregados do IBGE (Construção, MCMV, Pessoas)

A API do IBGE (Sistema IBGE de Recuperação Automática - SIDRA) funciona baseada em **Agregados** (que são as pesquisas/tabelas específicas). 

Abaixo, os principais agregados relacionados à construção civil, déficit habitacional e características da população que podem ser relevantes para a inteligência de mercado relacionada ao Minha Casa Minha Vida (MCMV) e setor imobiliário:

## 1. Dados Diretos de Construção Civil

Estas são pesquisas focadas diretamente na atividade da construção e nos custos envolvidos.

* **Agregado 5932 - PIB da Construção Civil (Contas Nacionais Trimestrais)**
  * **O que é:** Volume e variações (trimestral e anual) do Produto Interno Bruto focado no setor.
  * **Uso:** Monitorar o aquecimento ou retração do setor a nível nacional.

* **Agregado 2296 - SINAPI (Sistema Nacional de Pesquisa de Custos e Índices da Construção Civil)**
  * **O que é:** Custo médio do metro quadrado e sua variação percentual.
  * **Uso:** Avaliar a inflação dos custos de insumos e mão de obra no setor. Essencial para precificação de empreendimentos e viabilidade do MCMV (cujo teto de preço é muito sensível aos custos da obra).

* **Pesquisa Anual da Indústria da Construção (PAIC)**
  * **Agregados:** Existem vários sob a família PAIC (e.g. 1779, 1780, etc.)
  * **O que é:** Mostra pessoal ocupado, salários, custos, receita bruta e valor das incorporações.
  * **Uso:** Muito bom para entender a estrutura financeira das empresas de construção e o volume anual de incorporações por estado.

## 2. Dados Relacionados ao MCMV (Demografia e Déficit Habitacional)

O MCMV é focado em suprir o déficit habitacional, especialmente nas faixas de menor renda. O IBGE não possui uma "Pesquisa MCMV", mas possui as pesquisas de demografia (PNAD Contínua e Censo Demográfico) que mapeiam o público-alvo e o déficit imobiliário.

* **PNAD Contínua - Características dos Domicílios e Moradores**
  * **Agregados Relevantes:** Existem vários agregados específicos sobre "Condição de ocupação do domicílio" (próprio, alugado, cedido).
  * **Uso:** Entender quantas pessoas moram de aluguel ou em habitações precárias (foco primário do MCMV) por UF e município.
  
* **Rendimento Domiciliar e Pessoal (PNAD Contínua e Censo)**
  * **O que é:** Distribuição da população por faixas de renda e rendimento médio.
  * **Uso:** O MCMV é dividido por faixas de renda (Faixa 1, 2, 3). Com esse dado, é possível estimar o *Tamanho do Mercado* endereçável em cada cidade (Ex: Quantas famílias têm renda entre R$ 2.640 e R$ 4.400 em São Paulo).

* **Déficit Habitacional (Fundação João Pinheiro + IBGE)**
  * **Nota:** O IBGE fornece os dados base, mas o déficit oficial muitas vezes é calculado pela FJP. Mesmo assim, usando os agregados sobre "Coabitação", "Ônus excessivo com aluguel" e "Habitação precária" no IBGE consegue-se um termômetro exato.

---

## Como Fazer Consultas "Genéricas" (Trazer todos os dados)

A requisição atual das DAGs especifica exatamente quais categorias e variáveis baixar, ex: `classificacao_id: 11255, categoria: 90694`.

**É possível sim puxar "tudo" e filtrar no momento do tratamento.** O IBGE permite o uso da palavra-chave `all` nos endpoints.

### Exemplo 1: Todas as variáveis de um Agregado
Se quiser pegar *todas* as variáveis do SINAPI em vez de apenas `48|1196|1197|1198`:
```text
GET https://servicodados.ibge.gov.br/api/v3/agregados/2296/periodos/-1/variaveis/all?localidades=N1[all]
```

### Exemplo 2: Todas as categorias de uma classificação (O "Pulo do Gato")
Em vez de especificar `categoria: 90694` (Construção), você pode pedir todas as categorias do setor usando `[all]`. Assim, ele traz Construção, Indústria Extrativista, Agropecuária, Comércio, etc., de uma vez só:

```text
GET https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/-1/variaveis/all?localidades=N1[all]&classificacao=11255[all]
```

### Exemplo 3: Tabelas Gerais (Total)
Para quase todos os agregados, se você **omitir** o parâmetro `classificacao`, a API irá retornar o total geral "default", sem os recortes minuciosos.

**Como adaptar isso no Airflow:**
Na sua DAG `ibge_ingest_dag.py`, você pode alterar no JSON de configuração:
```json
{
    "agregado": 5932,
    "variaveis": "all",
    "tabela": "pib_construcao_e_outros",
    "periodos": "-20",
    "classificacao_id": 11255,
    "categoria": "all"
}
```
E no `cliente_ibge.py`, certificar que o código monta a URL corretamente (o cliente atual já suporta enviar a string `"all"` no lugar dos ints no campo categoria/variáveis). Você pode baixar todo o dataset bruto e, usando ferramentas como Pandas ou DBT, isolar apenas a variável ou categoria que quiser no banco de dados.
