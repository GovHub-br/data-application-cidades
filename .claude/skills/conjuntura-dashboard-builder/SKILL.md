---
name: conjuntura-dashboard-builder
description: Use para montar, atualizar ou republicar o dashboard do Boletim de Conjuntura do Setor Habitacional no Superset (slug boletim-conjuntura) com a identidade visual do Gov Hub — fechar a edição de um trimestre, incluir ou tirar um quadro, reconstruir uma página, ajustar o tema (cores, fontes) ou conferir o visual. Dispara com "atualiza o dashboard do conjuntura", "monta o boletim do <trimestre> no Superset", "reconstrói a página N", "aplica o tema Gov Hub no boletim", "o dashboard está com cara errada".
---

# Montar o dashboard do conjuntura no Superset

O dashboard é **um só**: `/superset/dashboard/boletim-conjuntura/` (id 14) na
homolog, com sete abas (uma por página do boletim) e 22 quadros. Todos os quadros
são tabelas (viz `table`), e um filtro nativo de **Trimestre** (coluna `edicao`)
escolhe a edição. Não existe edição padrão: o dbt publica todas, e quem consulta
escolhe.

| O que | Onde |
|---|---|
| Quadros: página, título, colunas, SQL | `scripts/superset/build_boletim.py`, `pagina_01()` … `pagina_07()` |
| Construção: datasets, charts, layout, filtro e tema | mesmo arquivo, segunda metade (`main`) |
| Tema Gov Hub | `scripts/superset/tema_govhub.css`, gravado no campo `css` do dashboard a cada construção |
| Dados | tabelas `ouro.ouro_conjuntura_boletim_p<N>_*` (dbt `conjuntura_dbt`) |
| Credenciais | `.env`: `SUPERSET_URL`, `SUPERSET_USERNAME` (ou `SUPERSET_USER`), `SUPERSET_PASSWORD` |

## O fluxo

A ordem é obrigatória. O construtor não carrega SQL próprio: se a tabela ouro não
existe, ele falha.

1. **Frescor.** Rode a skill `conjuntura-verifier`. Uma fonte atrasada vira uma
   célula vazia no dashboard. Isso é informação correta ("ainda não chegou"),
   mas precisa ser sabido antes de publicar.
2. **dbt.** A `conjuntura_dag` na homolog roda o `conjuntura_dbt` (prata + ouro).
   Ela escreve em tabelas compartilhadas: só rode com pedido explícito.
3. **Congelar, ao fechar uma edição.** `make conjuntura-congelar` (= `dbt snapshot`).
   As fontes revisam o passado, e o snapshot guarda o retrato da edição publicada.
4. **Construir.**
   ```bash
   uv run python scripts/superset/build_boletim.py --dry-run         # confere, não grava
   uv run python scripts/superset/build_boletim.py --paginas 3,5     # só essas páginas
   uv run python scripts/superset/build_boletim.py                   # tudo
   ```
   Sem `--paginas`, o construtor **sobrescreve** o que foi editado pela interface do
   Superset. Prefira reconstruir só as páginas que mudaram.
5. **Conferir o visual.** É obrigatório depois de mudar o tema ou o layout:
   ```bash
   uvx --with playwright==1.55.0 --with python-dotenv python \
       .claude/skills/conjuntura-dashboard-builder/scripts/print_dashboard.py <pasta> 1,3
   ```
   Abra os PNGs e olhe de fato: título, abas, cabeçalho das tabelas, quadros vazios.
6. **Conferir os números.** Rode `make conjuntura-validar-boletins` (seed + teste
   contra os boletins impressos). Uma divergência não impede publicar, porque as
   fontes revisam, mas precisa ser dita.

## O tema Gov Hub

Os tokens vêm da skill `govhub-visual-identity` (MIV). No dashboard:

- **Roxo `#613EFF`** (assinatura): cabeçalho das tabelas, aba ativa, filete no topo de cada quadro.
- **Navy `#0A005A`**: título do dashboard (Oswald, uppercase), títulos dos quadros, rótulos das abas.
- **Rosa `#F9006F`**: só o botão "Apply filters". É o acento pontual; não use em mais nada.
- **Fundos:** `#F7F7F7` no painel; zebra `#F8F9FA` nas linhas; pêssego `#FFE7E1` no hover.
- **Fontes:** Reddit Sans no corpo, Oswald só no título e nas abas. Entram por `@import` do Google Fonts, que a CSP da homolog permite (ela só restringe `frame-ancestors`).
- As barras de célula verdes e vermelhas das tabelas são do Superset (positivo e negativo). Ficam como estão: são cor semântica, não de marca.

Para mudar o tema, edite `tema_govhub.css` e reconstrua. Para testar sem reconstruir
nada, grave só o CSS:

```python
import sys

sys.path.insert(0, "scripts/superset")
import build_boletim as b

api = b.Superset()
d = api.get("dashboard", b.SLUG)
api.atualizar("dashboard", d["id"], {"css": b.TEMA.read_text(encoding="utf-8")})
```

## Armadilhas já encontradas

1. **A imagem é `apache/superset:latest`.** As classes do DOM mudam entre versões.
   Seletor que não casa falha em silêncio. Depois de qualquer atualização do
   Superset, rode o print e confira. Em 26/11/2026 valia o seguinte:
   - nome do dashboard: `.dashboard-header-container .title-panel > :first-child`. Os selos "Published" e o autor são os irmãos seguintes: não estilize o `.title-panel` inteiro.
   - texto da aba: `.ant-tabs-tab .editable-title`, que tem fonte própria (Inter). A regra no `.ant-tabs-tab-btn` não chega até ele.
   - quadro: `.dashboard-component-chart-holder`.
2. **Login.** O bearer JWT é cego para este dashboard (404). O cliente do
   `build_boletim.py` e o script de print autenticam pelo formulário.
3. **`networkidle` nunca chega.** O Superset fica consultando o servidor. Espere o
   seletor do quadro.
4. **Edição sem dado.** Se a fonte atrasou, a célula fica vazia. Não invente valor e
   não esconda a linha. Confira com o verificador.

## Entrega

Diga em texto o que mudou (páginas reconstruídas, tema, números conferidos) com o link
do dashboard. Os prints ficam numa pasta local; mostre-os se pedirem. Não publique
artefato.
