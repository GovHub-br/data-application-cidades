---
name: atualizar-docs-pages
description: Use quando for mexer no site docs-pages deste repositório — atualizar os números depois de novos PRs, escrever ou corrigir o contexto de um domínio, incluir domínio novo, mudar o recorte do escopo MCID, ajustar entrega que aparece no domínio errado, ou editar o texto das visões. Também quando o site estiver desatualizado, o build falhar, ou alguém perguntar onde se edita a documentação.
---

# Atualizar o site docs-pages

## O princípio

O site é **híbrido**: os fatos são lidos do repositório a cada build, a narrativa é
escrita à mão. Você edita só a narrativa. Se um número está errado no site, a
correção nunca é no texto — é no código ou na coleta.

## O que é gerado e o que é seu

| Arquivo | Natureza | Você edita? |
|---|---|---|
| `docs-pages/src/_data/*.json` | acervo lido do repo | **Não.** Sai de `make docs-collect` |
| `docs-pages/src/_diagramas/*.svg` | linhagem das golds, em cache | **Não.** O build regenera quando a linhagem muda |
| `docs-pages/src/dominios.yml` | escopo e contexto curado | **Sim.** É o arquivo principal |
| `docs-pages/src/templates/*.j2` | estrutura e texto de abertura | Sim, com cuidado |
| `docs-pages/src/assets/tema.css` | identidade visual | Sim |
| `docs-pages/tooling/**` | coletores e build | Só para mudar o mecanismo |
| `docs-pages/site/` | saída do build | **Não.** É descartada a cada build |

## O fluxo

```bash
make docs-collect                        # 1. atualiza o acervo (usa rede: git, gh)
git diff --stat docs-pages/src/_data/    # 2. veja o que mudou no período
                                         # 3. edite a curadoria (ver tabela abaixo)
make docs-serve                          # 4. confira em localhost:8000
```

O passo 2 é o que torna isso útil: o diff do acervo mostra as entregas e os modelos
novos desde a última coleta. É a partir dele que se escreve a narrativa nova, em vez
de só atualizar contadores.

## Onde mexer, por sintoma

| O que você quer | Onde |
|---|---|
| Números do site estão velhos | `make docs-collect` — nada de editar texto |
| Reescrever o que um domínio significa | `dominios.yml` → `contexto`, `o_que_faz` |
| Mudar as perguntas que um domínio responde | `dominios.yml` → `perguntas` |
| Entrega no domínio errado, ou faltando | `dominios.yml` → `chaves` do domínio |
| Incluir um domínio novo | novo item em `dominios.yml` → `dominios` |
| Incluir/excluir DAGs ou projeto dbt do escopo | `dominios.yml` → `escopo` |
| Texto de abertura de uma visão | `templates/gestao|tecnico|vitrine.html.j2` |
| Texto da vitrine | `dominios.yml` → `programa`; todo bloco exige `fonte` |
| Aparência dos diagramas de linhagem | `tooling/mermaid.py` → `ESTILOS` |
| Rótulo das abas ou das páginas | `tooling/build.py` → `ABAS` e `PAGINAS` |

## Domínio novo

O `slug` **precisa** ser o nome da pasta em
`airflow_lappis/dags/dbt/<projeto>/models/<slug>_dbt/`, senão nenhum modelo casa.
O build falha quando isso acontece — é quase sempre erro de digitação. Se o
domínio existe de propósito antes da implementação, declare `sem_modelos: true`.

```yaml
  - slug: nome_da_pasta_sem_o_sufixo_dbt
    rotulo: Nome Legível
    subtitulo: Uma linha que diz para que serve
    contexto: |
      Que problema de gestão existe aqui, e por que ele é difícil hoje.
    o_que_faz: |
      O que o pipeline faz com isso, da fonte à tabela de uso.
    perguntas:
      - Pergunta de gestão que este domínio responde?
    sistemas: [Nome do sistema de origem]
    chaves: [termo, outro termo]
```

`chaves` são termos em minúsculas procurados no título, no corpo e na referência de
cada entrega. Chave com 4 letras ou menos casa palavra inteira — `far` não pega
"farmácia". Chave genérica demais rouba entregas de outro domínio: prefira o termo
que só aparece neste assunto.

## Regras

1. **Nunca digite um número no template.** Se o número que você quer não existe,
   ele se calcula em `tooling/dados.py` → `metricas()`, e o template consome.
2. **Nunca edite `src/_data/*.json` à mão.** A próxima coleta sobrescreve, e o
   número passa a mentir em silêncio.
3. **Rode `make docs-serve` antes de commitar.** O build falha de propósito em
   link interno quebrado e em markup escapado — são erros que passariam batido.
4. **Commite o acervo junto.** Os JSONs são versionados: sem eles, o build do CI
   não reproduz o site.

## Quando o build falha

| Mensagem | Causa |
|---|---|
| `acervo incompleto` | falta rodar `make docs-collect` |
| `dominio 'x' nao casou nenhum modelo` | slug não bate com a pasta em `models/` |
| `link quebrado em ...` | href aponta para página que não existe; confira o `rel` |
| `markup escapado em ...` | HTML/SVG gerado chegou ao template sem ser `Markup` |
| `'x' is undefined` | template usa variável que `_contexto()` não fornece |
| `N entrega(s) fora do escopo` | não é erro: são entregas de `ipea`/`mir`, fora do MCID |

## Antes de considerar pronto

- [ ] `make docs-serve` roda sem erro e você abriu as páginas alteradas
- [ ] Nenhum número foi digitado à mão
- [ ] O acervo (`src/_data/*.json`) está no commit, se você rodou a coleta
- [ ] O texto novo diz o que o domínio resolve, não o que o código faz
