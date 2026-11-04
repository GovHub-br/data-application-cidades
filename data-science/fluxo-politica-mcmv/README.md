# Ciência de dados: curva S, atraso e paralisação (Gov Hub Cidades)

Frente de ciência de dados sobre a série mensal dos empreendimentos do MCMV. O roteiro vem da conversa com o professor Mintsu (23/10/2026) cruzada com as diretrizes do secretário (13/10/2026): ver `infos/Reuniões/reuniao_mintsu-23-10/alinhamento_mintsu_x_secretario.md`.

## Ambiente

Python 3.13 em ambiente virtual local (`.venv`), com JupyterLab. Criado com:

```bash
/opt/homebrew/bin/python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m ipykernel install --user --name govhub-cidades-ds --display-name "Python (govhub cidades ds)"
```

Para abrir o Jupyter:

```bash
cd data_science
.venv/bin/jupyter lab
```

## Pastas

- `notebooks/`: `00_ambiente.ipynb` (teste do ambiente e da conexão) na raiz; uma pasta por linha do MCMV, com os mesmos três notebooks numerados: `far/` (01 inventário, 02 fluxo da política, 03 linha do tempo do contrato) e `entidades/` (idem para o FDS). Os notebooks rodam com a pasta deles como diretório de trabalho (`sys.path` e caches em `../../`).
- `src/`: código reaproveitado pelos notebooks (`db.py` conecta na réplica Postgres do Ministério, senha no `~/.pgpass`).
- `data/raw` e `data/interim`: extrações e intermediários, fora do git.
- `output/`: tabelas e figuras finais.

## Dados

Postgres medalhão do Lab em `10.0.0.50/cidades` (schemas `bronze`, `prata`, `ouro`, `seeds`), materializado pelo dbt do `data-application-cidades`. Credenciais em `.env` (fora do git). A réplica do Ministério (`10.0.0.73`) foi usada só no primeiro dia e não serve para a série: suas tabelas MONIT guardam um mês só.

## Etapas

1. Base analítica por APF e mês de obra, com a regra dos 90 dias e o status da CAIXA.
2. Régua: curva S média física e financeira com faixa (média aparada e sigma).
3. Rótulo estatístico validado contra o status da CAIXA.
4. Outliers, lista de atenção e clusterização.
5. Classificador (Random Forest) por mês de obra com feature importance.
6. Tempo até a entrega (sobrevivência) com o histórico pré-2024.

## Achados do inventário (notebook 01, 29/10/2026)

Tabelas de partida no banco do Lab: `ouro.ouro_far_serie_mensal_apf` (886 APFs do FAR novo, 2024-02 a 2026-08, físico, financeiro, situação MONIT e CAIXA, data de início de obra) para a régua; `bronze.bronze_sftp_gehis_andamento_obra` (153 mil linhas, arquivos semanais da CAIXA de 2021-07 a 2026-08) para o rótulo oficial no vocabulário da CAIXA (NORMAL, EM ATENÇÃO, RISCO DE PARALISAÇÃO, PARALISADO, OBRA NAO INICIADA, OBRA SOLUCIONADA, mais rótulos antigos como RITMO LENTO COVID19 e CRITICO); `prata.prata_far_caixa_serie` (4.505 APFs da CAIXA, legado e novo, 2024-06 a 2026-07) e `prata.prata_hist_far_empreendimento` (retratos de 2019-12 a 2026-07, com situação canônica e motivo de paralisação) para a curva S inteira e a sobrevivência.

## Notebook 02: o fluxo da política do FAR (30/10/2026)

`notebooks/far/02_fluxo_politica_far.ipynb` segue o mapa da política do FAR (`mapeamento-fluxos/novos_mapeamentos_politica/subsidiada`) etapa por etapa, com números e 22 gráficos: orçamento (ação 00AF no SIAFI), seleção (funil de propostas do GFAR e portarias), contratação (carteira, UF, tipologia, custo por UH, contrapartidas), execução (situação CAIXA, GEHIS, execução física, curva S por mês calendário e por mês de obra com faixa p20 a p80), desembolso (liberações por mês, composição, por UF), entrega (UH entregues, previsões) e famílias. O gerador do notebook está em `scripts/build_nb02.py` (edite lá e rode `.venv/bin/python scripts/build_nb02.py` seguido de `jupyter nbconvert --execute --inplace`). As consultas ficam em cache parquet em `data/interim/` (apague para recarregar do banco).

## Página "O fluxo do MCMV-FAR contado pelos dados" (30/10/2026)

`mapeamento-fluxos/novos_mapeamentos_politica/subsidiada/fluxo_far_pelos_dados.html`: página interativa no padrão do diagrama de arquitetura do Gov Hub (`pesquisa/diagrama_arquitetura_govhub_en_v3.html`), com a visão geral do mapa da política do FAR (atores, sete etapas numeradas, camada de dados e a régua da curva S) e uma vista por etapa com a política, as bases, os números, figuras SVG e as lacunas; painel lateral com diretrizes do secretário, recomendações do Mintsu, lacunas a pedir à CAIXA e glossário. Gerada por `scripts/build_fluxo_far_html.py` a partir do modelo e dos parquets de `data/interim` (rode o notebook 02 antes para atualizar o cache).

## Notebook 03: a vida de um contrato (30/10/2026)

`notebooks/far/03_linha_do_tempo_contrato.ipynb` organiza cada APF em três fases (pré-obra com cláusula suspensiva, obra, pós-obra até a entrada das famílias), mede a duração de cada fase, mostra a carteira sobre a escala do contrato (fase por mês de contrato e curva S alinhada), um gráfico de Gantt dos 800 contratos ativos, o atraso por fase com o que a base diz de cada um (GEHIS, detalhamento da CAIXA, contratos das famílias) e o caso do Residencial Vila Florida V. Gerador em `scripts/build_nb03.py`; marcos por APF em `data/interim/far_marcos_apf.parquet`; lista de contratos atrasados por fase em `output/far_contratos_atrasados_por_fase.csv`.

## Linha Entidades (FDS): notebooks em `notebooks/entidades/` (30/10/2026)

Mesmo roteiro do FAR, com a entidade organizadora como dimensão própria. Geradores em `scripts/build_ent_nb01.py`, `build_ent_nb02.py` e `build_ent_nb03.py` (rode o gerador e depois `jupyter nbconvert --execute --inplace`). Caches parquet com prefixo `fds_` em `data/interim/`; saídas em `output/fds_*.csv`.

- `01_inventario_entidades.ipynb`: as 26 tabelas da linha (prata `prata_fds_*`, ouro ficha e panorama por entidade, eixo histórico `prata_hist_fds_empreendimento`, seeds), a carteira de 364 APFs do Novo MCMV-Entidades (182 contratos de projeto e 182 de obra, 175 entidades), as três fontes da série no tempo, o legado PMCMV-E (2009 a 2019) e o que o FAR tem e o Entidades não tem (GEHIS, retrato mensal da CAIXA, funil de propostas, beneficiários).
- `02_fluxo_politica_entidades.ipynb`: o mapa da política do Entidades etapa por etapa: ação 00CY (SIAFI 2025 na prata, SIOP para os demais anos), ciclos de seleção (355/2024 contratado; 698/2026 selecionado, 35,8 mil UH, zero contratos), habilitação das entidades (nível, concentração, risco), contratação (UF, tipologia, regime, composição do investimento, terreno da União), execução (GEFUS x MONIT, previsto x realizado, curva S por mês de contrato separando projeto e obra), desembolso (patamares do fundo desde 2012, composição, antecipação por porte), entrega (alienação, promessas vencidas, legado) e trabalho social (planilha da CAIXA na bronze, com motivo).
- `03_linha_do_tempo_contrato_entidades.ipynb`: a vida do contrato em quatro fases (projeto, pré-obra com cláusula suspensiva, obra, pós-obra), duração de cada fase com o legado como régua, a carteira sobre a escala do contrato com a curva S alinhada, o Gantt dos 364 contratos, o atraso por fase com os motivos que o banco traz (legado e trabalho social) e o caso da UMP-PB (João Pessoa).

Achados que mudam a leitura: o cadastro PJ traz `dt_inicio_obra` igual à contratação em todos os contratos; o "físico" do contrato de projeto mede o projeto, não a obra; a decodificação do regime de obra na ouro parece invertida frente ao INT059; o INT059 marca "paralisada" obras que o MONIT e o livro-razão mostram andando; no legado, três em cada quatro paralisações com motivo acontecem depois da obra (legalização e entrega).

Caixas vermelhas marcam cada dado que não vem do banco (série do SIOP, portarias dos ciclos, níveis de habilitação da Portaria 861/2023, parâmetros de régua e data de hoje) e como levá-lo ao banco (seeds e modelos dbt).
