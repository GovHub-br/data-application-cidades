---
name: conjuntura-verifier
description: Use para verificar o frescor e a saúde de todo o pipeline do Boletim de Conjuntura do Setor Habitacional — da fonte oficial ao lake, ao banco (ouro) e ao Airflow. Dispara com "os dados do conjuntura estão frescos?", "o que falta para o boletim do <trimestre>", "verifica o pipeline do conjuntura", "qual a última competência de cada fonte", "por que tal quadro está vazio", ou antes de fechar uma edição do boletim.
---

# Verificar o pipeline do conjuntura

Responde, por indicador, a quatro perguntas, nesta ordem:
1. Qual é o dado mais recente **publicado na fonte**?
2. O que está no **lake**, em `snh/staging/`?
3. O que está no **banco**, nas tabelas `ouro.ouro_conjuntura_*`?
4. Como foi a **última execução da DAG** na homolog?

A regra de leitura é a seguinte.
- Fonte > lake: a ingestão está atrasada ou quebrada.
- Lake > banco: falta rodar o dbt (`conjuntura_dag`).
- Fonte = lake = banco: o dado está em dia.

Toda a verificação é **só leitura**.

## Rodar

Rode na raiz do repo e com VPN. O banco, o lake e o Airflow só respondem pela rede do Lab:

```bash
.venv/bin/python .claude/skills/conjuntura-verifier/scripts/verificar_frescor.py            # completo (~3 min)
.venv/bin/python .claude/skills/conjuntura-verifier/scripts/verificar_frescor.py --sem-fonte # só lake/banco/Airflow
```

O script imprime uma tabela markdown. Sem VPN ele falha logo no banco: **pare e
peça a VPN**, não tire conclusão de dado parcial.

O script não cobre tudo. Confira à mão:
- **PDFs** (CBIC, releases de RI das construtoras, ABRAMAT): veja o calendário, faça busca web e peça o PDF ao Lucas;
- **ICST**, que precisa de login no FGV Dados: o script mostra só o lake;
- **conteúdo dos anexos do Tesouro**: o script vê só a data dos e-mails (ver armadilhas).

Para o catálogo das fontes (tipo, URL, DAG e tabela ouro de cada uma), veja [references/fontes.md](references/fontes.md).

## Depois de verificar

- Para entregar o resultado, use a tabela do script com uma coluna "próxima ação". Ponha o resultado na resposta ou num arquivo do repo, nunca num artefato publicado.
- Não corrija nada no meio da verificação. Liste o que achou, separe o que é decisão do Lucas (troca de fonte, cálculo novo no dbt) e o que é execução (rodar DAG, rodar dbt).
- Se o problema é "lake > banco", a correção é rodar a `conjuntura_dag`. Ela escreve em tabelas compartilhadas, então só rode com pedido explícito.

## Armadilhas já encontradas (26/11/2026)

1. **A DAG "verde" não prova dado novo.** As DAGs de API pulam a carga quando o md5 do arquivo é igual ao da última. Uma fonte que parou de atualizar (o Sinduscon-PR no INCC-M, em 07/2026) aparece verde todo dia. Compare sempre a competência, nunca só o estado da DAG.
2. **Caminho fixo no `sources.yml`.** GEAVO e GEFUS chegam com a data no nome, e a fonte do dbt aponta para um arquivo específico. O lake pode estar em dia e o dbt lendo o arquivo de meses atrás. Confira o `meta.caminho` em `dbt/mcid/models/conjuntura_dbt/sources.yml`.
3. **Relatório do Tesouro com a página errada.** O `dotacao_execucao_outras_fontes_mcid` exportava a página "Ano Lançamento: 2025". Abra o CSV do anexo e leia as linhas 3 a 7 (filtro e página) antes de confiar no OGU. Os agendamentos do Tesouro Gerencial pararam em 31/10/2026.
4. **Power BI do Novo CAGED.** O modelo saiu da capacity em 06/2026. A fonte agora são os microdados do PDET, com saldo = MOV + FOR − EXC. Se o CAGED parar, olhe o FTP (`ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/<ano>/<ano><mês>/`).
5. **GEAVO é chroot.** O login `sftp_geavo` vê `/home/caixa.geavo` como `/`: a pasta no servidor é `/GEAVO`. O `MC<data>.zip` traz os `.mdb` sem data no nome.
6. **pg_duckdb não expõe colunas de `select * from read_parquet()`** fora de uma tabela materializada. Para consultar a staging pelo banco, use `r['coluna']`.
7. **O diretório do repositório pode ser compartilhado com outra sessão.** Antes de commitar, confira `git branch --show-current`.
