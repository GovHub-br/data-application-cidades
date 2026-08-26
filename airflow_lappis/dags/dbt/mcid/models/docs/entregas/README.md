# Entregas das Issues de Arquitetura e Silver

Esta pasta consolida textos prontos para anexar ou colar nas issues GitHub.

## Arquivos

- `issue-119-entrega.md`: entrega da padronizacao das tabelas silver por frente MCMV e base semantica para marts de dashboard.
- `issue-118-entrega-final.md`: estrategia final de historico, padrao de versionamento, base piloto dbt e testes de reprocessamento.
- `issue-117-status-adr-pendente.md`: status da arquitetura de producao; registra que o ADR ainda precisa ser formalizado.
- `issue-66-entrega-indicadores-historicos-relogio-alertas.md`: entrega dos indicadores historicos, fontes do relogio de metas e base de alertas.
- `issue-119-ajuste-frentes-faltantes.md`: evidencia do ajuste das frentes faltantes na silver, com SUB50/FNHIS conectado e Pro-Moradia ainda sem fonte confirmada.
- `issue-119-correcao-arquitetura-duckdb-staging.md`: correcao de arquitetura para garantir que silvers sejam geradas somente a partir do MinIO `staging/` via DuckDB.
- `issue-130-dicionario-indicadores.md`: dicionario dos indicadores do reloginho (grupo A) e de gargalo/desempenho (grupo B) com os 14 campos solicitados na issue #130, incluindo fontes, regras, granularidades e pendencias (meta oficial, serie historica e validacao de negocio).
- `issue-130-matriz-indicador-fonte-campo-regra.csv`: matriz indicador x fonte x tabela x campos x regra para os grupos A (reloginho, status pendente) e B (gargalo, status definido a validar) da issue #130.
- `issue-130-checklist-validacao-negocio.md`: checklist de validacao de negocio (Fase 5) com 19 decisoes em 4 blocos (metas, regras do reloginho, limiares do gargalo e outras decisoes) para levar a area responsavel.
- `issue-130-decisoes-pendentes-validacao.csv`: planilha das 19 decisoes pendentes de validacao, com valor atual de referencia, resposta esperada e indicadores bloqueados, para registro das respostas da area.
- `issue-130-estrategia-apf-fases.md`: estrategia de identidade de empreendimento na silver (APF variavel por fase) revisada pelo @oracle; define `id_empreendimento` (hash do APF-ancora), `dim_empreendimento`, fontes de mapeamento e testes dbt.
- `issue-130-validacao-tecnica-fases-2-4.md`: validacao tecnica das fases 2-4 (cobertura historica, regras de calculo e calculos em amostra com acesso ao banco `cidades`); registra a duplicacao 2x por APF, a serie mensal 2024-06+ e a comparacao com a referencia #66.
- `issue-130-resumo-final.md`: resumo final consolidado da issue #130 (artefatos, validacoes empiricas, implementacao APF/fases, decisoes pendentes e pendencias para fechar).
- `issue-130-pendencias-encoding-canonicalizacao-sftp-minio.md`: registro das pendencias de encoding (mojibake) e canonicalizacao (`gefus_*`/`_canonicas.csv`) das bases SFTP no MinIO; decisao de usar MinIO como fonte e reaproveitar o pipeline local de tratamento.
- `issue-130-d1-reconciliacao-novo-mcmv-far.md`: analise da sobreposicao SFTP x Novo MCMV (D1, opcao B — FAR disjunto, FDS sobreposto) e mapeamento de colunas do `novo_mcmv_far` (cad_pj + obra_mensal) para o contrato comum do modelo.
- `issue-130-implementacao-modelos-historicos-empreendimentos.md`: documentacao da implementacao dos modelos historicos de empreendimentos (historico mensal FAR/FDS/Rural + snapshot corrente derivado), fontes, decisoes, validacao e pendencias.

## Observacao de Commit

No momento da geracao destes documentos, as mudancas ainda estavam locais na branch
`feat/tratamento-dados-historicos`. Nao incluir `local.env` nem `.dbt-venv/` no commit.
