# Relatorio Oficial HTML/PDF

Use este padrao quando gerar documento oficial para revisao de tratamento.

## Estrutura

- Capa com projeto, issue, branch, data, responsavel e fonte de dados.
- Sumario executivo com contagens: tabelas revisadas, tratadas, descartadas, erro, flags criticas.
- Metodologia: pandas, amostragem, arquivos analisados, limitacoes.
- Achados por severidade: critica, alta, media, baixa, incerta.
- Evidencias: tabelas antes/depois, prints pandas, graficos, hashes/dedup, SFTP.
- Recomendacoes: ajustes de regra, testes, reprocessamento, dados faltantes.
- Anexos: comandos, paths, CSVs gerados.

## Frontend

- Preferir HTML estatico robusto para impressao A4.
- Usar CSS inline profissional com tokens de cor da frente Cidades/MCMV.
- Quando a rede/dependencias permitirem, hidratar com React e Twind; quando nao, manter fallback visual identico sem depender de CDN.
- Evitar header/footer automatico do navegador no PDF.

## Evidencias Minimas

- CSV completo da auditoria pandas.
- CSV de flags/problemas.
- CSV de amostras/prints.
- CSV de impacto SFTP/MinIO, mesmo que vazio com cabecalho e limitacao.
- PNGs ou tabelas para distribuicao de status, missingness e tipos de flags.
