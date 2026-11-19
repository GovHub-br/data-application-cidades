# BI — Linha Financiada

Artefatos comuns ao dashboard do Superset e ao relatório do Power BI.

## Páginas

1. **Visão executiva** — contratos, valor financiado, descontos, fontes e segmentos.
2. **Evolução** — contratação mensal e relatório semanal de FGTS/Fundo Social.
3. **Território e empreendimentos** — mapa, município, UF, estágio da obra e eventos.
4. **Contrapartidas** — valores conhecidos, contratos beneficiados e cobertura da fonte.
5. **Orçamento e predição** — orçamento oneroso, descontos, execução e série de features.

## Filtros previstos

- período;
- fonte de recurso;
- segmento da Linha Financiada;
- Classe Média, MCMV Cidades e Pró-Moradia;
- UF e município;
- faixa, programa, modalidade e tipo de imóvel;
- agente financeiro;
- situação do empreendimento.

Os filtros nativos compartilhados serão configurados após a primeira
publicação, quando os datasets estiverem validados no Superset.

## Superset

Após materializar as Golds no PostgreSQL:

```bash
python scripts/superset/linha_financiada.py --dry-run
python scripts/superset/linha_financiada.py
```

O script cria nove datasets, dez gráficos e o dashboard
`/superset/dashboard/linha-financiada/` de forma idempotente.

## Power BI

Use o conector PostgreSQL em modo **Importação** para as tabelas resumidas e
**DirectQuery** somente se houver necessidade operacional de atualização
intradiária. As consultas estão em `powerbi/consultas.pq` e as medidas em
`powerbi/medidas.dax`.

O arquivo `.pbix` não é versionado: ele é binário e dificulta revisão. O que
fica no Git são consultas, medidas, tema e definição funcional reproduzível.

## Ressalvas

- `features_preditivas` prepara atributos; não representa uma previsão validada.
- contrapartidas são parciais até o agente financeiro fornecer a base completa.
- orçamento FGTS é anual; não deve ser exibido como execução mensal.
