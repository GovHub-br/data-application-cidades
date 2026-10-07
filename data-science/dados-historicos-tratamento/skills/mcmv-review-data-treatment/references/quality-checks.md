# Checklist de Qualidade do Tratamento

Use este checklist para classificar achados.

## Limpeza

- Linhas 100% vazias ou apenas `-` nao devem permanecer.
- Colunas 100% vazias nao devem permanecer.
- Totais/rodapes devem ser marcados ou removidos de acordo com o perfil, nunca misturados silenciosamente a eventos.
- `missing_pct` alto exige amostra e causa provavel.

## Padronizacao de Campos

- Colunas devem estar normalizadas, sem acentos, espacos ou caracteres instaveis.
- Nomes duplicados apos normalizacao indicam risco de sobrescrita ou ambiguidade.
- Metadados obrigatorios: `source_table`, `report_date`, `institution`, `profile`, `content_hash`.

## Datas

- Datas de evento/referencia devem parsear com taxa alta.
- Datas fora do periodo esperado do MCMV ou no futuro devem ser flagadas.
- `report_date` deve ser coerente com filename/SFTP path.

## Valores

- Colunas `valor`, `vlr`, `total` devem ser numericas depois de tratar virgula decimal.
- Valores monetarios com texto, `R$`, milhares e virgula decimal devem ser validados por amostra.
- Negativos, zeros massivos ou outliers extremos precisam de justificativa.

## Tipos e Identificadores

- CNPJ, contrato, APF, codigos, `nr_` e `nu_` devem preservar zeros a esquerda.
- Quantidades devem aceitar nulos sem virar float quando o tipo canonico esperado e inteiro nullable.
- Coercao que transforma muitos valores em nulo e possivel tratamento excessivo.

## Encoding

- Procurar `Ã`, `Â`, `â€`, `�` e caracteres de substituicao.
- Verificar exemplos reais antes/depois quando houver reparo.

## Duplicidades

- Deduplicacao deve usar hash de conteudo, nao apenas nome.
- Duplicatas internas de linhas podem ser validas em evento, mas suspeitas em lookup.
- Duplicatas de colunas quase sempre exigem ajuste.

## Futuro Preditivo

- Flaggar ausencia de chave estavel, periodo, instituicao, granularidade e linhagem.
- Flaggar risco de vazamento temporal quando informacao futura aparece como feature.
- Recomendacoes devem separar problema tecnico de baixa utilidade analitica.
