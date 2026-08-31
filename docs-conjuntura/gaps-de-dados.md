# Gaps e limites de comparabilidade do boletim

Registro operacional dos limites que não devem ser tratados como erro de
transformação. Este documento não contém valores individuais de pessoas nem
dados da Raw; serve para decidir safra, fonte e regra de publicação.

| Tema | Situação em 2026-08-29 | Decisão operacional | Fechamento definitivo |
|---|---|---|---|
| FGTS-PJ, 2T2025 | A base GEAVO é viva e pode registrar contratos tardiamente. A diferença histórica concentra-se nesse trimestre, que não aparece isolado nos três boletins disponíveis. | Tratar comparações de acumulado histórico como dependentes de safra; o comparador valida os trimestres publicados isoladamente. | Congelar a extração GEAVO utilizada por cada edição. |
| FGTS-PF, total por condição de uso | A `Base_PF_FGTS` está contínua até jun/2026, mas não contém a linha Faixa 3 Fundo Social. A fonte complementar foi localizada no MinIO: `PMCMV_FAIXA3_MCID`; a safra de 22/05 reproduz no 1T26 as 9.102 UH novas e 19.437 usadas do boletim. A cópia atual do Postgres está parada em 27/02. | Manter os campos PF existentes como recorte MCMV enquanto a fonte complementar não entra no modelo. Ao integrar, projetar somente data, código de condição de uso e métrica aprovada; excluir o código de tipo 5 até haver regra documentada. | Atualizar a carga Bronze a partir da safra escolhida, criar Silver sem identificadores pessoais, combinar com a Base PF e revalidar o gabarito. |
| Tenda, 1T2026 | Três campos manuais de variação estavam preenchidos com a medida errada. | Corrigidos pela migração `0007__CORRIGE_TENDA_1T2026_12M.sql`; Silver e Gold foram reconstruídas e as seis medidas passaram no gabarito. | Concluído. |
| Ticket médio da Cury | Os três boletins não publicam a série comparável de ticket médio da Cury. Não há valor publicado contra o qual validar a base 4T2020. | Não usar Cury como número validado em quadro editorial; manter o campo apenas como série auxiliar enquanto houver fonte manual. | Obter série publicada de VGV e unidades, ou retirar a coluna do produto editorial. |
| OGU, ações 00TI e 00XF | A Gold reflete o snapshot mais recente do SIAFI e expõe `dt_referencia_extracao`; o boletim usa um corte específico. Além disso, 00XF é crédito reembolsável e não tem dotação orçamentária equivalente no OGU tradicional. | Não comparar a tabela corrente com boletins antigos sem safra. Sempre exibir a data de referência; `dotacao = 0` em 00XF não é imputada. | Persistir snapshot da edição no schema de boletim antes de publicar. |

## Regra de aceite

Um número é considerado validado quando está no `gabarito-boletins.yml` com
fonte/página, consulta Gold e tolerância. Se a fonte revisa o passado, a regra
deve dizer `divergencia_esperada` e apontar a safra necessária; não se deve
alterar a Gold corrente para imitar uma edição histórica.
