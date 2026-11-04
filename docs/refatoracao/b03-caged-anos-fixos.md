# B3 · O Novo CAGED para de coletar em janeiro de 2027

> Parte do [guia das refatorações](README.md) · Prioridade P0 · Status: issue aberta · Issue: [#250](https://github.com/GovHub-br/data-application-cidades/issues/250) · PR: —

## Problema (✔ conferido no código)

- **Anos fixos.** `anos = [2024, 2025, 2026]` está escrito à mão em `plugins/cliente_novo_caged.py:255`.
- **Corte de mês errado por dois** (`:282`).
- **O mês que falha some da staging.** Ele é descartado em silêncio (`:288`) e a staging é regravada inteira.

## Correção proposta

- Derivar anos e meses da data de execução.
- Falhar quando um mês esperado não vier, em vez de regravar a staging sem ele.

## Critério de aceite

Um teste em `tests/` deriva anos e meses a partir de uma data fixa: a virada de ano e o corte do mês corrente.

## Relação

Pode ir antes da [F2](f2-ingestao.md). A remodelagem da ingestão deve manter a regra.
