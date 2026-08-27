# Diretrizes para Agentes de Desenvolvimento (AGENTS.md)

## 1. Regras de Commits

- **Estilo de Mensagem:** Siga o padrão *Conventional Commits* em português (ex: `feat(governance): ...`, `fix(dbt): ...`, `refactor(airflow): ...`).
- **Validação Prévia:** Nunca faça commit de código antes de testar e validar o funcionamento.
- **Assinatura Obrigatória:** Todo commit deve conter no final do corpo da mensagem a linha:
  ```text
  Assisted-by: TOOL:MODEL
  ```
  Exemplo: `Assisted-by: omp:openrouter/google/gemini-3.7-flash`.

## 2. Princípios & Abordagem Pragmática (Ponytail)

- **Simplicidade e Pragmatismo:** Em fases de protótipo e desenvolvimento, prefira a solução mais simples e direta que funcione (YAGNI). Evite abstrações especulativas ou engenharia excessiva.
- **Sem Hacks de `PYTHONPATH`:**
  - Clientes e módulos auxiliares compartilhados devem ficar dentro de `airflow_lappis/dags/clientes/` com `__init__.py`.
  - O Airflow 3 adiciona a pasta `dags/` ao `sys.path` nativamente.

## 3. Padrões do Apache Airflow 3

- **Parâmetros de Agendamento:** Use sempre `schedule="<cron>"` em vez de `schedule_interval`.
- **Contexto de Operadores:** Não use `provide_context=True` em `PythonOperator` (descontinuado no Airflow 3).
