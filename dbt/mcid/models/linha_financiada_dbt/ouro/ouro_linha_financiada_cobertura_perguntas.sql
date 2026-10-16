{{ config(materialized='table') }}

select * from (values
    ('Análise preditiva de contratações', 'Respondível', 'Histórico mensal FGTS e Fundo Social; features temporais publicadas.'),
    ('Análise preditiva da execução orçamentária', 'Parcial', 'FGTS disponível em orçamento anual; Fundo Social possui posição de execução, mas ainda não uma série mensal completa.'),
    ('Contrapartidas e contratos beneficiados', 'Parcial', 'Aportes MCMV Cidades e contrapartida de parceria são identificados; não existe base institucional completa.'),
    ('Atualização e unificação automatizadas', 'Respondível', 'Fontes integradas por dbt e execução diária pelo Airflow.'),
    ('Painel de empreendimentos para eventos', 'Respondível', 'Cadastro, posição de obra, unidades, valores e marcadores de modalidade.'),
    ('Relatório semanal FGTS e Fundo Social em BI', 'Respondível', 'Gold semanal pronta para consumo em BI.'),
    ('Mapa interativo de execução', 'Respondível', 'Agregação territorial com filtros de faixa, programa, modalidade e tipo de imóvel.'),
    ('Base agregada automatizada', 'Respondível', 'Gold contratual unificada e consolidação mensal específica Base PF/FGTS + Fundo Social, ambas sem identificadores pessoais diretos.')
) as t(pergunta_produto, situacao_resposta, justificativa)
