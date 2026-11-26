-- As Golds da Linha Financiada não podem expor identificadores pessoais.
-- Roda depois das ouros que vigia (macros/depende_de.sql); ouro nova do domínio entra aqui.
{{ depende_de([
        'ouro_linha_financiada_base_agregada',
        'ouro_linha_financiada_base_pf_fundo_social',
        'ouro_linha_financiada_cobertura_perguntas',
        'ouro_linha_financiada_contrapartidas',
        'ouro_linha_financiada_empreendimentos',
        'ouro_linha_financiada_execucao_orcamentaria',
        'ouro_linha_financiada_features_preditivas',
        'ouro_linha_financiada_mapa_execucao',
        'ouro_linha_financiada_mapa_uf',
        'ouro_linha_financiada_projecao_anual',
        'ouro_linha_financiada_ranking_territorial',
        'ouro_linha_financiada_relatorio_semanal',
        'ouro_linha_financiada_resumo_mensal',
    ]) }}
select table_schema, table_name, column_name
from information_schema.columns
where
    table_schema = 'ouro'
    and table_name like 'ouro_linha_financiada_%'
    and
    {{ casa_regex(
        'lower(column_name)',
        '(^|_)(cpf|cnpj|nis|pis|nome_pessoa|nome_beneficiario|endereco|logradouro|cep|data_nascimento)($|_)'
    ) }}
