{#
    Mascaramento de PII de mutuário (change frentes-restantes-mcmv-historico,
    D4): `nu_cpf_cnpj_mutuario`, `no_mutuario` e `dt_nascimento_mutuario` SHALL
    existir na bronze (cópia fiel, linhagem/auditoria) mas NÃO SHALL ser
    expostas pela prata — mesmo tratamento já dado a CPF em outras
    frentes/reloginho do projeto (retenção só na camada de auditoria interna,
    nunca em silver/gold consumível).

    Não há transformação (hash/truncamento parcial): a regra é OMISSÃO total
    das 3 colunas da projeção da prata. Este macro é o ponto único que
    declara quais colunas são essas — reaproveitado pelas pratas de Classe
    Média e Reforma Casa Brasil (D3, schemas quase idênticos) e pelo teste
    genérico `pii_mutuario_ausente` (macros/data_quality/pii_mutuario_ausente.sql),
    que confere a ausência das 3 no schema materializado.
#}
{% macro colunas_pii_mutuario() %}
    {{ return(['nu_cpf_cnpj_mutuario', 'no_mutuario', 'dt_nascimento_mutuario']) }}
{% endmacro %}
