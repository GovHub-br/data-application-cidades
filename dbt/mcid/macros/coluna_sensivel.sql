{#
    Identificação de colunas que carregam dado pessoal.

    Regra do projeto: **nada de dado sensível na documentação nem nas camadas
    de consumo**, e essa garantia NÃO pode depender de a anonimização a
    montante ter funcionado. Se o mascaramento falhar lá atrás, aqui ainda
    tem que segurar.

    Duas peças usam isto:
      - `mascarar_coluna_sensivel()` — troca o NOME por um rótulo estável nos
        models de qualidade, que são gold e chegam ao Superset. O rótulo
        preserva a detecção de drift (nome mudou -> hash muda) sem publicar o
        identificador.
      - `tests/conjuntura_sem_dado_sensivel.sql` — falha o build se coluna
        assim aparecer em qualquer camada persistida do contínuo.

    A Raw no MinIO é a cópia de origem e não é consultada por este teste. A
    Bronze do contínuo é uma projeção mínima, por isso também não pode conter
    esses identificadores.

    Os padrões são de IDENTIFICADOR DE PESSOA, propositalmente estreitos.
    Termos genéricos como `nome` gerariam falso positivo em coisas como
    `acao_governo_nome` e `periodo_nome`, e um teste que grita à toa é um
    teste que o time desliga.
#}

{% macro padroes_sensiveis() %}
    {{ return([
        'cpf', 'cnpj', 'mutuario', 'nascimento', 'logradouro', 'endereco',
        'telefone', 'celular', 'email', 'nis', 'titular', 'beneficiario',
    ]) }}
{% endmacro %}


{% macro expressao_sensivel(coluna) %}
    (
        lower({{ coluna }}) ~ '({{ padroes_sensiveis() | join("|") }})'
        or lower({{ coluna }}) = 'cep'
    )
{% endmacro %}


{% macro mascarar_coluna_sensivel(coluna) %}
    case
        when {{ expressao_sensivel(coluna) }}
            then 'sensivel_' || substr(md5({{ coluna }}), 1, 8)
        else {{ coluna }}
    end
{% endmacro %}


{#
    ---------------------------------------------------------------------
    Detecção por CONTEÚDO
    ---------------------------------------------------------------------
    Nome de coluna não basta: um CPF pode estar dentro de um campo
    `observacao`, ou a coluna pode ter sido renomeada. As expressões abaixo
    olham o VALOR.

    `parece_cpf` valida o dígito verificador, e não só o formato. Isso
    importa: sequências de 11 dígitos aparecem à vontade em código de
    contrato e identificador interno, e um teste que acusa todos eles é um
    teste que o time desliga na primeira semana. Com o DV, o falso positivo
    fica raro.
#}

{% macro _digito_cpf(v, ate, peso_inicial) %}
    (
        case
            when (11 - mod(
                {%- for i in range(1, ate + 1) %}
                    substr({{ v }}, {{ i }}, 1)::int * {{ peso_inicial - i + 1 }}
                    {{- " +" if not loop.last }}
                {%- endfor %}
            , 11)) >= 10 then 0
            else 11 - mod(
                {%- for i in range(1, ate + 1) %}
                    substr({{ v }}, {{ i }}, 1)::int * {{ peso_inicial - i + 1 }}
                    {{- " +" if not loop.last }}
                {%- endfor %}
            , 11)
        end
    )
{% endmacro %}


{% macro parece_cpf(v) %}
    (
        {{ v }} ~ '^[0-9]{11}$'
        and {{ v }} !~ '^(.)\1{10}$'          {# 00000000000, 11111111111... #}
        and substr({{ v }}, 10, 1)::int = {{ _digito_cpf(v, 9, 10) }}
        and substr({{ v }}, 11, 1)::int = {{ _digito_cpf(v, 10, 11) }}
    )
{% endmacro %}


{% macro parece_dado_pessoal(v) %}
    (
        {#- CPF e CNPJ formatados: forma inequívoca, não precisa de DV -#}
        {{ v }} ~ '[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]{2}'
        or {{ v }} ~ '[0-9]{2}\.[0-9]{3}\.[0-9]{3}/[0-9]{4}-[0-9]{2}'
        {#- e-mail -#}
        or {{ v }} ~* '[a-z0-9._%%+-]+@[a-z0-9.-]+\.[a-z]{2,}'
        {#- CPF sem máscara, confirmado pelo dígito verificador -#}
        or {{ parece_cpf(v) }}
    )
{% endmacro %}
