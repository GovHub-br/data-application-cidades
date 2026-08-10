{#
    ===========================================================================
    Padrao da camada silver -- v2
    ===========================================================================
    Implementa docs/padrao-camada-silver.md. Aplicar na PROJECAO EXTERNA do
    model (lado Postgres), depois que o dado saiu do bloco duckdb.query.

    O PRINCIPIO QUE ORGANIZA TUDO
    Silver guarda a verdade; gold decide a apresentacao.

    Ausencia em atributo descritivo e ruido: 'Não Informado' e mais util que
    NULL, porque atravessa join e ferramenta de BI sem surpresa. Ausencia em
    MEDIDA e informacao: trocar por zero faz media, minimo e razao mentirem,
    e ninguem recebe erro. Por isso as duas coisas sao tratadas de formas
    opostas -- e nao por inconsistencia.

        atributo (texto, codigo, UF)  -> nunca nulo
        boolean                        -> nunca nulo
        contagem verdadeira            -> nunca nula (zero e o valor certo)
        MEDIDA (valor, percentual)     -> NULL preservado
        DATA                           -> NULL preservado
        chave                          -> nunca nula (a linha nao entra)

    Quem consome e nao tolera nulo -- Superset, uma planilha -- recebe o
    tratamento na gold, com medida_ou_zero() e data_ou_sentinela().
#}


{# --------------------------------------------------------------------------
   Constantes
   -------------------------------------------------------------------------- #}

{% macro silver_texto_ausente() %}'Não Informado'{% endmacro %}
{% macro silver_uf_ausente() %}'ND'{% endmacro %}

{#
    Fuso padrao das origens. Todas as datas do FGTS chegam sem fuso e sao
    horario de Brasilia; a conversao para UTC acontece no silver_timestamp.
#}
{% macro fuso_origem() %}'America/Sao_Paulo'{% endmacro %}

{#
    Sentinela de data -- usada SOMENTE na gold, por data_ou_sentinela(), quando
    a ferramenta de consumo nao aceita nulo. Nunca na silver.
#}
{% macro data_sentinela() %}'0001-01-01'{% endmacro %}


{# --------------------------------------------------------------------------
   Atributos -- nunca nulos
   -------------------------------------------------------------------------- #}

{#
    Texto. Alem do NULL, trata os tres disfarces de ausencia que aparecem nas
    origens: string vazia e os literais 'None' e 'NaN' -- que sao texto, nao
    nulo, e por isso atravessam coalesce e is null sem serem notados.
#}
{% macro silver_texto(expressao, ausente=none) %}
    coalesce(
        nullif(nullif(nullif(trim({{ expressao }}), ''), 'None'), 'NaN'),
        {{ ausente or silver_texto_ausente() }}
    )
{% endmacro %}


{#
    Codigo ou identificador. Igual ao texto, mas com normalizacao de caixa
    opcional -- ligue `maiuscula` quando a origem for inconsistente e voce
    tiver certeza de que o codigo nao e case-sensitive.

    Codigo e sempre varchar, nunca inteiro: zero a esquerda e significativo e
    desaparece no cast.
#}
{% macro silver_codigo(expressao, maiuscula=false) %}
    {%- set limpo = "nullif(nullif(nullif(trim(" ~ expressao ~ "), ''), 'None'), 'NaN')" -%}
    coalesce(
        {% if maiuscula %}upper({{ limpo }}){% else %}{{ limpo }}{% endif %},
        {{ silver_texto_ausente() }}
    )
{% endmacro %}


{#
    Unidade federativa. Largura fixa de dois caracteres, entao 'ND' em vez de
    'Não Informado', para nao estourar o formato esperado por mapas e filtros.
#}
{% macro silver_uf(expressao) %}
    coalesce(
        upper(nullif(nullif(nullif(trim({{ expressao }}), ''), 'None'), 'NaN')),
        {{ silver_uf_ausente() }}
    )
{% endmacro %}


{#
    Boolean. Ausencia vira falso -- em flags do tipo is_paralisada, "nao
    sabemos" e "nao esta" tem o mesmo efeito a jusante. Se a distincao importar
    no seu caso, o certo e uma coluna de status com tres valores, nao um
    boolean.
#}
{% macro silver_booleano(expressao) %}
    coalesce({{ expressao }}, false)::boolean
{% endmacro %}


{#
    Contagem verdadeira -- numero de itens contados, em que "nenhum" e uma
    resposta legitima. Ex.: quantidade de parcelas registradas.

    NAO use para medida que pode simplesmente nao ter sido informada; para
    essa, use silver_medida_inteira().
#}
{% macro silver_contagem(expressao) %}
    coalesce({{ expressao }}, 0)::integer
{% endmacro %}


{# --------------------------------------------------------------------------
   Medidas -- NULL preservado
   -------------------------------------------------------------------------- #}

{#
    Valor monetario ou percentual. NULL quando a origem nao informou.

    Zero aqui seria mentira: distorce media, minimo e qualquer razao, e o erro
    nao aparece como erro. Se o consumidor precisa de zero, a gold decide com
    medida_ou_zero().

    Armazenado como numeric(15,2) -- numero, sem formatacao. Formato brasileiro
    e apresentacao: formata_valor_br(), na gold.
#}
{% macro silver_medida(expressao, precisao=15, escala=2) %}
    ({{ expressao }})::numeric({{ precisao }}, {{ escala }})
{% endmacro %}


{#
    Medida inteira -- quantidade que pode nao ter sido informada.
#}
{% macro silver_medida_inteira(expressao) %}
    ({{ expressao }})::integer
{% endmacro %}


{# --------------------------------------------------------------------------
   Datas e fuso
   -------------------------------------------------------------------------- #}

{#
    Timestamp com fuso. As origens gravam horario local sem fuso; aqui ele e
    interpretado no fuso de origem e armazenado como timestamptz, que o
    Postgres normaliza para UTC internamente.

    Isso resolve a classe de bug mais silenciosa de camada analitica: dois
    models comparando horarios que parecem iguais e nao sao, ou agregacao
    diaria que muda de resultado conforme o fuso da sessao.

    NULL preservado -- data ausente e informacao, nao ruido.
#}
{% macro silver_timestamp(expressao, fuso=none) %}
    (({{ expressao }})::timestamp at time zone {{ fuso or fuso_origem() }})
{% endmacro %}


{#
    Data pura, sem hora. Nao carrega fuso por definicao -- uma data de
    assinatura de contrato e a mesma em qualquer lugar do mundo.
#}
{% macro silver_data(expressao) %}
    ({{ expressao }})::date
{% endmacro %}


{# --------------------------------------------------------------------------
   Consumo na gold -- onde a apresentacao decide
   -------------------------------------------------------------------------- #}

{#
    Zera a medida para exibicao. Use quando o consumidor precisa de numero e
    "sem informacao" pode ser mostrado como zero -- tipicamente em soma, nunca
    em media.
#}
{% macro medida_ou_zero(coluna) %}
    coalesce({{ coluna }}, 0)
{% endmacro %}


{#
    Substitui data nula pela sentinela, para ferramenta que nao aceita nulo.
    So na gold, e so em coluna destinada a exibicao.
#}
{% macro data_ou_sentinela(coluna) %}
    coalesce({{ coluna }}, {{ data_sentinela() }}::timestamptz)
{% endmacro %}


{#
    Predicado legivel para contar quem tem data.

        count(*) filter (where {{ tem_data('p.data_termino') }})
#}
{% macro tem_data(coluna) %}
    ({{ coluna }} is not null)
{% endmacro %}


{#
    Exibicao no fuso local. O dado esta em timestamptz (UTC por dentro);
    converta apenas na saida.
#}
{% macro exibe_local(coluna, fuso=none) %}
    ({{ coluna }} at time zone {{ fuso or fuso_origem() }})
{% endmacro %}


{#
    Formatacao brasileira -- 1.234.567,89. Ponto no milhar, virgula no decimal.

    So na GOLD, e apenas em coluna de exibicao. O resultado e TEXTO: nao soma,
    nao ordena e nao compara. Mantenha sempre a coluna numerica ao lado.

    O truque dos tres replace evita depender de lc_numeric do servidor, que
    varia por instalacao.
#}
{% macro formata_valor_br(coluna, casas=2) %}
    replace(
        replace(
            replace(
                to_char({{ coluna }}, 'FM999999999999990{% if casas > 0 %}.{{ '0' * casas }}{% endif %}'),
                '.', '@'
            ),
            ',', '.'
        ),
        '@', ','
    )
{% endmacro %}


{#
    Data no formato brasileiro, ja convertida para o fuso local.
#}
{% macro formata_data_br(coluna, fuso=none) %}
    to_char({{ exibe_local(coluna, fuso) }}, 'DD/MM/YYYY')
{% endmacro %}
