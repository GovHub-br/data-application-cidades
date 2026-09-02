-- UDF para desfazer mojibake gravado NA ORIGEM.
--
-- Diferente do bug que corrigimos no raw_para_staging.py (lá o decode era nosso e foi
-- consertado), alguns arquivos chegam com o mojibake já dentro deles: o valor "SÃ£o
-- Paulo" está gravado como utf-8 válido no CSV. É o caso dos exports canônicos do
-- SharePoint, que saíram do dump antigo do Postgres — o dump foi ingerido com o bug, o
-- export preservou o estrago, e agora nenhuma leitura correta o desfaz.
--
-- Como o estrago é um round-trip byte a byte, ele é reversível sem perda: re-encodar em
-- LATIN1 e decodificar em UTF8 recupera o original exatamente. É o mesmo raciocínio da
-- corrigir_mojibake_texto() do scripts/lake_utils.py, aqui em SQL.
--
-- Conservadora, e é isso que a torna segura de aplicar em qualquer coluna textual:
--   1. sem marcador de mojibake, devolve o texto intacto (o caso da esmagadora maioria);
--   2. se o texto não couber em LATIN1, ou se os bytes não formarem utf-8 válido, a
--      conversão levanta e o EXCEPTION devolve o original — nunca chuta.
-- Ou seja, é no-op sobre texto limpo. Quando a origem for corrigida, ela para de agir
-- sozinha, sem precisar mexer nos models.
{% macro create_f_corrigir_mojibake() %}

    create or replace function {{ target.schema }}.corrigir_mojibake(in_text text)
    returns text
    as $$
    declare
        atual text := in_text;
        tentativa text;
        i int;
    begin
        if in_text is null then
            return null;
        end if;

        -- Até 3 passadas: há texto que passou pelo round-trip mais de uma vez
        -- ("SÃƒÂ£o" é o mojibake do mojibake de "São"). Para no ponto fixo.
        for i in 1..3 loop
            -- Ã cobre os acentos latinos, Â os símbolos (º, °), â€ a pontuação
            if atual !~ '[ÃÂ]|â€' then
                return atual;
            end if;

            begin
                -- WIN1252 primeiro, e não LATIN1: a corrupção veio de ler utf-8 como
                -- cp1252, e só o cp1252 tem € (0x80), aspas curvas (0x93/0x94) e
                -- travessão (0x97). Com LATIN1 sozinho, todo texto livre com pontuação
                -- tipográfica levantava e voltava intacto — era o caso dos 9.603
                -- situacao_detalhamento da CAIXA.
                tentativa := convert_from(convert_to(atual, 'WIN1252'), 'UTF8');
            exception
                when others then
                    begin
                        tentativa := convert_from(convert_to(atual, 'LATIN1'), 'UTF8');
                    exception
                        when others then
                            return atual;
                    end;
            end;

            if tentativa = atual then
                return atual;
            end if;
            atual := tentativa;
        end loop;

        return atual;
    end;
    $$
    language plpgsql
    immutable
    ;

{% endmacro %}
