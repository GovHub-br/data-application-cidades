-- UDF para desfazer mojibake gravado NA ORIGEM: arquivos em que "SÃ£o Paulo" está
-- gravado como utf-8 válido, e que nenhuma leitura correta desfaz. É o caso dos exports
-- canônicos do SharePoint. Equivale à corrigir_mojibake_texto() do lake_utils.py.
--
-- O estrago é um round-trip byte a byte, então é reversível sem perda: re-encodar em
-- LATIN1 e decodificar em UTF8 recupera o original exatamente.
--
-- Conservadora, e é isso que a torna segura em qualquer coluna textual:
--   1. sem marcador de mojibake, devolve o texto intacto;
--   2. se o texto não couber em LATIN1, ou os bytes não formarem utf-8 válido, a
--      conversão levanta e o EXCEPTION devolve o original — nunca chuta.
-- É no-op sobre texto limpo.
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
