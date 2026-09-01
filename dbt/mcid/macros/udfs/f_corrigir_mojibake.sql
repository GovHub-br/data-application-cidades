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
    begin
        if in_text is null then
            return null;
        end if;
        -- Ã cobre os acentos latinos, Â os símbolos (º, °), â€ a pontuação tipográfica
        if in_text !~ '[ÃÂ]|â€' then
            return in_text;
        end if;
        return convert_from(convert_to(in_text, 'LATIN1'), 'UTF8');
    exception
        when others then
            return in_text;
    end;
    $$
    language plpgsql
    immutable
    ;

{% endmacro %}
