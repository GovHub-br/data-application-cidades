{#
    Os nomes de coluna que a staging antiga gravava, sobre o cabeçalho original.

    A ingestão nova (plugins/ingestion) guarda na staging o cabeçalho como a fonte
    mandou; o `raw_para_staging` gravava nomes normalizados
    (`ingestion.text.normalizar_colunas`), e é por eles que as pratas leem. Este
    macro reproduz a regra na leitura, para que trocar a bronze de origem não mude
    a prata:

        select apf, situacao_obra, _source_file, _ingested_at
        from {{ normalizar_colunas(ref('bronze_sftp_int059_fds_caixa_empreendimentos')) }} as b

    - mojibake desfeito quando o round-trip utf-8 -> latin-1 é reversível;
    - acento tirado pelo NFKD (o mapa de `_dobra_ascii`, gerado do `unicodedata`
      para todo o BMP; fora dele o caractere não ASCII cai);
    - minúsculas, tudo que não é [a-z0-9] vira `_`, vazio vira `coluna_<i>` e
      repetido ganha `_2`, `_3`...;
    - `_source_file` (o `filename` da staging) e `_ingested_at` (texto, a
      partição da ingestão) no lugar das colunas de linhagem da staging antiga.

    A relação precisa existir quando o modelo compila (os nomes saem do catálogo)
    e ter a coluna `filename` (as fontes com `load_mode` no `fonte_lake`).
    tests/ingestion/dbt/test_normalizar_colunas.py compara com a função Python.
#}
{% macro normalizar_colunas(relation) %}
    {%- if not execute -%}{{ return(relation) }}{%- endif -%}
    {%- set nomes = adapter.get_columns_in_relation(relation)
        | map(attribute="name") | list -%}
    {%- if "filename" not in nomes -%}
        {{ exceptions.raise_compiler_error(
            "normalizar_colunas: " ~ relation ~ " não tem a coluna filename") }}
    {%- endif -%}
    {%- set originais = nomes | reject("equalto", "filename") | list -%}
    {%- set finais = nomes_normalizados(originais) -%}
    (
        select
            {%- for original in originais %}
            "{{ original | replace('"', '""') }}" as "{{ finais[loop.index0] }}",
            {%- endfor %}
            filename as _source_file,
            cast({{ lake_dt_ingest("filename") }} as varchar) as _ingested_at
        from {{ relation }}
    )
{% endmacro %}


{#- normalizar_colunas de ingestion.text: normaliza e deduplica, na ordem. -#}
{% macro nomes_normalizados(originais) %}
    {%- set finais = [] -%}
    {%- set dobra = _dobra_ascii() -%}
    {%- for original in originais -%}
        {%- set base = norm_header(original, dobra) or "coluna_" ~ loop.index -%}
        {%- set nome = namespace(valor=base, n=2) -%}
        {%- if base in finais -%}
            {#- o while do Python: no máximo uma volta por nome já usado -#}
            {%- for _ in finais if nome.valor in finais -%}
                {%- set nome.valor = base ~ "_" ~ nome.n -%}
                {%- set nome.n = nome.n + 1 -%}
            {%- endfor -%}
        {%- endif -%}
        {%- do finais.append(nome.valor) -%}
    {%- endfor -%}
    {{ return(finais) }}
{% endmacro %}


{#- norm_header de ingestion.text: snake_case ASCII. -#}
{% macro norm_header(texto, dobra=none) %}
    {%- set re = modules.re -%}
    {%- set dobra = dobra or _dobra_ascii() -%}
    {%- set ascii = namespace(texto="") -%}
    {%- for c in _corrigir_mojibake(texto) -%}
        {%- set ascii.texto = ascii.texto ~ dobra.get(c, c) -%}
    {%- endfor -%}
    {%- set s = re.sub("[^\\x00-\\x7f]", "", ascii.texto) -%}
    {%- set s = s.lower().strip().strip('"').strip() -%}
    {{ return(re.sub("[^a-z0-9]+", "_", s).strip("_")) }}
{% endmacro %}


{#- corrigir_mojibake_texto de ingestion.text: só quando o texto tem um marcador,
    é representável em latin-1 e esses bytes são utf-8 válido (a regex é a
    gramática do utf-8 sobre os caracteres latin-1, as duas condições juntas). -#}
{% macro _corrigir_mojibake(texto) %}
    {%- set utf8 = (
        "(?:[\\x00-\\x7f]|[\\xc2-\\xdf][\\x80-\\xbf]"
        ~ "|\\xe0[\\xa0-\\xbf][\\x80-\\xbf]"
        ~ "|[\\xe1-\\xec\\xee\\xef][\\x80-\\xbf]{2}"
        ~ "|\\xed[\\x80-\\x9f][\\x80-\\xbf]"
        ~ "|\\xf0[\\x90-\\xbf][\\x80-\\xbf]{2}"
        ~ "|[\\xf1-\\xf3][\\x80-\\xbf]{3}"
        ~ "|\\xf4[\\x80-\\x8f][\\x80-\\xbf]{2})*"
    ) -%}
    {%- set marcado = "\u00c3" in texto or "\u00c2" in texto or "\u00e2\u20ac" in texto -%}
    {%- if marcado and modules.re.fullmatch(utf8, texto) -%}
        {{ return(texto.encode("latin-1").decode("utf-8")) }}
    {%- endif -%}
    {{ return(texto) }}
{% endmacro %}


{#- Caractere -> ASCII do NFKD, agrupado pelo resultado. Gerado com
    unicodedata.normalize("NFKD", c).encode("ascii", "ignore") para
    U+0080-U+FFFF; o teste confere o mapa inteiro. -#}
{% macro _dobra_ascii() %}
    {%- set grupos = {
        "\u0020": "\u00a0¨¯´¸˘˙˚˛˜˝ͺ΄΅᾽᾿῀῁῍῎῏῝῞῟῭΅´῾\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a‗\u202f‾\u205f\u3000゛゜ﱞﱟﱠﱡﱢﱣﷻ﹉﹊﹋﹌ﹰﹲﹴﹶﹸﹺﹼﹾ￣",
        "\u0020\u0020\u0020": "ﷺ",
        "!": "︕﹗！",
        "!!": "‼",
        "!?": "⁉",
        "\"": "＂",
        "#": "﹟＃",
        "$": "﹩＄",
        "%": "﹪％",
        "&": "﹠＆",
        "'": "＇",
        "(": "⁽₍︵﹙（",
        "()": "㈀㈁㈂㈃㈄㈅㈆㈇㈈㈉㈊㈋㈌㈍㈎㈏㈐㈑㈒㈓㈔㈕㈖㈗㈘㈙㈚㈛㈜㈝㈞㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩㈪㈫㈬㈭㈮㈯㈰㈱㈲㈳㈴㈵㈶㈷㈸㈹㈺㈻㈼㈽㈾㈿㉀㉁㉂㉃",
        "(1)": "⑴",
        "(10)": "⑽",
        "(11)": "⑾",
        "(12)": "⑿",
        "(13)": "⒀",
        "(14)": "⒁",
        "(15)": "⒂",
        "(16)": "⒃",
        "(17)": "⒄",
        "(18)": "⒅",
        "(19)": "⒆",
        "(2)": "⑵",
        "(20)": "⒇",
        "(3)": "⑶",
        "(4)": "⑷",
        "(5)": "⑸",
        "(6)": "⑹",
        "(7)": "⑺",
        "(8)": "⑻",
        "(9)": "⑼",
        "(a)": "⒜",
        "(b)": "⒝",
        "(c)": "⒞",
        "(d)": "⒟",
        "(e)": "⒠",
        "(f)": "⒡",
        "(g)": "⒢",
        "(h)": "⒣",
        "(i)": "⒤",
        "(j)": "⒥",
        "(k)": "⒦",
        "(l)": "⒧",
        "(m)": "⒨",
        "(n)": "⒩",
        "(o)": "⒪",
        "(p)": "⒫",
        "(q)": "⒬",
        "(r)": "⒭",
        "(s)": "⒮",
        "(t)": "⒯",
        "(u)": "⒰",
        "(v)": "⒱",
        "(w)": "⒲",
        "(x)": "⒳",
        "(y)": "⒴",
        "(z)": "⒵",
        ")": "⁾₎︶﹚）",
        "*": "﹡＊",
        "+": "⁺₊﬩﹢＋",
        ",": "︐﹐，",
        "-": "﹣－",
        ".": "․﹒．",
        "..": "‥︰",
        "...": "…︙",
        "/": "／",
        "0": "⁰₀⓪㍘０",
        "03": "↉",
        "1": "¹₁⅟①㋀㍙㏠１",
        "1.": "⒈",
        "10": "⑩㋉㍢㏩",
        "10.": "⒑",
        "11": "⑪㋊㍣㏪",
        "11.": "⒒",
        "110": "⅒",
        "12": "½⑫㋋㍤㏫",
        "12.": "⒓",
        "13": "⅓⑬㍥㏬",
        "13.": "⒔",
        "14": "¼⑭㍦㏭",
        "14.": "⒕",
        "15": "⅕⑮㍧㏮",
        "15.": "⒖",
        "16": "⅙⑯㍨㏯",
        "16.": "⒗",
        "17": "⅐⑰㍩㏰",
        "17.": "⒘",
        "18": "⅛⑱㍪㏱",
        "18.": "⒙",
        "19": "⅑⑲㍫㏲",
        "19.": "⒚",
        "2": "²₂②㋁㍚㏡２",
        "2.": "⒉",
        "20": "⑳㍬㏳",
        "20.": "⒛",
        "21": "㉑㍭㏴",
        "22": "㉒㍮㏵",
        "23": "⅔㉓㍯㏶",
        "24": "㉔㍰㏷",
        "25": "⅖㉕㏸",
        "26": "㉖㏹",
        "27": "㉗㏺",
        "28": "㉘㏻",
        "29": "㉙㏼",
        "3": "³₃③㋂㍛㏢３",
        "3.": "⒊",
        "30": "㉚㏽",
        "31": "㉛㏾",
        "32": "㉜",
        "33": "㉝",
        "34": "¾㉞",
        "35": "⅗㉟",
        "36": "㊱",
        "37": "㊲",
        "38": "⅜㊳",
        "39": "㊴",
        "4": "⁴₄④㋃㍜㏣４",
        "4.": "⒋",
        "40": "㊵",
        "41": "㊶",
        "42": "㊷",
        "43": "㊸",
        "44": "㊹",
        "45": "⅘㊺",
        "46": "㊻",
        "47": "㊼",
        "48": "㊽",
        "49": "㊾",
        "5": "⁵₅⑤㋄㍝㏤５",
        "5.": "⒌",
        "50": "㊿",
        "56": "⅚",
        "58": "⅝",
        "6": "⁶₆⑥㋅㍞㏥６",
        "6.": "⒍",
        "7": "⁷₇⑦㋆㍟㏦７",
        "7.": "⒎",
        "78": "⅞",
        "8": "⁸₈⑧㋇㍠㏧８",
        "8.": "⒏",
        "9": "⁹₉⑨㋈㍡㏨９",
        "9.": "⒐",
        ":": "︓﹕：",
        "::=": "⩴",
        ";": ";︔﹔；",
        "<": "≮﹤＜",
        "=": "⁼₌≠﹦＝",
        "==": "⩵",
        "===": "⩶",
        ">": "≯﹥＞",
        "?": "︖﹖？",
        "?!": "⁈",
        "??": "⁇",
        "@": "﹫＠",
        "A": "ÀÁÂÃÄÅĀĂĄǍǞǠǺȀȂȦᴬḀẠẢẤẦẨẪẬẮẰẲẴẶÅⒶ㎂Ａ",
        "AU": "㍳",
        "Am": "㏟",
        "B": "ᴮḂḄḆℬⒷＢ",
        "Bq": "㏃",
        "C": "ÇĆĈĊČḈℂ℃ℭⅭⒸꟲＣ",
        "Ckg": "㏆",
        "Co.": "㏇",
        "D": "ĎᴰḊḌḎḐḒⅅⅮⒹＤ",
        "DZ": "ǄǱ",
        "Dz": "ǅǲ",
        "E": "ÈÉÊËĒĔĖĘĚȄȆȨᴱḔḖḘḚḜẸẺẼẾỀỂỄỆℰⒺＥ",
        "F": "Ḟ℉ℱⒻ㎌ꟳＦ",
        "FAX": "℻",
        "G": "ĜĞĠĢǦǴᴳḠⒼＧ",
        "GB": "㎇",
        "GHz": "㎓",
        "GPa": "㎬",
        "Gy": "㏉",
        "H": "ĤȞᴴḢḤḦḨḪℋℌℍⒽＨ",
        "HP": "㏋",
        "Hg": "㋌",
        "Hz": "㎐",
        "I": "ÌÍÎÏĨĪĬĮİǏȈȊᴵḬḮỈỊℐℑⅠⒾＩ",
        "II": "Ⅱ",
        "III": "Ⅲ",
        "IJ": "Ĳ",
        "IU": "㍺",
        "IV": "Ⅳ",
        "IX": "Ⅸ",
        "J": "ĴᴶⒿＪ",
        "K": "ĶǨᴷḰḲḴKⓀＫ",
        "KB": "㎅",
        "KK": "㏍",
        "KM": "㏎",
        "L": "ĹĻĽĿᴸḶḸḺḼℒⅬⓁＬ",
        "LJ": "Ǉ",
        "LTD": "㋏",
        "Lj": "ǈ",
        "M": "ᴹḾṀṂℳⅯⓂ㏁Ｍ",
        "MB": "㎆",
        "MHz": "㎒",
        "MPa": "㎫",
        "MV": "㎹",
        "MW": "㎿",
        "N": "ÑŃŅŇǸᴺṄṆṈṊℕⓃＮ",
        "NJ": "Ǌ",
        "Nj": "ǋ",
        "No": "№",
        "O": "ÒÓÔÕÖŌŎŐƠǑǪǬȌȎȪȬȮȰᴼṌṎṐṒỌỎỐỒỔỖỘỚỜỞỠỢⓄＯ",
        "P": "ᴾṔṖℙⓅＰ",
        "PH": "㏗",
        "PPM": "㏙",
        "PR": "㏚",
        "PTE": "㉐",
        "Pa": "㎩",
        "Q": "ℚⓆꟴＱ",
        "R": "ŔŖŘȐȒᴿṘṚṜṞℛℜℝⓇＲ",
        "Rs": "₨",
        "S": "ŚŜŞŠȘṠṢṤṦṨⓈＳ",
        "SM": "℠",
        "Sv": "㏜",
        "T": "ŢŤȚᵀṪṬṮṰⓉＴ",
        "TEL": "℡",
        "THz": "㎔",
        "TM": "™",
        "U": "ÙÚÛÜŨŪŬŮŰŲƯǓǕǗǙǛȔȖᵁṲṴṶṸṺỤỦỨỪỬỮỰⓊＵ",
        "V": "ṼṾⅤⓋⱽ㎶Ｖ",
        "VI": "Ⅵ",
        "VII": "Ⅶ",
        "VIII": "Ⅷ",
        "Vm": "㏞",
        "W": "ŴᵂẀẂẄẆẈⓌ㎼Ｗ",
        "Wb": "㏝",
        "X": "ẊẌⅩⓍＸ",
        "XI": "Ⅺ",
        "XII": "Ⅻ",
        "Y": "ÝŶŸȲẎỲỴỶỸⓎＹ",
        "Z": "ŹŻŽẐẒẔℤℨⓏＺ",
        "[": "﹇［",
        "\\": "﹨＼",
        "]": "﹈］",
        "^": "＾",
        "_": "︳︴﹍﹎﹏＿",
        "`": "`｀",
        "a": "ªàáâãäåāăąǎǟǡǻȁȃȧᵃḁẚạảấầẩẫậắằẳẵặₐⓐａ",
        "a.m.": "㏂",
        "a/c": "℀",
        "a/s": "℁",
        "b": "ᵇḃḅḇⓑｂ",
        "bar": "㍴",
        "c": "çćĉċčᶜḉⅽⓒｃ",
        "c/o": "℅",
        "c/u": "℆",
        "cal": "㎈",
        "cc": "㏄",
        "cd": "㏅",
        "cm": "㎝",
        "cm2": "㎠",
        "cm3": "㎤",
        "d": "ďᵈḋḍḏḑḓⅆⅾⓓｄ",
        "dB": "㏈",
        "da": "㍲",
        "dl": "㎗",
        "dm": "㍷",
        "dm2": "㍸",
        "dm3": "㍹",
        "dz": "ǆǳ",
        "e": "èéêëēĕėęěȅȇȩᵉḕḗḙḛḝẹẻẽếềểễệₑℯⅇⓔｅ",
        "eV": "㋎",
        "erg": "㋍",
        "f": "ᶠḟⓕｆ",
        "ff": "ﬀ",
        "ffi": "ﬃ",
        "ffl": "ﬄ",
        "fi": "ﬁ",
        "fl": "ﬂ",
        "fm": "㎙",
        "g": "ĝğġģǧǵᵍḡℊⓖ㎍ｇ",
        "gal": "㏿",
        "h": "ĥȟʰḣḥḧḩḫẖₕℎⓗｈ",
        "hPa": "㍱",
        "ha": "㏊",
        "i": "ìíîïĩīĭįǐȉȋᵢḭḯỉịⁱℹⅈⅰⓘｉ",
        "ii": "ⅱ",
        "iii": "ⅲ",
        "ij": "ĳ",
        "in": "㏌",
        "iv": "ⅳ",
        "ix": "ⅸ",
        "j": "ĵǰʲⅉⓙⱼｊ",
        "k": "ķǩᵏḱḳḵₖⓚ㏀ｋ",
        "kA": "㎄",
        "kHz": "㎑",
        "kPa": "㎪",
        "kV": "㎸",
        "kW": "㎾",
        "kcal": "㎉",
        "kg": "㎏",
        "kl": "㎘",
        "km": "㎞",
        "km2": "㎢",
        "km3": "㎦",
        "kt": "㏏",
        "l": "ĺļľŀˡḷḹḻḽₗℓⅼⓛ㎕ｌ",
        "lj": "ǉ",
        "lm": "㏐",
        "ln": "㏑",
        "log": "㏒",
        "lx": "㏓",
        "m": "ᵐḿṁṃₘⅿⓜ㎛ｍ",
        "m2": "㎡",
        "m3": "㎥",
        "mA": "㎃",
        "mV": "㎷",
        "mW": "㎽",
        "mb": "㏔",
        "mg": "㎎",
        "mil": "㏕",
        "ml": "㎖",
        "mm": "㎜",
        "mm2": "㎟",
        "mm3": "㎣",
        "mol": "㏖",
        "ms": "㎧㎳",
        "ms2": "㎨",
        "n": "ñńņňŉǹṅṇṉṋⁿₙⓝｎ",
        "nA": "㎁",
        "nF": "㎋",
        "nV": "㎵",
        "nW": "㎻",
        "nj": "ǌ",
        "nm": "㎚",
        "ns": "㎱",
        "o": "ºòóôõöōŏőơǒǫǭȍȏȫȭȯȱᵒṍṏṑṓọỏốồổỗộớờởỡợₒℴⓞｏ",
        "oV": "㍵",
        "p": "ᵖṕṗₚⓟｐ",
        "p.m.": "㏘",
        "pA": "㎀",
        "pF": "㎊",
        "pV": "㎴",
        "pW": "㎺",
        "pc": "㍶",
        "ps": "㎰",
        "q": "ⓠｑ",
        "r": "ŕŗřȑȓʳᵣṙṛṝṟⓡｒ",
        "rad": "㎭",
        "rads": "㎮",
        "rads2": "㎯",
        "s": "śŝşšſșˢṡṣṥṧṩẛₛⓢ㎲ｓ",
        "sr": "㏛",
        "st": "ﬅﬆ",
        "t": "ţťțᵗṫṭṯṱẗₜⓣｔ",
        "u": "ùúûüũūŭůűųưǔǖǘǚǜȕȗᵘᵤṳṵṷṹṻụủứừửữựⓤｕ",
        "v": "ᵛᵥṽṿⅴⓥｖ",
        "vi": "ⅵ",
        "vii": "ⅶ",
        "viii": "ⅷ",
        "w": "ŵʷẁẃẅẇẉẘⓦｗ",
        "x": "ˣẋẍₓⅹⓧｘ",
        "xi": "ⅺ",
        "xii": "ⅻ",
        "y": "ýÿŷȳʸẏẙỳỵỷỹⓨｙ",
        "z": "źżžᶻẑẓẕⓩｚ",
        "{": "︷﹛｛",
        "|": "｜",
        "}": "︸﹜｝",
        "~": "～"
    } -%}
    {%- set dobra = {} -%}
    {%- for ascii, caracteres in grupos.items() -%}
        {%- for c in caracteres -%}{%- do dobra.update({c: ascii}) -%}{%- endfor -%}
    {%- endfor -%}
    {{ return(dobra) }}
{% endmacro %}
