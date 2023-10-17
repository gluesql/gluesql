use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        prelude::Value::*,
    },
};

test_case!(character_conversion, {
    let glue = get_glue!();

    let actual = values(vec![
        vec![f::ascii("'\t'"), f::chr(9)],
        vec![f::ascii("'\n'"), f::chr(10)],
        vec![f::ascii("'\r'"), f::chr(13)],
        vec![f::ascii("' '"), f::chr(32)],
        vec![f::ascii("'!'"), f::chr(33)],
        vec![f::ascii("'\"'"), f::chr(34)],
        vec![f::ascii("'#'"), f::chr(35)],
        vec![f::ascii("'$'"), f::chr(36)],
        vec![f::ascii("'%'"), f::chr(37)],
        vec![f::ascii("'&'"), f::chr(38)],
        vec![f::ascii("''''"), f::chr(39)],
        vec![f::ascii("','"), f::chr(44)],
    ])
    .alias_as("Sub")
    .select()
    .project("column1 AS ascii")
    .project("column2 AS char")
    .execute(glue)
    .await;
    let expected = Ok(select!(
        ascii | char
        U8    | Str;
        9        "\t".to_owned();
        10       "\n".to_owned();
        13       "\r".to_owned();
        32       " ".to_owned();
        33       "!".to_owned();
        34       "\"".to_owned();
        35       "#".to_owned();
        36       "$".to_owned();
        37       "%".to_owned();
        38       "&".to_owned();
        39       "'".to_owned();
        44        ",".to_owned()
    ));
    assert_eq!(actual, expected, "ascii and char set should match");
});
