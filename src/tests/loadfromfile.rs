use crate::*;

use crate::{parse, sled, Glue, SledStorage};
use std::convert::TryFrom;
use std::fs;

pub fn loadfromfile(_tester: impl tests::Tester) {
    let config = sled::Config::default()
        .path("examples/recipes/recipe.db")
        .temporary(false);
    let storage = SledStorage::try_from(config).unwrap();
    let mut glue = Glue::new(storage);
    let tables = vec![
        "examples/recipes/ingredient.sql",
        "examples/recipes/category.sql",
        "examples/recipes/categoryandrecipe.sql",
        "examples/recipes/recipe.sql",
        "examples/recipes/recipestep.sql",
        "examples/recipes/recipestepingredient.sql",
    ];
    // Load all tables in the database
    for atable in tables {
        let sqls = fs::read_to_string(atable).expect("Something went wrong reading the file");
        for query in parse(&sqls).unwrap() {
            let _result = glue.execute(&query);
        }
    }
    // LOAD DONE

    // Do some elemantary selections. no automatic validation yet

    let statements = vec![
        vec![
            "SELECT * FROM ingredient ",
            "WHERE energy > 400 ",
            "ORDER BY 1;",
        ],
        vec![
            "SELECT * FROM category ",
            "WHERE categoryid > 40 ",
            "order BY 1;",
        ],
        vec![
            "SELECT * FROM recipe ",
            "WHERE maketime > 100 ",
            "order BY 1;",
        ],
        vec![
            "SELECT * FROM recipestep ",
            "WHERE minutes > 100 ",
            "order BY 1;",
        ],
        //which steps use a certain ingredientid
        vec![
            "SELECT * FROM recipestepingredient ",
            "WHERE ingredientid = 38 ",
            "order BY 1;",
        ],
        vec![
            "SELECT ingredientid ",
            "FROM   ingredient ",
            "WHERE  ingredientname = 'mjölk';",
        ],
        // what recipes use ingredient 'mjölk'
        // slow
        vec![
            "SELECT recipename ",
            "FROM   recipe ",
            "       JOIN recipestepingredient ",
            "         ON recipe.recipeid = recipestepingredient.recipeid ",
            "WHERE  ingredientid = (SELECT ingredientid ",
            "                       FROM   ingredient ",
            "                       WHERE  ingredientname = 'mjölk') ;",
        ],
        // same as above but with 3 joins. slow
        vec![
            "SELECT recipename ",
            "FROM   recipe ",
            "        JOIN recipestepingredient ",
            "          ON recipe.recipeid = recipestepingredient.recipeid ",
            "        JOIN ingredient ",
            "          ON ingredient.ingredientid = recipestepingredient.ingredientid ",
            "WHERE  ingredient.ingredientname = 'mjölk';",
        ],
        // what are the most used category in recipe.
        vec![
            "SELECT categoryid, ",
            "       Count(*) ",
            "FROM   categoryandrecipe ",
            "GROUP  BY categoryid ",
            "ORDER BY 2;",
        ],
        // what are the most used category in recipe. answer recipename and count
        // will give Err 'TableFactorNotSupported'
        vec![
            "SELECT derived_alias.categoryname, ",
            "       Count(*) ",
            "FROM   (SELECT category.categoryname ",
            "        FROM   category ",
            "               JOIN categoryandrecipe ",
            "                 ON category.categoryid = categoryandrecipe.categoryid) AS ",
            "       derived_alias ",
            "GROUP  BY derived_alias.categoryname ;",
        ],
    ];
    for (i, statementpart) in statements.iter().enumerate() {
        eprintln!("sql({:?})={:#?}", i, statementpart);
        for query in parse(&statementpart.concat()).unwrap() {
            match glue.execute(&query).unwrap() {
                Payload::Select(rows) => {
                    for arow in rows {
                        let Row(values) = arow;
                        for acolumnvalue in values {
                            match acolumnvalue {
                                Value::Str(ref s) => eprint!("{:?}\t", s),
                                Value::F64(ref f) => eprint!("{:?}\t", f),
                                Value::I64(ref i) => eprint!("{:?}\t", i),
                                _ => eprint!("acolumnvalue={:?}", acolumnvalue),
                            };
                        }
                        eprintln!("");
                    }
                }
                _ => { /* Payload::Select*/ }
            }
        }
    }
}
