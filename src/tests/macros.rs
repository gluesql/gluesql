#[macro_export]
macro_rules! row {
    ( $( $p:path )* ; $( $v:expr )* ) => (
        vec![$( $p($v) ),*]
    )
}

#[macro_export]
macro_rules! idx {
    () => {
        vec![]
    };
    ($name: path, $op: path, $sql_expr: literal) => {
        vec![ast::IndexItem {
            name: stringify!($name).to_owned(),
            asc: None,
            cmp_expr: Some((
                $op,
                translate::translate_expr(&parse_sql::parse_expr($sql_expr).unwrap()).unwrap(),
            )),
        }]
    };
    ($name: path) => {
        vec![ast::IndexItem {
            name: stringify!($name).to_owned(),
            asc: None,
            cmp_expr: None,
        }]
    };
    ($name: path, ASC) => {
        vec![ast::IndexItem {
            name: stringify!($name).to_owned(),
            asc: Some(true),
            cmp_expr: None,
        }]
    };
    ($name: path, DESC) => {
        vec![ast::IndexItem {
            name: stringify!($name).to_owned(),
            asc: Some(false),
            cmp_expr: None,
        }]
    };
}

#[macro_export]
macro_rules! select {
    ( $( $c: tt )|+ $( ; )? $( $t: path )|+ ; $( $v: expr )+ ; $( $( $v2: expr )+ );+) => ({
        let mut rows = vec![
            row!($( $t )+ ; $( $v )+),
        ];

        executor::Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: concat_with!(rows ; $( $t )+ ; $( $( $v2 )+ );+)
        }
    });
    ( $( $c: tt )|+ $( ; )? $( $t: path )|+ ; $( $v: expr )+ ) => (
        executor::Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: vec![row!($( $t )+ ; $( $v )+ )],
        }
    );
    ( $( $c: tt )|+ $( ; )?) => (
        executor::Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: vec![],
        }
    );
}

#[macro_export]
macro_rules! concat_with {
    ( $rows: ident ; $( $t:path )* ; $( $v: expr )* ) => ({
        $rows.push(row!($( $t )* ; $( $v )*));

        $rows
    });
    ( $rows: ident ; $( $t:path )* ; $( $v: expr )* ; $( $( $v2: expr )* );* ) => ({
        $rows.push(row!($( $t )* ; $( $v )*));

        concat_with!($rows ; $( $t )* ; $( $( $v2 )* );* )
    });
}

#[macro_export]
macro_rules! select_with_null {
    ( $( $c: tt )|* ; $( $v: expr )* ) => (
        executor::Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: vec![vec![$( $v ),*]],
        }
    );
    ( $( $c: tt )|* ; $( $v: expr )* ; $( $( $v2: expr )* );*) => ({
        let mut rows = vec![
            vec![$( $v ),*]
        ];

        executor::Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: concat_with_null!(rows ; $( $( $v2 )* );*),
        }
    });
}

#[macro_export]
macro_rules! concat_with_null {
    ( $rows: ident ; $( $v: expr )* ) => ({
        $rows.push(vec![$( $v ),*]);

        $rows
    });
    ( $rows: ident ; $( $v: expr )* ; $( $( $v2: expr )* );* ) => ({
        $rows.push(vec![$( $v ),*]);

        concat_with_null!($rows ; $( $( $v2 )* );* )
    });
}
