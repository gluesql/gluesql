#[macro_export]
macro_rules! row {
    ( $( $p:path )* ; $( $v:expr )* ) => (
        Row(vec![$( $p($v) ),*])
    )
}

#[macro_export]
macro_rules! select {
    ( $( $c: tt )|+ $( ; )? $( $t: path )|+ ; $( $v: expr )+ ; $( $( $v2: expr )+ );+) => ({
        let mut rows = vec![
            row!($( $t )+ ; $( $v )+),
        ];

        Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: concat_with!(rows ; $( $t )+ ; $( $( $v2 )+ );+)
        }
    });
    ( $( $c: tt )|+ $( ; )? $( $t: path )|+ ; $( $v: expr )+ ) => (
        Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: vec![row!($( $t )+ ; $( $v )+ )],
        }
    );
    ( $( $c: tt )|+ $( ; )?) => (
        Payload::Select {
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
macro_rules! select_with_empty {
    ( $( $c: tt )|* ; $( $v: expr )* ) => (
        Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: vec![Row(vec![$( $v ),*])],
        }
    );
    ( $( $c: tt )|* ; $( $v: expr )* ; $( $( $v2: expr )* );*) => ({
        let mut rows = vec![
            Row(vec![$( $v ),*])
        ];

        Payload::Select {
            labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            rows: concat_with_empty!(rows ; $( $( $v2 )* );*),
        }
    });
}

#[macro_export]
macro_rules! concat_with_empty {
    ( $rows: ident ; $( $v: expr )* ) => ({
        $rows.push(Row(vec![$( $v ),*]));

        $rows
    });
    ( $rows: ident ; $( $v: expr )* ; $( $( $v2: expr )* );* ) => ({
        $rows.push(Row(vec![$( $v ),*]));

        concat_with_empty!($rows ; $( $( $v2 )* );* )
    });
}
