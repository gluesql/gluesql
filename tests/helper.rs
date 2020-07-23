#[macro_export]
macro_rules! test {
    ($title: ident, $content: expr) => {
        #[test_case(memory_storage::MemoryTester::new() ; "memory")]
        #[test_case(sled_storage::SledTester::new(&format!("data/{}", stringify!($title))) ; "sled")]
        fn $title<T: 'static + std::fmt::Debug>(mut tester: impl gluesql::Tester<T>) {
            $content
        }
    };
}

#[macro_export]
macro_rules! row {
    ( $( $p:path )* ; $( $v:expr )* ) => (
        Row(vec![$( $p($v) ),*])
    )
}

#[macro_export]
macro_rules! select {
    ( $( $t:path )* ; $( $v: expr )* ) => (
        Payload::Select(vec![
            row!($( $t )* ; $( $v )* )
        ])
    );
    ( $( $t:path )* ; $( $v: expr )* ; $( $( $v2: expr )* );*) => ({
        let mut rows = vec![
            row!($( $t )* ; $( $v )*),
        ];

        Payload::Select(
            concat_with!(rows ; $( $t )* ; $( $( $v2 )* );*)
        )
    });
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
    ( $( $v: expr )* ) => (
        Payload::Select(vec![
            Row(vec![$( $v ),*])
        ])
    );
    ( $( $v: expr )* ; $( $( $v2: expr )* );*) => ({
        let mut rows = vec![
            Row(vec![$( $v ),*])
        ];

        Payload::Select(
            concat_with_empty!(rows ; $( $( $v2 )* );*)
        )
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
