use crate::build_suite;

build_suite!(column_options;
    default,
    nullable,
    types,
    unique,
    (nullable_text, column_options::nullable::nullable_text)
);
