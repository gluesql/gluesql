We have test failure in "Run storage tests" github action. The error is comming from "Run cd storages" step. Seems like there's an issue with testing for parquet_storage.
Here's the error message:

```
failures:

---- float_vector stdout ----
fs::remove_file Os { code: 2, kind: NotFound, message: "No such file or directory" }
[RUN] 
CREATE TABLE vectors (
    id INTEGER,
    embedding FLOAT_VECTOR
)
[RUN] 
INSERT INTO vectors VALUES
    (1, '[1.0, 2.0, 3.0]'),
    (2, '[0.5, 1.5, 2.5]'),
    (3, '[2.0, 3.0, 4.0]');


thread 'float_vector' panicked at /home/runner/work/gluesql/gluesql/test-suite/src/tester.rs:143:35:
called `Result::unwrap()` on an `Err` value: Value(FailedToParseHexString("[1.0, 2.0, 3.0]"))
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- float_vector_data_type_validation stdout ----
fs::remove_file Os { code: 2, kind: NotFound, message: "No such file or directory" }
[RUN] CREATE TABLE typed_vectors (id INTEGER, vec FLOAT_VECTOR)
[RUN] INSERT INTO typed_vectors VALUES (1, '[1.0, 2.0]')

thread 'float_vector_data_type_validation' panicked at /home/runner/work/gluesql/gluesql/test-suite/src/tester.rs:143:35:
called `Result::unwrap()` on an `Err` value: Value(FailedToParseHexString("[1.0, 2.0]"))

---- float_vector_json_serialization stdout ----
fs::remove_file Os { code: 2, kind: NotFound, message: "No such file or directory" }
[RUN] CREATE TABLE json_vectors (id INTEGER, data FLOAT_VECTOR)
[RUN] INSERT INTO json_vectors VALUES (1, '[1.5, 2.5, 3.5]')

thread 'float_vector_json_serialization' panicked at /home/runner/work/gluesql/gluesql/test-suite/src/tester.rs:143:35:
called `Result::unwrap()` on an `Err` value: Value(FailedToParseHexString("[1.5, 2.5, 3.5]"))

---- vector_functions_advanced_with_tables stdout ----
fs::remove_file Os { code: 2, kind: NotFound, message: "No such file or directory" }
[RUN] CREATE TABLE similarity_search (id INTEGER, query_vec FLOAT_VECTOR, doc_vec FLOAT_VECTOR)
[RUN] 
        INSERT INTO similarity_search VALUES 
        (1, '[1.0, 0.0, 1.0]', '[1.0, 1.0, 0.0]'),
        (2, '[2.0, 3.0, 1.0]', '[1.0, 2.0, 3.0]'),
        (3, '[0.0, 1.0, 0.0]', '[1.0, 0.0, 1.0]')
    

thread 'vector_functions_advanced_with_tables' panicked at /home/runner/work/gluesql/gluesql/test-suite/src/tester.rs:143:35:
called `Result::unwrap()` on an `Err` value: Value(FailedToParseHexString("[1.0, 0.0, 1.0]"))

---- vector_functions_basic stdout ----
fs::remove_file Os { code: 2, kind: NotFound, message: "No such file or directory" }
[RUN] CREATE TABLE vectors (id INTEGER, vec FLOAT_VECTOR)
[RUN] INSERT INTO vectors VALUES (1, '[3.0, 4.0]'), (2, '[1.0, 1.0, 1.0]')

thread 'vector_functions_basic' panicked at /home/runner/work/gluesql/gluesql/test-suite/src/tester.rs:143:35:
called `Result::unwrap()` on an `Err` value: Value(FailedToParseHexString("[3.0, 4.0]"))

---- vector_functions_with_tables stdout ----
fs::remove_file Os { code: 2, kind: NotFound, message: "No such file or directory" }
[RUN] CREATE TABLE embeddings (id INTEGER, vec1 FLOAT_VECTOR, vec2 FLOAT_VECTOR)
[RUN] INSERT INTO embeddings VALUES (1, '[1.0, 0.0]', '[0.0, 1.0]'), (2, '[1.0, 1.0]', '[1.0, 1.0]')

thread 'vector_functions_with_tables' panicked at /home/runner/work/gluesql/gluesql/test-suite/src/tester.rs:143:35:
called `Result::unwrap()` on an `Err` value: Value(FailedToParseHexString("[1.0, 0.0]"))


failures:
    float_vector
    float_vector_data_type_validation
    float_vector_json_serialization
    vector_functions_advanced_with_tables
    vector_functions_basic
    vector_functions_with_tables

test result: FAILED. 191 passed; 6 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.69s

error: test failed, to rerun pass `--test parquet_storage`
/home/runner/work/_temp/aa77466f-697e-4f49-a19e-53f3fc698a87.sh: line 8: cd: file-storage: No such file or directory
/home/runner/work/_temp/aa77466f-697e-4f49-a19e-53f3fc698a87.sh: line 9: cd: redb-storage: No such file or directory
/home/runner/work/_temp/aa77466f-697e-4f49-a19e-53f3fc698a87.sh: line 10: cd: sled-storage: No such file or directory
```

Could you help me to fix this issue?
