# Testing

Tests are maintained code and executable documentation. **Both insufficient
and excessive testing are problems.** Missing tests leave required behavior
unchecked; unnecessary tests add maintenance work without verifying a distinct
behavior, condition, or responsibility. Add cases that fill gaps and remove
cases that add no verification value.

A reader should be able to understand what a test verifies without the original
discussion or change history. Review the maintenance cost of test setup,
helpers, fault injection, and production code changes made to support them.

One scenario can cover several branches. Different behaviors may need separate
examples even when they use the same branch.

## Unit tests

- **Test every reachable branch and check transformation results.**

    Start with the behavior and expected result, then inspect the implementation
    for missing cases. Use inputs that reach each branch, and assert the returned
    value or state change. A test that fails before reaching the target code
    does not test that code's error handling.

- **Test the caller's own behavior when it calls another function.**

    Check how the caller uses return values and handles errors. Test the called
    function's internal algorithms and branches in the module that implements it.
    Do not repeat a shared function's case list in every caller.

    Passing tests for the called function do not verify the caller. Run the caller
    and assert its result or state change.

    For a helper that only forwards a call or wraps its result, check the result
    through the caller. Do not add a separate helper test that repeats the same
    assertion.

- **Do not put integration test scenarios in unit tests.**

    A unit test for a sorting function can pass `[3, 1, 2]` to the function and
    assert that the result is `[1, 2, 3]`.

    Creating a table, inserting those values, and checking the result of
    `SELECT ... ORDER BY` tests SQL execution together with data storage and
    retrieval. Put that scenario in the shared SQL fixtures in `test-suite`,
    not in the sorting function's unit tests.

- **Write inputs and expected results that show the behavior being tested.**

    Long AST or plan construction code can obscure the transformation being
    tested. In that case, write expected results as SQL and let a helper parse
    them for comparison.

    This example uses SQL to show that the query builder should combine two
    filters with `AND`:

    ```rust
    let actual = table("Items")
        .select()
        .filter("n > 10")
        .filter("n < 20");
    let expected = "SELECT * FROM Items WHERE n > 10 AND n < 20";
    test_query_builder(actual, expected);
    ```

    The helper compares the builder's output with the parsed expected SQL as ASTs
    and plans. Parsing makes the expected result easier to read; the test checks
    the builder's filter combination. Do not add parser syntax cases to this test.

    Other modules may prepare inputs or express expected results. Using them for
    those tasks does not make their behavior part of this test's scope.

- **Keep unit tests in the module that contains the implementation.**

    Write them in an inline `#[cfg(test)] mod tests`. Do not put them in a separate
    `tests.rs` file or a module that only re-exports the implementation. Move the
    tests when you move the implementation. Do not widen production visibility
    just to let tests access private code.

## SQL fixtures

Group the SQL fixtures in `test-suite/fixtures` by feature and behavior. Use
representative examples that show successful usage and expected errors. Give
each case a clear name, and keep its setup, SQL, and expected results easy to
read.

The fixtures also serve a unit-testing role for the execution layer.
For each behavior covered by a fixture, test every executor branch and error
path that SQL can reach. Run the cases through the production SQL pipeline and
assert their results. When a case tests storage behavior, also check the stored
state. Readable examples and branch coverage are both required.

Constructing an AST or plan directly does not prove that SQL can reach the same
behavior. For guards that require a manually constructed plan or inconsistent
storage, use the setup and assertions in [Error prefixes](errors.md#error-prefixes).

## Storage integration tests

Put behavior that storage implementations must share in `test-suite`. Use SQL
fixtures for shared SQL behavior, including index updates and transaction
commit or rollback. Keep these cases in the shared suite; storage crates
register and run them.

Put behavior that can only be verified for a particular storage implementation
in that storage crate's `tests/` directory. For example, transaction isolation
levels can differ between storages. Test concurrent transaction visibility
against that storage's isolation guarantees in its integration tests. Do not
put one storage's expected visibility in the shared suite as a requirement
for all storages.

Tests that reopen a storage's database files, migrate its on-disk format, or
inspect its internal data structures also belong in that storage's tests.

Apply the same placement rule to errors: shared failure conditions belong in
`test-suite`, and storage-specific failures belong in that storage's tests.
Call the operation that should fail and assert its error. Constructing an
internal error and calling its conversion helper does not verify what the
operation returns.

## Expected results

Assert the expected result:

- For a plan transformation, check the expected plan structure.
- For an error, check the variant and its relevant fields.
- For formatting, compare the exact output.

Use success or failure checks alone only when no result value or structure
needs to be verified.

Write expected values explicitly. Do not copy or modify the actual output to
create them. Do not calculate them with the code being tested.

A helper may convert an expected value to the type used by the assertion.
For example, a planner test may parse and translate expected SQL into a plan.
Do not run the planner being tested to build the expected plan.

## Inputs and helpers

Keep the inputs and expected results visible in each test case. Also show the
setup that determines the behavior being tested.

Extract repeated parsing, calls, or assertions when a helper makes the tests
easier to understand and change. Keeping a short sequence inline can make a
case easier to follow. Do not hide choices that the test checks inside a helper,
such as variants, IDs, defaults, or traversal rules.
