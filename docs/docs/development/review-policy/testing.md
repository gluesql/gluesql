# Testing

## Unit tests

Test every reachable branch of the implementation. Use inputs that reach each
branch, and assert its result or state change. A test that fails earlier in the
pipeline does not cover that branch.

Put unit tests in an inline `#[cfg(test)] mod tests` in the module that contains
the implementation. Do not put them in a separate `tests.rs` file or a module
that only re-exports the implementation.

Move the tests when you move the implementation. Do not widen production
visibility just to let tests access private code.

Test the branches of shared code in its own module. Caller tests cover the
caller's branches, including how it uses results and handles errors from shared
code.

For a planner pass, prepare the inputs it needs and call the pass directly.
Use separate test cases to check that the full planner pipeline runs the pass.

Test an execution entry point's own branches in its unit tests.

## SQL fixtures

The SQL fixtures in `test-suite/fixtures` are executable documentation for
people and tools. Group cases by SQL feature and behavior. Use representative
examples that show successful usage and expected errors. Give each case a clear
name, and keep its setup, SQL, and expected results easy to read.

The fixtures also serve a unit-testing role for the execution layer.
For each behavior covered by a fixture, test every executor branch and error
path that SQL can reach. Run the cases through the production SQL pipeline and
assert their results. When a case tests storage behavior, also check the stored
state. Readable examples and branch coverage are both required.

One scenario can cover several branches. Different SQL behaviors may need
separate examples even when they use the same branch.

Constructing an AST or plan directly does not prove that SQL can reach the same
behavior. For guards that require a manually constructed plan or inconsistent
storage, use the setup and assertions in [Error prefixes](errors.md#error-prefixes).

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

Use helpers for repeated parsing, calls, and assertions. Do not hide choices
that the test checks inside a helper, such as variants, IDs, defaults, or
traversal rules.
