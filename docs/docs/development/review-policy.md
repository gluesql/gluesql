# Review Policy

Apply this policy to new and modified code and tests in GlueSQL. The linked
topic documents are part of this policy; read and apply the rules relevant to
the change. Keep unrelated migrations of existing code in separate changes.

| Topic | Rules |
| --- | --- |
| [Errors](review-policy/errors.md) | Error ownership, variants, prefixes, and error-path testing. |
| [Testing](review-policy/testing.md) | Test responsibilities and maintenance, unit tests, SQL fixtures, storage integration tests, assertions, and helpers. |
| [Code Structure](review-policy/code-structure.md) | Guidance on design and declaration order, plus minimum visibility. |

## Change scope

Include caller updates and refactoring required by the change. Put unrelated
renames, formatting changes, and helper rewrites in separate changes.
