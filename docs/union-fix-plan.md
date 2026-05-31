# UNION 구현 버그 수정 계획

현재 `feat/sql-union` 브랜치는 최소 기능 구현 단계입니다.
이 문서는 외부 DB(PostgreSQL, MySQL, SQLite)와의 동작 불일치 중 **현 단계에서 반드시 수정해야 할 항목**만 다룹니다.

---

## 수정 대상 이슈

| # | 우선순위 | 이슈 | 영향 |
|---|---------|------|------|
| 1 | 🔴 높음 | 컬럼 수 불일치를 실행 시점(execute)이 아닌 계획 시점(plan)에서 감지해야 함 | 불필요한 두 쿼리 실행 후 오류 |
| 2 | 🔴 높음 | 숫자 타입 간 암묵적 승격 부재 (`INT UNION FLOAT` 거부) | 표준 SQL 위반 |

그 외 INTERSECT/EXCEPT 미지원, 복잡한 표현식 타입 추론 등은 이후 단계에서 다룹니다.

---

## 이슈 1: 컬럼 수 불일치 감지 시점

### 현재 동작

```sql
SELECT a, b FROM t1 UNION SELECT x FROM t2
-- SelectError::UnionColumnCountMismatch 가 execute 단계에서 발생
```

`select_union()` 함수 내부에서 두 쿼리를 **모두 실행한 후** 컬럼 수를 비교합니다:

```rust
// core/src/executor/select.rs — select_union()
let (labels, left_stream) = select_with_labels(...).await?;
let (right_labels, right_stream) = select_with_labels(...).await?;

if labels.len() != right_labels.len() {              // ← 두 쿼리 실행 이후
    return Err(SelectError::UnionColumnCountMismatch { ... }.into());
}
```

### 기대 동작

PostgreSQL / MySQL / SQLite 모두 컬럼 수 불일치를 **parse/plan 단계**에서 거부합니다.

```
-- PostgreSQL
ERROR:  each UNION query must have the same number of columns
-- MySQL
ERROR 1222 (21000): The used SELECT statements have a different number of columns
```

### 수정 방법

`validate_set_expr()` 안에서 타입 벡터를 비교하기 **전에** 길이를 먼저 검사합니다.

**`core/src/plan/error.rs`** — 새 에러 variant 추가

```rust
#[error(
    "each UNION query must have the same number of columns: left has {left}, right has {right}"
)]
UnionColumnCountMismatch { left: usize, right: usize },
```

> `SelectError::UnionColumnCountMismatch` 는 execute 단계 전용으로 그대로 두되,
> plan 단계 오류는 `PlanError::UnionColumnCountMismatch` 로 분리합니다.

**`core/src/plan/union.rs`** — `validate_set_expr()` 수정

```rust
SetExpr::Union { left, right, .. } => {
    validate_set_expr(schema_map, left)?;
    validate_set_expr(schema_map, right)?;

    let left_types = infer_column_types(schema_map, left);
    let right_types = infer_column_types(schema_map, right);

    if let (Some(left_types), Some(right_types)) = (left_types, right_types) {
        // ① 컬럼 수 검사를 타입 비교보다 먼저 수행
        if left_types.len() != right_types.len() {
            return Err(PlanError::UnionColumnCountMismatch {
                left: left_types.len(),
                right: right_types.len(),
            });
        }

        for (index, (lt, rt)) in left_types.iter().zip(right_types.iter()).enumerate() {
            if let (Some(l), Some(r)) = (lt, rt) {
                if !types_compatible(l, r) {           // ← 이슈 2 에서 교체
                    return Err(PlanError::UnionColumnTypeMismatch {
                        index,
                        left: format!("{l}"),
                        right: format!("{r}"),
                    });
                }
            }
        }
    }

    Ok(())
}
```

### 주의 사항

`infer_column_types()` 가 `None` 을 반환하는 경우(한쪽이 `VALUES`, 스키마리스, 복잡한 서브쿼리 등)에는 정적 컬럼 수 확인이 불가합니다. 이 때는 기존처럼 execute 단계 fallback(`SelectError::UnionColumnCountMismatch`)이 동작합니다.

---

## 이슈 2: 숫자 타입 암묵적 승격 부재

### 현재 동작

```sql
SELECT 1 UNION SELECT 1.5
-- PlanError::UnionColumnTypeMismatch { index: 0, left: "INT", right: "FLOAT" }
```

`infer_literal_type()` 이 정수 리터럴 → `DataType::Int`, 소수점 리터럴 → `DataType::Float` 로 분류하고,
`validate_set_expr()` 은 `l != r` 이면 즉시 에러를 반환합니다.

### 기대 동작

주요 DB의 숫자 타입 승격 규칙:

| DB | 동작 |
|----|------|
| PostgreSQL | `INT` + `NUMERIC` → `NUMERIC` 으로 자동 승격, 쿼리 통과 |
| MySQL | `INT` + `FLOAT` → `FLOAT` 로 자동 승격 |
| SQLite | 동적 타입 — 항상 허용 |
| **GlueSQL (현재)** | 즉시 `PlanError` — **표준 위반** |

### 수정 방법

`core/src/plan/union.rs` 에 `types_compatible()` 함수를 추가하고,
`l != r` 직접 비교를 이 함수로 교체합니다.

```rust
/// 두 DataType 이 UNION 컬럼으로 함께 쓰일 수 있는지 판단합니다.
/// 동일한 타입이거나, 숫자 계열 내에서 승격 가능한 조합이면 true 를 반환합니다.
fn types_compatible(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    is_numeric_promotable(left, right)
}

/// INT ↔ FLOAT, INT ↔ DECIMAL, FLOAT ↔ DECIMAL 조합을 허용합니다.
fn is_numeric_promotable(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    matches!(
        (a, b),
        (Int | Int8 | Int16 | Int32 | Int128 | Uint8 | Uint16 | Uint32 | Uint64, Float | Decimal)
        | (Float | Decimal, Int | Int8 | Int16 | Int32 | Int128 | Uint8 | Uint16 | Uint32 | Uint64)
        | (Float, Decimal)
        | (Decimal, Float)
    )
}
```

그리고 `validate_set_expr()` 의 비교 조건을 교체합니다:

```rust
// 변경 전
if let (Some(l), Some(r)) = (lt, rt)
    && l != r
{
    return Err(PlanError::UnionColumnTypeMismatch { ... });
}

// 변경 후
if let (Some(l), Some(r)) = (lt, rt) {
    if !types_compatible(l, r) {
        return Err(PlanError::UnionColumnTypeMismatch { ... });
    }
}
```

### 실행 시점 동작 (execute)

Plan 통과 후 execute 단계에서는 두 컬럼의 실제 `Value` 가 혼재합니다. 예를 들어
`SELECT 1 UNION SELECT 1.5` 의 결과는 `Value::I64(1)` 과 `Value::F64(1.5)` 가 섞입니다.

이것은 현재 GlueSQL의 schemaless 모델에서는 자연스럽게 허용됩니다.
단, 이후에 실행 결과를 단일 타입으로 강제해야 하는 요구사항이 생기면 `select_union()` 에서
`Value::cast()` 를 사용해 오른쪽 값을 왼쪽 타입으로 변환하는 단계를 추가하면 됩니다.

---

## 변경 파일 요약

| 파일 | 변경 내용 |
|------|---------|
| `core/src/plan/error.rs` | `PlanError::UnionColumnCountMismatch { left, right }` 추가 |
| `core/src/plan/union.rs` | `validate_set_expr()` 에 컬럼 수 검사 추가; `types_compatible()` / `is_numeric_promotable()` 추가 |
| `test-suite/src/union.rs` | 두 이슈에 대한 테스트 케이스 추가 (아래 참고) |

---

## 추가할 테스트 케이스

`test-suite/src/union.rs` 에 아래 케이스를 추가합니다.

```rust
// 이슈 1: 컬럼 수 불일치 — plan 단계에서 감지
test!(
    plan_column_count_mismatch,
    "SELECT a, b FROM SchemaTable UNION SELECT a FROM SchemaTable",
    Err(PlanError::UnionColumnCountMismatch { left: 2, right: 1 })
);

// 이슈 2: 숫자 타입 승격 허용
test!(
    int_float_promotion,
    "SELECT 1 UNION SELECT 1.5",
    Ok(select!(
        "column1"
        I64 | F64;
        1;
        1.5   // FLOAT 리터럴은 F64로 처리
    ))
);
```

> `PlanError` 반환 방식은 기존 `test-suite/src/union.rs` 의 `type_mismatch` 테스트를 참고합니다.

---

## 관련 이슈 / 참고

- `core/src/plan/union.rs` — `validate_set_expr`, `infer_literal_type`
- `core/src/executor/select.rs` — `select_union`, `SelectError::UnionColumnCountMismatch`
- `core/src/plan/error.rs` — `PlanError`
- PostgreSQL type resolution for `UNION`: https://www.postgresql.org/docs/current/typeconv-union-case.html
