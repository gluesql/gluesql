# FloatVector Test Coverage Recovery Plan

## Overview
The FloatVector implementation has introduced a 0.52% coverage decrease (from 97.85% to 97.34%) with 234 missing lines across 15+ files. This plan addresses the coverage gaps systematically to restore the project's high test coverage standards.

## Current Coverage Gaps Analysis

### Critical Files (Highest Priority)
1. **core/src/ast/function.rs** - 48 missing lines (0.00% coverage)
   - All 16 new vector function enum variants lack coverage
   - ToSql implementations for vector functions untested

2. **core/src/executor/evaluate/function.rs** - 65 missing lines (68.75% coverage)
   - Vector function evaluation logic missing edge case tests
   - Error handling paths untested for invalid inputs

3. **core/src/data/float_vector.rs** - 58 missing lines (83.38% coverage)  
   - Advanced distance metric error cases
   - Dimension validation edge cases
   - SIMD fallback paths

### Medium Priority Files
4. **core/src/data/value/convert.rs** - 25 missing lines (35.89% coverage)
5. **core/src/data/value.rs** - 10 missing lines (23.07% coverage)
6. **core/src/data/value/json.rs** - 7 missing lines (85.10% coverage)
7. **core/src/translate/function.rs** - 7 missing lines (93.57% coverage)

## Recovery Strategy

### Phase 1: Core AST Function Coverage (Priority 1) ✅ COMPLETED
**Target: Achieve 100% coverage for core/src/ast/function.rs**

- ✅ **Added AST function definitions** for all 16 vector functions in `core/src/ast_builder/expr/function.rs`
- ✅ **Implemented conversion logic** with comprehensive TryFrom implementations
- ✅ **Added convenience methods** to ExprNode for fluent API usage
- ✅ **Created comprehensive unit tests** covering all vector function variants
- ✅ **Added integration tests** in `test-suite/src/ast_builder/function/vector.rs`
- ✅ **Integrated tests** into main test library with proper macro entries

**Completion Status**: All AST-level vector function support implemented with full test coverage.

### Phase 2: Vector Function Execution Coverage (Priority 1) ✅ COMPLETED
**Target: Achieve 90%+ coverage for core/src/executor/evaluate/function.rs**

- ✅ **Error case testing** for all 16 vector functions:
  - ✅ Invalid vector dimensions with proper error messages
  - ✅ NaN/Infinite input handling  
  - ✅ Type mismatch errors
  - ✅ Division by zero in normalization
- ✅ **Edge case testing**:
  - ✅ Empty vectors (returns proper error)
  - ✅ Single-element vectors
  - ✅ Large dimension vectors (100 elements tested)
  - ✅ Zero magnitude vectors
- ✅ **eval_to_float_vector helper function testing**:
  - ✅ String literal parsing failures
  - ✅ Invalid JSON format handling
  - ✅ Type conversion edge cases
- ✅ **NULL value handling** for all vector functions
- ✅ **Complex error scenarios** in WHERE and ORDER BY clauses

**Completion Status**: All vector function execution error paths now have comprehensive test coverage with 5 new test cases covering 65+ missing lines in executor/evaluate/function.rs.

### Phase 3: FloatVector Core Logic Coverage (Priority 2)
**Target: Achieve 95%+ coverage for core/src/data/float_vector.rs**

- **Advanced distance metrics edge cases**:
  - Minkowski distance with invalid p values
  - Jaccard similarity with zero vectors
  - Canberra distance with zero denominators
- **Vector validation edge cases**:
  - Vectors with mixed valid/invalid floats
  - Boundary dimension testing (1, 1024, 1025)
  - Performance stress testing paths

### Phase 4: Value Conversion Coverage (Priority 2)
**Target: Achieve 80%+ coverage for value conversion files**

- **core/src/data/value/convert.rs**:
  - TryFrom<&Value> error paths for FloatVector
  - Invalid JSON array conversion cases
  - Mixed type list conversion failures
- **core/src/data/value.rs**:
  - FloatVector casting edge cases
  - Hash implementation testing
  - Serialization/deserialization failures
- **core/src/data/value/json.rs**:
  - FloatVector JSON round-trip testing
  - Malformed JSON handling

### Phase 5: SQL Integration Coverage (Priority 3)
**Target: Achieve 95%+ coverage for SQL integration**

- **core/src/translate/function.rs**:
  - Vector function name parsing edge cases
  - Invalid function signature handling
- **core/src/plan/expr/function.rs**:
  - Vector function planning error cases
  - Type validation in query planning

### Phase 6: Storage Backend Coverage (Priority 3)
**Target: Achieve 90%+ coverage for storage integrations**

- **storages/mongo-storage/src/row/value.rs**:
  - BSON array conversion edge cases
  - MongoDB-specific serialization failures
- **Cross-storage compatibility testing**:
  - Error handling in different storage backends
  - Serialization format edge cases

## Implementation Strategy

### New Test Files to Create
1. **test-suite/src/ast/function_vector.rs** - AST-level vector function tests
2. **test-suite/src/executor/vector_error_cases.rs** - Error case testing
3. **test-suite/src/data/float_vector_edge_cases.rs** - Advanced edge case testing
4. **test-suite/src/integration/vector_cross_storage.rs** - Cross-storage testing

### Test Categories to Implement

#### 1. Unit Tests (Direct function testing)
- All vector mathematical operations with edge inputs
- Error condition validation
- Type conversion boundary testing

#### 2. Integration Tests (SQL-level testing)  
- Complex queries with vector functions
- Error messages and SQL syntax validation
- Performance regression testing

#### 3. Property-Based Tests (Fuzz testing)
- Random vector generation with property validation
- Mathematical property verification (e.g., dot product commutativity)
- Stress testing with large dimensions

#### 4. Cross-Storage Tests
- Vector data consistency across all storage backends
- Serialization/deserialization verification
- Error handling uniformity

## Success Criteria

### Coverage Targets
- **Overall project coverage**: Restore to 97.8%+ (current 97.34%)
- **Core vector files**: Achieve 90%+ coverage each
- **AST function file**: Achieve 100% coverage
- **Integration tests**: Cover all 16 vector functions with edge cases

### Quality Gates
- All new tests must pass in CI/CD
- No performance regression in existing functionality
- Maintain backward compatibility
- Zero new clippy warnings

## Timeline Estimate
- **Phase 1**: 2-3 hours (AST coverage) ✅ COMPLETED
- **Phase 2**: 4-5 hours (Function execution coverage) ✅ COMPLETED  
- **Phase 3**: 3-4 hours (Core logic coverage)
- **Phase 4**: 2-3 hours (Value conversion coverage)
- **Phase 5**: 1-2 hours (SQL integration coverage)
- **Phase 6**: 2-3 hours (Storage backend coverage)

**Total Estimated Time**: 14-20 hours
**Completed Time**: ~6-8 hours (Phases 1 & 2)

This plan will systematically address all coverage gaps while maintaining code quality and ensuring the FloatVector implementation is production-ready with comprehensive test coverage.

## Files with Missing Coverage Details

Based on Codecov analysis, here are the specific files that need attention:

| File | Missing Lines | Coverage % | Priority |
|------|---------------|------------|----------|
| core/src/executor/evaluate/function.rs | 65 | 68.75% | ✅ Completed |
| core/src/data/float_vector.rs | 58 | 83.38% | Critical |
| core/src/ast/function.rs | 48 | 0.00% | ✅ Completed |
| core/src/data/value/convert.rs | 25 | 35.89% | High |
| core/src/data/value.rs | 10 | 23.07% | High |
| core/src/data/value/json.rs | 7 | 85.10% | Medium |
| core/src/translate/function.rs | 7 | 93.57% | Medium |
| core/src/executor/evaluate.rs | 4 | 93.54% | Medium |
| storages/mongo-storage/src/row/value.rs | 3 | 85.71% | Low |
| core/src/plan/expr/function.rs | 2 | 93.93% | Low |

**Total Missing Lines**: 234
**Overall Coverage Impact**: -0.52% (97.85% → 97.34%)
**Completed Coverage**: ~113 lines (Phases 1 & 2)
**Remaining Coverage**: ~121 lines (Phases 3-6)