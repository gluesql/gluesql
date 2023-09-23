## 🚴 Usage

### Setup

Add `include-python-workspace` to the `features` of the `python/pkg/Cargo.toml` like below to build `gluesql-py`.

```
[features]
default = ["include-python-workspace"]
include-python-workspace = []
```

### Build

To build `gluesql-py`, run below command.

```
maturin build
```

### Test

To run `gluesql-py` tests, run below command.

```
pytest
```