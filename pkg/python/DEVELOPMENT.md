## 🚴 Usage

### Setup
Uncomment `pkg/python` lines in `workspace` of `Cargo.toml`
```
# enable this only for gluesql-py build
[workspace]
members = [
	...
	"pkg/python",
	...
]
default-members = [
	...
	"pkg/python",
	...
]
```

### Build
```
cd pkg/python
maturin build
```

### Test
```
```