## 🚴 MongoStorage - Mongo storage support for GlueSQL

### ⚙️ Prerequisites

#### Start mongodb by docker

```
docker run --name mongo-glue -d -p 27017:27017 mongo
```

### Test with features

```
cargo test --features test-mongo
```
