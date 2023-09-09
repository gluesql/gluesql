## 🚴 MongoStorage - Mongo storage support for GlueSQL

### ⚙️ Prerequisites

Install & start up MongoDB

#### 1. By Docker

##### 1-1) Install docker

https://docs.docker.com/engine/install/

##### 1-2) Start up MongoDB by docker

```
docker run --name mongo-glue -d -p 27017:27017 mongo
```

#### 2. By local installation

https://www.mongodb.com/docs/manual/installation/

### 🧪 Test with features

```
cargo test --features test-mongo
```
