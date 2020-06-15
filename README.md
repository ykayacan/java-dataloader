# Java DataLoader

![Publish Snapshot](https://github.com/ykayacan/java-dataloader/workflows/Publish%20Snapshot/badge.svg?branch=master)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/io.github.ykayacan/java-dataloader?server=https%3A%2F%2Foss.sonatype.org)

This small library is a Java 11 port of [GraphQL DataLoader](https://github.com/graphql/dataloader).

DataLoader is a generic utility to be used as part of your application's data fetching layer to provide 
a consistent API over various backends and reduce requests to those backends via batching and caching.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Java 11

## Installation

#### Gradle

```groovy
dependencies {
  implementation 'io.github.ykayacan:java-dataloader:LATEST_VERSION'
}
```

#### Maven

```xml
<dependencies>
  <dependency>
    <groupId>io.github.ykayacan</groupId>
    <artifactId>java-dataloader</artifactId>
    <version>LATEST_VERSION</version>
  </dependency>
</dependencies>
```

#### Building
To build from source use the Gradle wrapper:

```bash
./gradlew clean build
```

### Snapshots

You can access the latest snapshot by adding the repository `https://oss.sonatype.org/content/repositories/snapshots` 
to your build.

Snapshots of the development version are available in [Sonatype's snapshots repository](https://oss.sonatype.org/content/repositories/snapshots/io/github/ykayacan/java-dataloader).

## Usage

To get started, create a `DataLoader`. Each `DataLoader` instance represents a unique cache.
Typically, instances created per request when used within a web-server.

### Batching

Batching is DataLoader's primary feature. Create loaders by providing a `BatchLoader`.

```java
BatchLoader<Long, Post> batchLoader = new BatchLoader<Long, Post>() {
    @Override
    public CompletionStage<List<Post>> load(List<Long> keys) {
        return CompletableFuture.supplyAsync(() -> postRepository.loadByIds(keys));
    }
};

DataLoader<Long, Post> dataLoader = DataLoader.create(batchLoader);
```

or shorter way

```java
DataLoader<Long, Post> dataLoader = 
    DataLoader.create(keys -> CompletableFuture.supplyAsync(() -> postRepository.loadByIds(keys)));
```

`BatchLoader` accepts a list of keys, and returns a `CompletionStage` which resolves to a list of values.

Then load individual values from the loader. DataLoader will coalesce all individual loads which occur within 
a single frame of execution (frame until calling `DataLoader.dispatch()`) and then call your `BatchLoader` 
with all requested keys. 

``` java
dataLoader.load(1)
    .thenAccept(user -> {
        System.out.println("user = " + user);
        dataLoader.load(user.getInvitedByID())
            .thenAccept(invitedBy -> {
                System.out.println("invitedBy = " + invitedBy);
            });
    });
    
// Elsewhere in your application
dataLoader.load(2)
    .thenAccept(user -> {
        System.out.println("user = " + user);
        dataLoader.load(user.getInvitedByID())
            .thenAccept(invitedBy -> {
                System.out.println("invitedBy = " + invitedBy);
            });
    });
    
dataLoader.dispatch();
```

A naive application may have issued four round-trips to a backend for the required information,
but with DataLoader this application will make at most two.

DataLoader allows you to decouple unrelated parts of your application without sacrificing the performance of 
batch data-loading. While the loader presents an API that loads individual values, all concurrent requests will be 
coalesced and presented to your batch loading function. This allows your application to safely distribute data 
fetching requirements throughout your application and maintain minimal outgoing data requests.

By default, batching is enabled and set size to 1. So, every dataLoader.load() operation issues a round-trip to backend.
You can override batchSize by providing a `DataLoaderOptions` to `DataLoader.create(..., options)`.

``` java
var options = DataLoaderOptions.<Long, Post>newBuilder()
    .maxBatchSize(2)
    .build();
DataLoader<Long, Post> dataLoader = DataLoader.create(batchLoader, options);

dataLoader.load(1);
dataLoader.load(2);
dataLoader.load(3);
dataLoader.load(4);
    
dataLoader.dispatch();

// Batched keys: [[1, 2], [3, 4]]
```

### Caching

DataLoader provides a memoization cache for all loads which occur in a single request to your application. 
After .load() is called once with a given key, the resulting value cached to eliminate redundant loads. 

``` java
var options = DataLoaderOptions.<Long, Post>newBuilder()
    .maxBatchSize(2)
    .build();
DataLoader<Long, Post> dataLoader = DataLoader.create(batchLoader, options);

dataLoader.load(1);
dataLoader.load(2);
dataLoader.load(1);
dataLoader.load(3);
    
dataLoader.dispatch();

// 1 is cached and never issued for a round-trip for the next loads
// Batched keys: [[1, 2], [3]]
```

By default, caching is enabled and use `ConcurrentHashMap` as in-memory cache under the hood. 
You can override default `CacheMap` by providing a custom `CacheMap` to `DataLoader.create(..., options)`.

``` java
class SimpleMap implements CacheMap<Long, CompletableFuture<Post>> {
    private Map<Long, CompletableFuture<Post>> stash = new LinkedHashMap<>();
    
    // omitted methods for clarity
}

var customCacheMap = new SimpleMap();

var options = DataLoaderOptions.<Long, Post>newBuilder()
    .maxBatchSize(2)
    .cacheMap(customCacheMap)
    .build();
DataLoader<Long, Post> dataLoader = DataLoader.create(batchLoader, options);
```

You can also provide a `CacheKey` to use your keys as complex types.

``` java
var customCacheKey = new CustomCacheKey();

var options = DataLoaderOptions.<Long, Post>newBuilder()
    .maxBatchSize(2)
    .cacheKey(customCacheKey)
    .build();
DataLoader<Long, Post> dataLoader = DataLoader.create(batchLoader, options);
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

```text
Copyright 2020 Yasin Sinan Kayacan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```