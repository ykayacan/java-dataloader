/*
 * Copyright (c) 2020 Yasin Sinan Kayacan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.ykayacan.dataloader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A `DataLoader` creates a public API for loading data from a particular data back-end with unique
 * keys such as the `id` column of a SQL table or document name in a MongoDB database, given a batch
 * loading function.
 *
 * <p>Each `DataLoader` instance contains a unique memoized cache. Use caution when used in
 * long-lived applications or those which serve many users with different access permissions and
 * consider creating a new instance per web request.
 *
 * @param <K> the key
 * @param <V> the value
 */
public class DefaultDataLoader<K, V> implements DataLoader<K, V> {

  private final BatchLoader<K, V> batchLoader;
  private final int maxBatchSize;
  private final BatchScheduler batchScheduler;
  private final CacheKey<K> cacheKey;
  private final CacheMap<Object, CompletableFuture<V>> cacheMap;
  private int batchCount;
  private Batch<K, V> existingBatch;

  @SuppressWarnings("unchecked")
  DefaultDataLoader(BatchLoader<K, V> batchLoader, DataLoaderOptions<K, V> options) {
    Assert.checkNotNull(batchLoader);

    this.batchLoader = batchLoader;
    this.maxBatchSize = options.getMaxBatchSize();
    this.batchScheduler = options.getBatchScheduler();
    this.cacheKey = options.getCacheKey();
    this.cacheMap = (CacheMap<Object, CompletableFuture<V>>) options.getCacheMap();
    this.batchCount = 0;
    this.existingBatch = null;
  }

  @Override
  public CompletableFuture<V> load(K key) {
    Assert.checkNotNull(key);

    synchronized (this) {
      var currentBatch = getCurrentBatch();
      Object mappedKey = cacheKey.getKey(key);

      // If caching and there is a cache-hit, return cached future.
      if (cacheMap != null) {
        var cachedFutureOpt = cacheMap.get(mappedKey);
        if (cachedFutureOpt.isPresent()) {
          var cachedFuture = cachedFutureOpt.get();
          CompletableFuture<V> future = new CompletableFuture<>();
          existingBatch.getCacheHits().add(() -> future.complete(cachedFuture.join()));
          return future;
        }
      }

      // Otherwise, produce a new future for this key, and enqueue it to be
      // dispatched along with the current batch.
      var future = new CompletableFuture<V>();
      currentBatch.add(key, future);

      // If caching, cache this future.
      if (cacheMap != null) {
        cacheMap.set(mappedKey, future);
      }

      return future;
    }
  }

  @Override
  public CompletableFuture<List<V>> loadMany(List<K> keys) {
    Assert.checkNotNull(keys);

    return CompletableFutures.allOf(
        keys.stream().map(this::load).collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public DefaultDataLoader<K, V> clear(K key) {
    Assert.checkNotNull(key);

    if (cacheMap != null) {
      Object mappedKey = cacheKey.getKey(key);
      synchronized (this) {
        cacheMap.delete(mappedKey);
      }
    }
    return this;
  }

  @Override
  public DefaultDataLoader<K, V> clearAll() {
    if (cacheMap != null) {
      synchronized (this) {
        cacheMap.clear();
      }
    }
    return this;
  }

  @Override
  public DefaultDataLoader<K, V> prime(K key, V value) {
    Assert.checkNotNull(key);
    Assert.checkNotNull(value);

    if (cacheMap != null) {
      Object mappedKey = cacheKey.getKey(key);
      synchronized (this) {
        if (!cacheMap.containsKey(mappedKey)) {
          cacheMap.set(mappedKey, CompletableFuture.completedFuture(value));
        }
      }
    }
    return this;
  }

  @Override
  public DefaultDataLoader<K, V> prime(K key, Exception error) {
    Assert.checkNotNull(key);
    Assert.checkNotNull(error);

    if (cacheMap != null) {
      Object mappedKey = cacheKey.getKey(key);
      synchronized (this) {
        if (!cacheMap.containsKey(mappedKey)) {
          cacheMap.set(mappedKey, CompletableFuture.failedFuture(error));
        }
      }
    }
    return this;
  }

  @Override
  public void dispatch() {
    synchronized (this) {
      var dispatch = batchScheduler.dispatch();
      // Workaround for race condition for dispatch
      // Schedule new dispatch after previous one.
      while (batchCount != 0) {
        dispatch.thenCompose(v -> batchScheduler.dispatch());
        batchCount--;
      }
      //dispatch.join();
    }
  }

  // Either returns the current batch, or creates and schedules a
  // dispatch of a new batch for the given loader.
  private Batch<K, V> getCurrentBatch() {
    // If there is an existing batch which has not yet dispatched and is within
    // the limit of the batch size, then return it.
    if (existingBatch != null
        && !existingBatch.isDispatched()
        && existingBatch.getKeys().size() < maxBatchSize
        && existingBatch.getCacheHits().size() < maxBatchSize) {
      return existingBatch;
    }

    // Otherwise, create a new batch for this loader.
    var newBatch = Batch.<K, V>create();
    // Store it on the loader so it may be reused.
    this.existingBatch = newBatch;
    // Then schedule a task to dispatch this batch of requests.
    batchCount++;
    var finishedCallback = new CompletableFuture<Void>();
    batchScheduler.schedule(() -> dispatchBatch(newBatch, finishedCallback), finishedCallback);

    return newBatch;
  }

  private void dispatchBatch(Batch<K, V> batch, CompletableFuture<Void> finishedCallback) {
    // Mark this batch as having been dispatched.
    batch.dispatched();

    List<K> keys = batch.getKeys();

    // If there's nothing to load, resolve any cache hits and return early.
    if (keys.isEmpty()) {
      batch.resolveCacheHits();
      return;
    }

    batchLoader
        .load(keys)
        .thenAccept(
            values -> {
              Assert.checkState(
                  values.size() == keys.size(),
                  "The size of the future values must be the same size as the key list");

              // Step through values, resolving each future in the batch.
              var callbacks = batch.getCallbacks();
              for (int i = 0; i < callbacks.size(); i++) {
                callbacks.get(i).complete(values.get(i));
              }

              batch.resolveCacheHits();

              finishedCallback.complete(null);
            })
        .exceptionally(
            throwable -> {
              failedDispatch(batch, throwable);
              return null;
            });
  }

  // Do not cache individual loads if the entire batch dispatch fails,
  // but still reject each request so they do not hang.
  private void failedDispatch(Batch<K, V> batch, Throwable throwable) {
    // Cache hits are resolved, even though the batch failed.
    batch.resolveCacheHits();

    var keys = batch.getKeys();
    var callbacks = batch.getCallbacks();
    int size = keys.size();
    for (int i = 0; i < size; i++) {
      clear(keys.get(i));
      callbacks.get(i).completeExceptionally(throwable);
    }
  }
}
