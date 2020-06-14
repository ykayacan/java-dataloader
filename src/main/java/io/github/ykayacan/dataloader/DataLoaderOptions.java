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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DataLoaderOptions<K, V> {
  private final BatchScheduler batchScheduler;
  private final boolean batchingEnabled;
  private final int maxBatchSize;
  private final boolean cachingEnabled;
  private final CacheKey<K> cacheKey;
  private final CacheMap<K, CompletableFuture<V>> cacheMap;

  private DataLoaderOptions(Builder<K, V> builder) {
    this.batchScheduler = getValidBatchScheduler(builder);
    this.batchingEnabled = builder.batchingEnabled;
    this.maxBatchSize = getValidMaxBatchSize(builder);
    this.cachingEnabled = builder.cachingEnabled;
    this.cacheKey = getValidCacheKey(builder);
    this.cacheMap = getValidCacheMap(builder);
  }

  public static <K, V> DataLoaderOptions<K, V> getDefaultInstance() {
    return DataLoaderOptions.<K, V>newBuilder().build();
  }

  public static <K, V> Builder<K, V> newBuilder() {
    return new Builder<>();
  }

  public BatchScheduler getBatchScheduler() {
    return batchScheduler;
  }

  public boolean isBatchingEnabled() {
    return batchingEnabled;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public boolean isCachingEnabled() {
    return cachingEnabled;
  }

  public CacheKey<K> getCacheKey() {
    return cacheKey;
  }

  public CacheMap<K, CompletableFuture<V>> getCacheMap() {
    return cacheMap;
  }

  private BatchScheduler getValidBatchScheduler(Builder<K, V> builder) {
    return builder.batchScheduler == null
        ? BatchScheduler.defaultScheduler()
        : builder.batchScheduler;
  }

  private int getValidMaxBatchSize(Builder<K, V> builder) {
    if (!builder.batchingEnabled) {
      return 1;
    }

    if (builder.maxBatchSize == 0) {
      return Integer.MAX_VALUE;
    }

    if (builder.maxBatchSize < 1) {
      throw new IllegalArgumentException("maxBatchSize must be a positive number");
    }

    return builder.maxBatchSize;
  }

  private CacheKey<K> getValidCacheKey(Builder<K, V> builder) {
    return builder.cacheKey == null ? CacheKey.identity() : builder.cacheKey;
  }

  private CacheMap<K, CompletableFuture<V>> getValidCacheMap(Builder<K, V> builder) {
    return !builder.cachingEnabled ? null : builder.cacheMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataLoaderOptions<?, ?> that = (DataLoaderOptions<?, ?>) o;
    return batchingEnabled == that.batchingEnabled
        && maxBatchSize == that.maxBatchSize
        && cachingEnabled == that.cachingEnabled
        && Objects.equals(batchScheduler, that.batchScheduler)
        && Objects.equals(cacheKey, that.cacheKey)
        && Objects.equals(cacheMap, that.cacheMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        batchScheduler, batchingEnabled, maxBatchSize, cachingEnabled, cacheKey, cacheMap);
  }

  @Override
  public String toString() {
    return "DataLoaderOptions{"
        + "batchScheduler="
        + batchScheduler
        + ", batchingEnabled="
        + batchingEnabled
        + ", maxBatchSize="
        + maxBatchSize
        + ", cachingEnabled="
        + cachingEnabled
        + ", cacheKey="
        + cacheKey
        + ", cacheMap="
        + cacheMap
        + '}';
  }

  public static final class Builder<K, V> {
    private BatchScheduler batchScheduler;
    private boolean batchingEnabled = true;
    private int maxBatchSize;
    private boolean cachingEnabled = true;
    private CacheKey<K> cacheKey;
    private CacheMap<K, CompletableFuture<V>> cacheMap = CacheMap.defaultCache();

    public Builder<K, V> batchScheduler(BatchScheduler batchScheduler) {
      this.batchScheduler = batchScheduler;
      return this;
    }

    public Builder<K, V> batchingEnabled(boolean enabled) {
      this.batchingEnabled = enabled;
      return this;
    }

    public Builder<K, V> maxBatchSize(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public Builder<K, V> cachingEnabled(boolean enabled) {
      this.cachingEnabled = enabled;
      return this;
    }

    public Builder<K, V> cacheKey(CacheKey<K> cacheKey) {
      this.cacheKey = cacheKey;
      return this;
    }

    public Builder<K, V> cacheMap(CacheMap<K, CompletableFuture<V>> cacheMap) {
      this.cacheMap = cacheMap;
      return this;
    }

    public DataLoaderOptions<K, V> build() {
      return new DataLoaderOptions<>(this);
    }
  }
}
