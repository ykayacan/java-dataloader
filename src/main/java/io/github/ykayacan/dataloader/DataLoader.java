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

public interface DataLoader<K, V> {

  /**
   * Create default data loader.
   *
   * @param <K>         the key
   * @param <V>         the value
   * @param batchLoader the batch loader
   * @return the default data loader
   */
  static <K, V> DataLoader<K, V> create(BatchLoader<K, V> batchLoader) {
    return create(batchLoader, null);
  }

  /**
   * Create default data loader.
   *
   * @param <K>         the key
   * @param <V>         the value
   * @param batchLoader the batch loader
   * @param options     the options
   * @return the default data loader
   */
  static <K, V> DataLoader<K, V> create(
      BatchLoader<K, V> batchLoader, DataLoaderOptions<K, V> options) {
    Assert.checkNotNull(batchLoader);

    return new DefaultDataLoader<>(
        batchLoader, options == null ? DataLoaderOptions.getDefaultInstance() : options);
  }

  /**
   * Loads a key, returning a `CompletableFuture` for the value represented by that key.
   *
   * @param key the key
   * @return the completable future
   */
  CompletableFuture<V> load(K key);

  /**
   * Loads multiple keys, returning a `CompletableFuture` for the array of values:
   *
   * @param keys the keys
   * @return the completable future
   */
  default CompletableFuture<List<V>> loadMany(K... keys) {
    return loadMany(List.of(keys));
  }

  /**
   * Loads multiple keys, returning a `CompletableFuture` for the array of values:
   *
   * @param keys the keys
   * @return the completable future
   */
  CompletableFuture<List<V>> loadMany(List<K> keys);

  /**
   * Clears the value at `key` from the cache, if it exists. Returns itself for method chaining.
   *
   * @param key the key
   * @return the default data loader
   */
  DefaultDataLoader<K, V> clear(K key);

  /**
   * Clears the entire cache. To be used when some event results in unknown invalidations across
   * this particular `DataLoader`. Returns itself for method chaining.
   *
   * @return the default data loader
   */
  DefaultDataLoader<K, V> clearAll();

  /**
   * Adds the provided key and value to the cache. If the key already exists, no change is made.
   * Returns itself for method chaining.
   *
   * <p>To prime the cache with an error at a key, provide an Try instance.
   *
   * @param key   the key
   * @param value the value
   * @return the default data loader
   */
  DefaultDataLoader<K, V> prime(K key, V value);

  DefaultDataLoader<K, V> prime(K key, Exception error);

  void dispatch();
}
