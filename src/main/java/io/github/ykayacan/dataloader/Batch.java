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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Describes a batch of requests.
 *
 * @param <K> the key
 * @param <V> the value
 */
final class Batch<K, V> {
  private final List<K> keys;
  private final List<CompletableFuture<V>> callbacks;
  private final List<Runnable> cacheHits;
  private boolean dispatched;

  private Batch(
      boolean dispatched,
      List<K> keys,
      List<CompletableFuture<V>> callbacks,
      List<Runnable> cacheHits) {
    this.keys = keys;
    this.callbacks = callbacks;
    this.cacheHits = cacheHits;
    this.dispatched = dispatched;
  }

  static <K, V> Batch<K, V> create() {
    return new Batch<>(false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }

  List<Runnable> getCacheHits() {
    return cacheHits;
  }

  List<K> getKeys() {
    return keys;
  }

  List<CompletableFuture<V>> getCallbacks() {
    return callbacks;
  }

  boolean isDispatched() {
    return dispatched;
  }

  void dispatched() {
    this.dispatched = true;
  }

  /**
   * Resolves the futures for any cache hits in this batch.
   */
  void resolveCacheHits() {
    cacheHits.forEach(Runnable::run);
  }

  void add(K key, CompletableFuture<V> future) {
    keys.add(key);
    callbacks.add(future);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Batch<?, ?> batch = (Batch<?, ?>) o;
    return dispatched == batch.dispatched
        && Objects.equals(keys, batch.keys)
        && Objects.equals(callbacks, batch.callbacks)
        && Objects.equals(cacheHits, batch.cacheHits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keys, callbacks, cacheHits, dispatched);
  }

  @Override
  public String toString() {
    return "Batch{"
        + "keys="
        + keys
        + ", callbacks="
        + callbacks
        + ", cacheHits="
        + cacheHits
        + ", dispatched="
        + dispatched
        + '}';
  }
}
