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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public interface CacheMap<K, V> {

  static <K, V> CacheMap<K, V> defaultCache() {
    return new DefaultCacheMap<>();
  }

  Optional<V> get(K key);

  default boolean containsKey(K key) {
    return get(key).isPresent();
  }

  CacheMap<K, V> set(K key, V value);

  CacheMap<K, V> delete(K key);

  CacheMap<K, V> clear();

  class DefaultCacheMap<K, V> implements CacheMap<K, V> {

    private final Map<K, V> cache;

    DefaultCacheMap() {
      cache = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<V> get(K key) {
      return Optional.ofNullable(cache.get(key));
    }

    @Override
    public CacheMap<K, V> set(K key, V value) {
      cache.put(key, value);
      return this;
    }

    @Override
    public CacheMap<K, V> delete(K key) {
      cache.remove(key);
      return this;
    }

    @Override
    public CacheMap<K, V> clear() {
      cache.clear();
      return this;
    }

    @Override
    public String toString() {
      return "DefaultCacheMap{" + "cache=" + cache + '}';
    }
  }
}
