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

class DataLoaderTestHelper {

  private DataLoaderTestHelper() {
    throw new AssertionError();
  }

  static <K, V> DataLoader<K, V> idLoader() {
    return idLoader(null, null);
  }

  static <K, V> DataLoader<K, V> idLoader(DataLoaderOptions<K, V> options) {
    return idLoader(options, null);
  }

  static <K, V> DataLoader<K, V> idLoader(List<List<K>> loadCalls) {
    return idLoader(null, loadCalls);
  }

  static <K, V> DataLoader<K, V> idLoader(
      DataLoaderOptions<K, V> options, List<List<K>> loadCalls) {
    return DataLoader.create(
        keys -> {
          if (loadCalls != null) {
            loadCalls.add(List.copyOf(keys));
          }
          @SuppressWarnings("unchecked")
          List<V> values = keys.stream().map(k -> (V) k).collect(Collectors.toList());
          return CompletableFuture.completedFuture(values);
        },
        options);
  }

  static <K, V> DataLoader<K, Try<V>> idLoaderWithTry() {
    return idLoaderWithTry(null, null);
  }

  static <K, V> DataLoader<K, Try<V>> idLoaderWithTry(List<List<K>> loadCalls) {
    return idLoaderWithTry(null, loadCalls);
  }

  static <K, V> DataLoader<K, Try<V>> idLoaderWithTry(
      DataLoaderOptions<K, Try<V>> options, List<List<K>> loadCalls) {
    return DataLoader.create(
        keys -> {
          if (loadCalls != null) {
            loadCalls.add(List.copyOf(keys));
          }
          @SuppressWarnings("unchecked")
          var results =
              keys.stream()
                  .map(k -> (V) k)
                  .map(key -> key.equals("bad") ? Try.<V>failure("bad key") : Try.success(key))
                  .collect(Collectors.toUnmodifiableList());
          return CompletableFuture.completedFuture(results);
        },
        options);
  }

  static CacheKey<JsonObject> cacheKey() {
    return key ->
        key.stream()
            .map(entry -> entry.getKey() + ":" + entry.getValue())
            .sorted()
            .collect(Collectors.joining());
  }
}
