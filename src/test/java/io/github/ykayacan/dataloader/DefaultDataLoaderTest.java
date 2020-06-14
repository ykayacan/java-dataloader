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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DefaultDataLoaderTest {

  @DisplayName("Primary API")
  @Nested
  class PrimaryAPI {

    @Test
    void builds_a_really_really_simple_data_loader() {
      var identityLoader = DataLoaderTestHelper.<Integer, Integer>idLoader();

      var future = identityLoader.load(1);
      assertThat(future).isInstanceOf(CompletableFuture.class);

      identityLoader.dispatch();

      var value = future.join();
      assertThat(value).isEqualTo(1);
    }

    @Test
    void supports_loading_multiple_keys_in_one_call() {
      var identityLoader = DataLoaderTestHelper.<Integer, Integer>idLoader();

      var futureAll = identityLoader.loadMany(List.of(1, 2));
      assertThat(futureAll).isInstanceOf(CompletableFuture.class);

      identityLoader.dispatch();

      var values = futureAll.join();
      assertThat(values).containsSequence(1, 2);

      var futureEmpty = identityLoader.loadMany(List.of());
      assertThat(futureEmpty).isInstanceOf(CompletableFuture.class);

      identityLoader.dispatch();

      var empty = futureEmpty.join();
      assertThat(empty).isEmpty();
    }

    @Test
    void supports_loading_multiple_keys_in_one_call_with_errors() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoaderWithTry(loadCalls);

      var futureAll = identityLoader.loadMany(List.of("a", "b", "bad"));
      assertThat(futureAll).isInstanceOf(CompletableFuture.class);

      identityLoader.dispatch();

      var values = futureAll.join();
      assertThat(values)
          .containsSequence(Try.success("a"), Try.success("b"), Try.failure("bad key"));
    }

    @Test
    void batches_multiple_requests() {
      var loadCalls = new ArrayList<List<Integer>>();
      var identityLoader = DataLoaderTestHelper.<Integer, Integer>idLoader(loadCalls);

      var future1 = identityLoader.load(1);
      var future2 = identityLoader.load(2);

      identityLoader.dispatch();

      List<Integer> values = CompletableFutures.allOf(List.of(future1, future2)).join();
      assertThat(values).containsSequence(1, 2);
      assertThat(loadCalls).containsSequence(List.of(List.of(1, 2)));
    }

    @Test
    void batches_multiple_requests_with_max_batch_sizes() {
      var loadCalls = new ArrayList<List<Integer>>();
      var options = DataLoaderOptions.<Integer, Integer>newBuilder().maxBatchSize(2).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var future1 = identityLoader.load(1);
      var future2 = identityLoader.load(2);
      var future3 = identityLoader.load(3);

      identityLoader.dispatch();

      List<Integer> values = CompletableFutures.allOf(List.of(future1, future2, future3)).join();
      assertThat(values).containsSequence(1, 2, 3);
      assertThat(loadCalls).containsSequence(List.of(List.of(1, 2), List.of(3)));
    }

    @Test
    void batches_cached_requests() {
      var loadCalls = new ArrayList<List<Integer>>();
      AtomicReference<Runnable> resolveBatch = new AtomicReference<>();
      var identityLoader =
          DataLoader.<Integer, Integer>create(
              keys -> {
                loadCalls.add(keys);
                var future = new CompletableFuture<List<Integer>>();
                resolveBatch.set(() -> future.complete(keys));
                return future;
              });

      identityLoader.prime(1, 1);

      var future1 = identityLoader.load(1);
      var future2 = identityLoader.load(2);

      var future1Resolved = new AtomicBoolean(false);
      var future2Resolved = new AtomicBoolean(false);
      future1.thenRun(() -> future1Resolved.set(true));
      future2.thenRun(() -> future2Resolved.set(true));

      assertThat(future1Resolved).isFalse();
      assertThat(future2Resolved).isFalse();

      identityLoader.dispatch();
      resolveBatch.get().run();

      assertThat(future1Resolved).isTrue();
      assertThat(future2Resolved).isTrue();

      var values = CompletableFutures.allOf(List.of(future1, future2)).join();
      assertThat(values).containsSequence(1, 2);
      assertThat(loadCalls).containsSequence(List.of(List.of(2)));
    }

    @Test
    void max_batch_size_respects_cached_results() {
      var loadCalls = new ArrayList<List<Integer>>();
      var options = DataLoaderOptions.<Integer, Integer>newBuilder().maxBatchSize(1).build();
      AtomicReference<Runnable> resolveBatch = new AtomicReference<>();
      var identityLoader =
          DataLoader.create(
              keys -> {
                loadCalls.add(keys);
                var future = new CompletableFuture<List<Integer>>();
                resolveBatch.set(() -> future.complete(keys));
                return future;
              },
              options);

      identityLoader.prime(1, 1);

      var future1 = identityLoader.load(1);
      var future2 = identityLoader.load(2);

      var future1Resolved = new AtomicBoolean(false);
      var future2Resolved = new AtomicBoolean(false);
      future1.thenRun(() -> future1Resolved.set(true));
      future2.thenRun(() -> future2Resolved.set(true));

      identityLoader.dispatch();

      // Future 1 resolves first since max batch size is 1
      assertThat(future1Resolved).isTrue();
      assertThat(future2Resolved).isFalse();

      identityLoader.dispatch();
      resolveBatch.get().run();

      assertThat(future1Resolved).isTrue();
      assertThat(future2Resolved).isTrue();

      var values = CompletableFutures.allOf(List.of(future1, future2)).join();
      assertThat(values).containsSequence(1, 2);
      assertThat(loadCalls).containsSequence(List.of(List.of(2)));
    }

    @Test
    void coalesces_identical_requests() {
      var loadCalls = new ArrayList<List<Integer>>();
      var identityLoader = DataLoaderTestHelper.<Integer, Integer>idLoader(loadCalls);

      var future1a = identityLoader.load(1);
      var future1b = identityLoader.load(1);

      identityLoader.dispatch();

      List<Integer> values = CompletableFutures.allOf(List.of(future1a, future1b)).join();
      assertThat(values).containsSequence(1, 1);
      assertThat(loadCalls).containsSequence(List.of(List.of(1)));
    }

    @Test
    void coalesces_identical_requests_across_sized_batches() {
      var loadCalls = new ArrayList<List<Integer>>();
      var options = DataLoaderOptions.<Integer, Integer>newBuilder().maxBatchSize(2).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var future1a = identityLoader.load(1);
      var future2 = identityLoader.load(2);
      var future1b = identityLoader.load(1);
      var future3 = identityLoader.load(3);

      identityLoader.dispatch();

      List<Integer> values =
          CompletableFutures.allOf(List.of(future1a, future2, future1b, future3)).join();
      assertThat(values).containsSequence(1, 2, 1, 3);
      assertThat(loadCalls).containsSequence(List.of(List.of(1, 2), List.of(3)));
    }

    @Test
    void caches_repeated_requests() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      var futureA = identityLoader.load("A");
      var futureB = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA, futureB)).join())
          .containsSequence("A", "B");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B")));

      var futureA2 = identityLoader.load("A");
      var futureC = identityLoader.load("C");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA2, futureC)).join())
          .containsSequence("A", "C");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("C")));

      var futureA3 = identityLoader.load("A");
      var futureB2 = identityLoader.load("B");
      var futureC2 = identityLoader.load("C");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA3, futureB2, futureC2)).join())
          .containsSequence("A", "B", "C");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("C")));
    }

    @Test
    void clears_single_value_in_loader() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      var futureA = identityLoader.load("A");
      var futureB = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA, futureB)).join())
          .containsSequence("A", "B");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B")));

      identityLoader.clear("A");

      var futureA2 = identityLoader.load("A");
      var futureB2 = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA2, futureB2)).join())
          .containsSequence("A", "B");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("A")));
    }

    @Test
    void clear_all_values_in_loader() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      var futureA = identityLoader.load("A");
      var futureB = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA, futureB)).join())
          .containsSequence("A", "B");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B")));

      identityLoader.clearAll();

      var futureA2 = identityLoader.load("A");
      var futureB2 = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA2, futureB2)).join())
          .containsSequence("A", "B");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("A", "B")));
    }

    @Test
    void allows_priming_the_cache() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      identityLoader.prime("A", "A");

      var futureA = identityLoader.load("A");
      var futureB = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(CompletableFutures.allOf(List.of(futureA, futureB)).join())
          .containsSequence("A", "B");
      assertThat(loadCalls).containsSequence(List.of(List.of("B")));
    }

    @Test
    void does_not_prime_keys_that_already_exists() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      identityLoader.prime("A", "X");

      var futureA1 = identityLoader.load("A");
      var futureB1 = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(futureA1.join()).isEqualTo("X");
      assertThat(futureB1.join()).isEqualTo("B");

      identityLoader.prime("A", "Y");
      identityLoader.prime("B", "Y");

      var futureA2 = identityLoader.load("A");
      var futureB2 = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(futureA2.join()).isEqualTo("X");
      assertThat(futureB2.join()).isEqualTo("B");

      assertThat(loadCalls).containsSequence(List.of(List.of("B")));
    }

    @Test
    void allows_forcefully_priming_the_cache() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      identityLoader.prime("A", "X");

      var futureA1 = identityLoader.load("A");
      var futureB1 = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(futureA1.join()).isEqualTo("X");
      assertThat(futureB1.join()).isEqualTo("B");

      identityLoader.clear("A").prime("A", "Y");
      identityLoader.clear("B").prime("B", "Y");

      var futureA2 = identityLoader.load("A");
      var futureB2 = identityLoader.load("B");

      identityLoader.dispatch();

      assertThat(futureA2.join()).isEqualTo("Y");
      assertThat(futureB2.join()).isEqualTo("Y");

      assertThat(loadCalls).containsSequence(List.of(List.of("B")));
    }
  }

  @DisplayName("Represents Errors")
  @Nested
  class RepresentsErrors {

    @Test
    void resolves_to_error_to_indicate_failure() {
      var loadCalls = new ArrayList<List<Integer>>();
      var evenLoader =
          DataLoader.<Integer, Try<Integer>>create(
              keys -> {
                loadCalls.add(List.copyOf(keys));
                List<Try<Integer>> values =
                    keys.stream()
                        .map(
                            key ->
                                (key % 2 == 0)
                                    ? Try.success(key)
                                    : Try.<Integer>failure("Odd: " + key))
                        .collect(Collectors.toUnmodifiableList());
                return CompletableFuture.completedFuture(values);
              });

      var future1 = evenLoader.load(1);
      evenLoader.dispatch();
      assertThat(future1.join()).isEqualTo(Try.failure("Odd: 1"));

      var future2 = evenLoader.load(2);
      evenLoader.dispatch();
      assertThat(future2.join()).isEqualTo(Try.success(2));

      assertThat(loadCalls).containsSequence(List.of(List.of(1), List.of(2)));
    }

    @Test
    void can_represent_failures_and_success_simultaneously() {
      var loadCalls = new ArrayList<List<Integer>>();
      var errorLoader =
          DataLoader.<Integer, Try<Integer>>create(
              keys -> {
                loadCalls.add(List.copyOf(keys));
                List<Try<Integer>> values =
                    keys.stream()
                        .map(
                            key ->
                                (key % 2 == 0)
                                    ? Try.success(key)
                                    : Try.<Integer>failure("Odd: " + key))
                        .collect(Collectors.toUnmodifiableList());
                return CompletableFuture.completedFuture(values);
              });

      var future1 = errorLoader.load(1);
      var future2 = errorLoader.load(2);
      errorLoader.dispatch();

      assertThat(future1.join()).isEqualTo(Try.failure("Odd: 1"));
      assertThat(future2.join()).isEqualTo(Try.success(2));

      assertThat(loadCalls).containsSequence(List.of(List.of(1, 2)));
    }

    @Test
    void caches_failed_fetches() {
      var loadCalls = new ArrayList<List<Integer>>();
      var errorLoader =
          DataLoader.<Integer, Try<Integer>>create(
              keys -> {
                loadCalls.add(List.copyOf(keys));
                List<Try<Integer>> values =
                    keys.stream()
                        .map(key -> Try.<Integer>failure("Error: " + key))
                        .collect(Collectors.toUnmodifiableList());
                return CompletableFuture.completedFuture(values);
              });

      var futureA = errorLoader.load(1);
      errorLoader.dispatch();
      assertThat(futureA.join()).isEqualTo(Try.failure("Error: 1"));

      var futureB = errorLoader.load(1);
      errorLoader.dispatch();
      assertThat(futureB.join()).isEqualTo(Try.failure("Error: 1"));

      assertThat(loadCalls).containsSequence(List.of(List.of(1)));
    }

    @Test
    void handles_priming_cache_with_an_error() {
      var loadCalls = new ArrayList<List<Integer>>();
      var identityLoader = DataLoaderTestHelper.idLoaderWithTry(loadCalls);

      identityLoader.prime(1, Try.failure("Error: 1"));

      var futureA = identityLoader.load(1);
      identityLoader.dispatch();
      assertThat(futureA.join()).isEqualTo(Try.failure("Error: 1"));

      assertThat(loadCalls).containsSequence(List.of());
    }

    @Test
    void can_clear_values_from_cache_after_errors() {
      var loadCalls = new ArrayList<List<Integer>>();
      var errorLoader =
          DataLoader.<Integer, Try<Integer>>create(
              keys -> {
                loadCalls.add(List.copyOf(keys));
                List<Try<Integer>> values =
                    keys.stream()
                        .map(key -> Try.<Integer>failure("Error: " + key))
                        .collect(Collectors.toUnmodifiableList());
                return CompletableFuture.completedFuture(values);
              });

      var futureA =
          errorLoader
              .load(1)
              .thenApply(
                  value -> {
                    if (value.isFailure()) {
                      // Presumably determine if this error is transient, and only clear the
                      // cache in that case.
                      errorLoader.clear(1);
                    }
                    return value;
                  });
      errorLoader.dispatch();
      assertThat(futureA.join()).isEqualTo(Try.failure("Error: 1"));

      var futureB =
          errorLoader
              .load(1)
              .thenApply(
                  value -> {
                    if (value.isFailure()) {
                      // Again, only do this if you can determine the error is transient.
                      errorLoader.clear(1);
                    }
                    return value;
                  });
      errorLoader.dispatch();
      assertThat(futureB.join()).isEqualTo(Try.failure("Error: 1"));

      assertThat(loadCalls).containsSequence(List.of(List.of(1), List.of(1)));
    }

    @Test
    void propagates_error_to_all_loads() {
      var loadCalls = new ArrayList<List<Integer>>();
      var failLoader =
          DataLoader.<Integer, Try<Integer>>create(
              keys -> {
                loadCalls.add(List.copyOf(keys));
                List<Try<Integer>> values =
                    keys.stream()
                        .map(key -> Try.<Integer>failure("I am a terrible loader"))
                        .collect(Collectors.toUnmodifiableList());
                return CompletableFuture.completedFuture(values);
              });

      var future1 = failLoader.load(1);
      var future2 = failLoader.load(2);

      failLoader.dispatch();

      assertThat(future1.join()).isEqualTo(Try.failure("I am a terrible loader"));
      assertThat(future2.join()).isEqualTo(Try.failure("I am a terrible loader"));

      assertThat(loadCalls).containsSequence(List.of(List.of(1, 2)));
    }
  }

  @DisplayName("Accepts any kind of key")
  @Nested
  class AcceptsAnyKindsOfKey {

    @Test
    void accepts_objects_as_keys() {
      var loadCalls = new ArrayList<List<Object>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      var keyA = new Object();
      var keyB = new Object();

      // Fetches as expected

      var futureA = identityLoader.load(keyA);
      var futureB = identityLoader.load(keyB);
      identityLoader.dispatch();
      List<Object> values = CompletableFutures.allOf(List.of(futureA, futureB)).join();

      assertThat(values).containsSequence(keyA, keyB);

      assertThat(loadCalls).hasSize(1);
      assertThat(loadCalls.get(0)).hasSize(2);
      assertThat(loadCalls.get(0).get(0)).isEqualTo(keyA);
      assertThat(loadCalls.get(0).get(1)).isEqualTo(keyB);

      // Caching

      identityLoader.clear(keyA);

      var futureA2 = identityLoader.load(keyA);
      var futureB2 = identityLoader.load(keyB);
      identityLoader.dispatch();
      List<Object> values2 = CompletableFutures.allOf(List.of(futureA2, futureB2)).join();

      assertThat(values2).containsSequence(keyA, keyB);

      assertThat(loadCalls).hasSize(2);
      assertThat(loadCalls.get(1)).hasSize(1);
      assertThat(loadCalls.get(1).get(0)).isEqualTo(keyA);
    }
  }

  @DisplayName("Accepts options")
  @Nested
  class AcceptsOptions {

    @Test
    void may_disable_batching() {
      var loadCalls = new ArrayList<List<Integer>>();
      var options = DataLoaderOptions.<Integer, Integer>newBuilder().batchingEnabled(false).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var future1 = identityLoader.load(1);
      var future2 = identityLoader.load(2);

      identityLoader.dispatch();
      List<Integer> values = CompletableFutures.allOf(List.of(future1, future2)).join();
      assertThat(values).containsSequence(1, 2);

      assertThat(loadCalls).containsSequence(List.of(List.of(1), List.of(2)));
    }

    @Test
    void may_disable_caching() {
      var loadCalls = new ArrayList<List<String>>();
      var options = DataLoaderOptions.<String, String>newBuilder().cachingEnabled(false).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var futureA = identityLoader.load("A");
      var futureB = identityLoader.load("B");
      identityLoader.dispatch();

      assertThat(futureA.join()).isEqualTo("A");
      assertThat(futureB.join()).isEqualTo("B");

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B")));

      var futureA2 = identityLoader.load("A");
      var futureC = identityLoader.load("C");
      identityLoader.dispatch();

      assertThat(futureA2.join()).isEqualTo("A");
      assertThat(futureC.join()).isEqualTo("C");

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("A", "C")));

      var futureA3 = identityLoader.load("A");
      var futureB2 = identityLoader.load("B");
      var futureC2 = identityLoader.load("C");
      identityLoader.dispatch();

      assertThat(futureA3.join()).isEqualTo("A");
      assertThat(futureB2.join()).isEqualTo("B");
      assertThat(futureC2.join()).isEqualTo("C");

      assertThat(loadCalls)
          .containsSequence(List.of(List.of("A", "B"), List.of("A", "C"), List.of("A", "B", "C")));
    }

    @Test
    void keys_are_repeated_in_batch_when_cache_disabled() {
      var loadCalls = new ArrayList<List<String>>();
      var options = DataLoaderOptions.<String, String>newBuilder().cachingEnabled(false).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var future1 = identityLoader.load("A");
      var future2 = identityLoader.load("C");
      var future3 = identityLoader.load("D");
      var future4 = identityLoader.loadMany(List.of("C", "D", "A", "A", "B"));
      identityLoader.dispatch();

      assertThat(future1.join()).isEqualTo("A");
      assertThat(future2.join()).isEqualTo("C");
      assertThat(future3.join()).isEqualTo("D");
      assertThat(future4.join()).containsSequence("C", "D", "A", "A", "B");

      assertThat(loadCalls)
          .containsSequence(List.of(List.of("A", "C", "D", "C", "D", "A", "A", "B")));
    }

    @Test
    void cacheMap_may_be_set_to_null_to_disable_cache() {
      var loadCalls = new ArrayList<List<String>>();
      var options = DataLoaderOptions.<String, String>newBuilder().cacheMap(null).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      identityLoader.load("A");
      identityLoader.dispatch();
      identityLoader.load("A");
      identityLoader.dispatch();

      assertThat(loadCalls).containsSequence(List.of(List.of("A"), List.of("A")));
    }

    @Test
    void does_not_interact_with_a_cache_when_cache_is_disabled() {
      var futureX = CompletableFuture.completedFuture("X");
      var cacheMap = CacheMap.<String, CompletableFuture<String>>defaultCache();
      cacheMap.set("X", futureX);
      var options =
          DataLoaderOptions.<String, String>newBuilder()
              .cachingEnabled(false)
              .cacheMap(cacheMap)
              .build();
      var identityLoader = DataLoaderTestHelper.idLoader(options);

      identityLoader.prime("A", "A");
      assertThat(cacheMap.get("A")).isEmpty();
      identityLoader.clear("X");
      assertThat(cacheMap.get("X")).isEqualTo(Optional.of(futureX));
      identityLoader.clearAll();
      assertThat(cacheMap.get("X")).isEqualTo(Optional.of(futureX));
    }

    @Test
    void complex_cache_behavior_via_clearAll() {
      var loadCalls = new ArrayList<List<String>>();
      var clearRunnable = new AtomicReference<Runnable>();
      // This loader clears its cache as soon as a batch function is dispatched.
      var identityLoader =
          DataLoader.<String, String>create(
              keys -> {
                clearRunnable.get().run();
                loadCalls.add(keys);
                return CompletableFuture.completedFuture(keys);
              });

      clearRunnable.set(identityLoader::clearAll);

      var futures1 =
          List.of(identityLoader.load("A"), identityLoader.load("B"), identityLoader.load("A"));
      identityLoader.dispatch();
      var values1 = CompletableFutures.allOf(futures1).join();
      assertThat(values1).containsSequence("A", "B", "A");

      var futures2 =
          List.of(identityLoader.load("A"), identityLoader.load("B"), identityLoader.load("A"));
      identityLoader.dispatch();

      var values2 = CompletableFutures.allOf(futures2).join();
      assertThat(values2).containsSequence("A", "B", "A");

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("A", "B")));
    }
  }

  @DisplayName("Accepts custom cacheMap instance")
  @Nested
  class AcceptsCustomCacheMapInstance {

    @Test
    void accepts_a_custom_cache_map_implementation() {
      class SimpleMap implements CacheMap<String, CompletableFuture<String>> {

        private Map<String, CompletableFuture<String>> stash = new LinkedHashMap<>();

        @Override
        public Optional<CompletableFuture<String>> get(String key) {
          return Optional.ofNullable(stash.get(key));
        }

        @Override
        public CacheMap<String, CompletableFuture<String>> set(String key, CompletableFuture<String> value) {
          stash.put(key, value);
          return this;
        }

        @Override
        public CacheMap<String, CompletableFuture<String>> delete(String key) {
          stash.remove(key);
          return this;
        }

        @Override
        public CacheMap<String, CompletableFuture<String>> clear() {
          stash.clear();
          return this;
        }

        public Map<String, CompletableFuture<String>> getStash() {
          return stash;
        }
      }

      var aCustomMap = new SimpleMap();
      var loadCalls = new ArrayList<List<String>>();
      var options =
          DataLoaderOptions.<String, String>newBuilder().cacheMap(aCustomMap).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var futureA = identityLoader.load("A");
      var futureB1 = identityLoader.load("B");
      identityLoader.dispatch();

      assertThat(futureA.join()).isEqualTo("A");
      assertThat(futureB1.join()).isEqualTo("B");

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B")));
      assertThat(aCustomMap.getStash().keySet()).containsSequence("A", "B");

      var futureC = identityLoader.load("C");
      var futureB2 = identityLoader.load("B");
      identityLoader.dispatch();

      assertThat(futureC.join()).isEqualTo("C");
      assertThat(futureB2.join()).isEqualTo("B");

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("C")));
      assertThat(aCustomMap.getStash().keySet()).containsSequence("A", "B", "C");

      // Supports clear

      identityLoader.clear("B");
      var futureB3 = identityLoader.load("B");
      identityLoader.dispatch();

      assertThat(futureB3.join()).isEqualTo("B");
      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("C"), List.of("B")));
      assertThat(aCustomMap.getStash().keySet()).containsSequence("A", "C", "B");

      // Supports clear all
      identityLoader.clearAll();

      assertThat(aCustomMap.getStash()).isEmpty();
    }
  }

  @DisplayName("It allows custom schedulers")
  @Nested
  class ItAllowsCustomSchedulers {

    @Test
    void supports_manual_dispatch() {
      var scheduler =
          new BatchScheduler() {
            private final List<Runnable> callbacks = new ArrayList<>();
            private CompletableFuture<Void> finishedCallback;

            @Override
            public void schedule(Runnable runnable, CompletableFuture<Void> finishedCallback) {
              this.finishedCallback = finishedCallback;
              callbacks.add(runnable);
            }

            @Override
            public CompletableFuture<Void> dispatch() {
              callbacks.forEach(Runnable::run);
              callbacks.clear();
              return finishedCallback;
            }
          };

      var loadCalls = new ArrayList<List<String>>();
      var options =
          DataLoaderOptions.<String, String>newBuilder().batchScheduler(scheduler).build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      identityLoader.load("A");
      identityLoader.load("B");
      identityLoader.dispatch();
      identityLoader.load("A");
      identityLoader.load("C");
      identityLoader.dispatch();
      // Note: never dispatched!
      identityLoader.load("D");

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B"), List.of("C")));
    }
  }

  @DisplayName("Accepts object key in custom cacheKey")
  @Nested
  class AcceptsObjectKeyInCustomCacheKey {

    @Test
    void accepts_objects_with_a_complex_key() {
      var loadCalls = new ArrayList<List<JsonObject>>();
      var options =
          DataLoaderOptions.<JsonObject, JsonObject>newBuilder()
              .cacheKey(DataLoaderTestHelper.cacheKey())
              .build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var key1 = new JsonObject().put("id", 123);
      var key2 = new JsonObject().put("id", 123);

      var future1 = identityLoader.load(key1);
      var future2 = identityLoader.load(key2);

      identityLoader.dispatch();

      System.out.println(key1.get("id"));
      System.out.println(future1.join());

      assertThat(loadCalls).containsSequence(List.of(List.of(key1)));
      assertThat(future1.join()).isEqualTo(key1);
      assertThat(future2.join()).isEqualTo(key1);
    }

    @Test
    void clears_objects_with_a_complex_key() {
      var loadCalls = new ArrayList<List<JsonObject>>();
      var options =
          DataLoaderOptions.<JsonObject, JsonObject>newBuilder()
              .cacheKey(DataLoaderTestHelper.cacheKey())
              .build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var key1 = new JsonObject().put("id", 123);
      var key2 = new JsonObject().put("id", 123);

      var future1 = identityLoader.load(key1);
      identityLoader.clear(key2);
      var future2 = identityLoader.load(key1);

      identityLoader.dispatch();

      assertThat(loadCalls).containsSequence(List.of(key1, key1));
      assertThat(future1.join()).isEqualTo(key1);
      assertThat(future2.join()).isEqualTo(key1);
    }

    @Test
    void accepts_objects_with_different_order_of_keys() {
      var loadCalls = new ArrayList<List<JsonObject>>();
      var options =
          DataLoaderOptions.<JsonObject, JsonObject>newBuilder()
              .cacheKey(DataLoaderTestHelper.cacheKey())
              .build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var keyA = new JsonObject().put("a", 123).put("b", 321);
      var keyB = new JsonObject().put("b", 321).put("a", 123);

      var futureA = identityLoader.load(keyA);
      var futureB = identityLoader.load(keyB);

      identityLoader.dispatch();
      var values = CompletableFutures.allOf(List.of(futureA, futureB)).join();

      assertThat(values).containsSequence(keyA, keyB);

      assertThat(loadCalls).hasSize(1);
      assertThat(loadCalls.get(0)).hasSize(1);
      assertThat(loadCalls.get(0).get(0)).isEqualTo(keyA);
    }

    @Test
    void allows_priming_the_cache_with_an_object_key() {
      var loadCalls = new ArrayList<List<JsonObject>>();
      var options =
          DataLoaderOptions.<JsonObject, JsonObject>newBuilder()
              .cacheKey(DataLoaderTestHelper.cacheKey())
              .build();
      var identityLoader = DataLoaderTestHelper.idLoader(options, loadCalls);

      var key1 = new JsonObject().put("id", 123);
      var key2 = new JsonObject().put("id", 123);

      identityLoader.prime(key1, key1);

      var future1 = identityLoader.load(key1);
      var future2 = identityLoader.load(key2);

      identityLoader.dispatch();

      assertThat(loadCalls).containsSequence(List.of());
      assertThat(future1.join()).isEqualTo(key1);
      assertThat(future2.join()).isEqualTo(key1);
    }
  }

  @DisplayName("It is resilient to job queue ordering")
  @Nested
  class ItIsResilientToJobQueueOrdering {

    @Test
    void batches_loads_occurring_within_future() {
      var loadCalls = new ArrayList<List<String>>();
      var identityLoader = DataLoaderTestHelper.idLoader(loadCalls);

      Supplier<Object> nullValue = () -> null;

      AtomicBoolean v4Called = new AtomicBoolean(false);

      CompletableFuture.supplyAsync(nullValue)
          .thenAccept(
              v1 -> {
                identityLoader.load("A");
                CompletableFuture.supplyAsync(nullValue)
                    .thenAccept(
                        v2 -> {
                          identityLoader.load("B");
                          CompletableFuture.supplyAsync(nullValue)
                              .thenAccept(
                                  v3 -> {
                                    identityLoader.load("C");
                                    CompletableFuture.supplyAsync(nullValue)
                                        .thenAccept(
                                            v4 -> {
                                              identityLoader.load("D");
                                              v4Called.set(true);
                                            });
                                  });
                        });
              });

      await().untilTrue(v4Called);

      identityLoader.dispatch();

      assertThat(loadCalls).containsSequence(List.of(List.of("A", "B", "C", "D")));
    }

    @Test
    void can_call_a_loader_from_a_loader() {
      var deepLoadCalls = new ArrayList<List<String>>();
      var deepLoader = DataLoaderTestHelper.<String, String>idLoader(deepLoadCalls);

      var aLoadCalls = new ArrayList<List<String>>();
      var aLoader =
          DataLoader.<String, String>create(
              keys -> {
                aLoadCalls.add(keys);
                return deepLoader.loadMany(keys);
              });

      var bLoadCalls = new ArrayList<List<String>>();
      var bLoader =
          DataLoader.<String, String>create(
              keys -> {
                bLoadCalls.add(keys);
                return deepLoader.loadMany(keys);
              });

      var a1 = aLoader.load("A1");
      var a2 = aLoader.load("A2");
      var b1 = bLoader.load("B1");
      var b2 = bLoader.load("B2");

      aLoader.dispatch();
      deepLoader.dispatch();

      bLoader.dispatch();
      deepLoader.dispatch();

      assertThat(a1.join()).isEqualTo("A1");
      assertThat(a2.join()).isEqualTo("A2");
      assertThat(b1.join()).isEqualTo("B1");
      assertThat(b2.join()).isEqualTo("B2");

      assertThat(aLoadCalls).containsSequence(List.of(List.of("A1", "A2")));
      assertThat(bLoadCalls).containsSequence(List.of(List.of("B1", "B2")));
      assertThat(deepLoadCalls).containsSequence(List.of(List.of("A1", "A2"), List.of("B1", "B2")));
    }

    @Test
    void allow_composition_of_data_loader_calls() {
      var options = DataLoaderOptions.<Integer, TestUser>newBuilder().maxBatchSize(11).build();
      var loadCalls = new ArrayList<List<Integer>>();

      var userRepository = new UserRepository();

      var loader =
          DataLoader.create(
              keys -> {
                loadCalls.add(keys);
                List<TestUser> users =
                    keys.stream().map(userRepository::get).collect(Collectors.toUnmodifiableList());
                return CompletableFuture.supplyAsync(() -> users);
              },
              options);

      AtomicBoolean user3Called = new AtomicBoolean(false);
      AtomicBoolean user4Called = new AtomicBoolean(false);

      var f1 =
          loader
              .load(1)
              .thenAccept(
                  user ->
                      loader
                          .load(user.getInvitedById())
                          .thenAccept(
                              invitedBy -> {
                                user3Called.set(true);
                                assertThat(invitedBy.getName()).isEqualTo("user1");
                              }));

      var f2 =
          loader
              .load(2)
              .thenAccept(
                  user ->
                      loader
                          .load(user.getInvitedById())
                          .thenAccept(
                              invitedBy -> {
                                user4Called.set(true);
                                assertThat(invitedBy.getName()).isEqualTo("user2");
                              }));

      loader.dispatch();

      CompletableFuture.allOf(f1, f2).join();

      await().untilTrue(user3Called);
      await().untilTrue(user4Called);

      assertThat(loadCalls).containsSequence(List.of(1, 2), List.of(3, 4));
    }
  }
}
