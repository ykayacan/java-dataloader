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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class CompletableFutures {

  private CompletableFutures() {
    throw new AssertionError();
  }

  /**
   * Converts {@code Collection<CompletableFuture<T>>} to {@code CompletableFuture<List<T>>}.
   *
   * @param <T>     the type parameter
   * @param futures the futures
   * @return the completable future
   * @see <a href="https://4comprehension.com/improving-completablefutureallof-anyof-api-java-methods">Improving CompletableFuture#allOf/anyOf API Java Methods</a>
   */
  public static <T> CompletableFuture<List<T>> allOf(Collection<CompletableFuture<T>> futures) {
    var result =
        futures.stream()
            .collect(
                Collectors.collectingAndThen(
                    Collectors.toList(),
                    l ->
                        CompletableFuture.allOf(l.toArray(new CompletableFuture[0]))
                            .thenApply(
                                __ ->
                                    l.stream()
                                        .map(CompletableFuture::join)
                                        .collect(Collectors.toList()))));

    for (CompletableFuture<?> f : futures) {
      f.handle((__, ex) -> ex == null || result.completeExceptionally(ex));
    }

    return result;
  }
}
