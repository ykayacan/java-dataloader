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

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface BatchScheduler {

  static BatchScheduler defaultScheduler() {
    return new DefaultBatchScheduler();
  }

  void schedule(Runnable runnable, CompletableFuture<Void> finishedCallback);

  CompletableFuture<Void> dispatch();

  class DefaultBatchScheduler implements BatchScheduler {

    private final Queue<Runnable> callbacks;
    private CompletableFuture<Void> finishedCallback;

    private DefaultBatchScheduler() {
      callbacks = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void schedule(Runnable runnable, CompletableFuture<Void> finishedCallback) {
      this.finishedCallback = finishedCallback;
      callbacks.add(runnable);
    }

    @Override
    public CompletableFuture<Void> dispatch() {
      while (!callbacks.isEmpty()) {
        callbacks.poll().run();
      }

      return finishedCallback;
    }
  }
}
