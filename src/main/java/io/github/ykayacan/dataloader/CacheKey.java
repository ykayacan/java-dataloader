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

@FunctionalInterface
public interface CacheKey<K> {

  static <K> CacheKey<K> identity() {
    return k -> k;
  }

  /**
   * Returns the cache key that is created from the provided input key.
   *
   * @param key the input key
   * @return the cache key
   */
  Object getKey(K key);
}
