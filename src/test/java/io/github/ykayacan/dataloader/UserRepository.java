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

class UserRepository {

  private static final Map<Integer, TestUser> USERS =
      Map.of(
          1, new TestUser(1, "user1", 3),
          2, new TestUser(2, "user2", 4),
          3, new TestUser(3, "user3", -1),
          4, new TestUser(4, "user4", -1));

  TestUser get(int userId) {
    return USERS.get(userId);
  }
}
