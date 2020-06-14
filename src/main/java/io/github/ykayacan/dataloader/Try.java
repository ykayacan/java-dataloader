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

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Try<T> {

  static <T> Try<T> success(T value) {
    return new Success<>(value);
  }

  static <T> Try<T> failure(String message) {
    return failure(new Exception(message));
  }

  static <T> Try<T> failure() {
    return failure(new Exception());
  }

  static <T> Try<T> failure(Throwable error) {
    return new Failure<>(error);
  }

  static <T> Try<T> of(Supplier<T> supplier) {
    try {
      return success(supplier.get());
    } catch (Exception error) {
      return failure(error);
    }
  }

  Throwable getCause();

  boolean isSuccess();

  boolean isFailure();

  /**
   * Returns the value if available. If not, it throws {@code NoSuchElementException}
   *
   * @return the wrapped value
   * @throws NoSuchElementException if value is not available
   */
  T get();

  default <R> Try<R> map(Function<T, R> mapper) {
    if (isSuccess()) {
      return success(mapper.apply(get()));
    }
    return failure(getCause());
  }

  default <R> Try<R> flatMap(Function<T, Try<R>> mapper) {
    if (isSuccess()) {
      return mapper.apply(get());
    }
    return failure(getCause());
  }

  default Try<T> onFailure(Consumer<Throwable> consumer) {
    if (isFailure()) {
      consumer.accept(getCause());
    }
    return this;
  }

  default Try<T> onSuccess(Consumer<T> consumer) {
    if (isSuccess()) {
      consumer.accept(get());
    }
    return this;
  }

  default Try<T> recover(Function<Throwable, T> mapper) {
    if (isFailure()) {
      return Try.of(() -> mapper.apply(getCause()));
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  default <X extends Throwable> Try<T> recoverWith(Class<X> type, Function<X, T> mapper) {
    if (isFailure()) {
      Throwable cause = getCause();
      if (type.isAssignableFrom(cause.getClass())) {
        return Try.of(() -> mapper.apply((X) getCause()));
      }
    }
    return this;
  }

  default <U> U fold(Function<Throwable, U> failureMapper, Function<T, U> successMapper) {
    return isSuccess() ? successMapper.apply(get()) : failureMapper.apply(getCause());
  }

  default Try<T> orElse(Try<T> orElse) {
    return isFailure() ? orElse : this;
  }

  default <X extends Throwable> T getOrElseThrow(Supplier<X> supplier) throws X {
    if (isSuccess()) {
      return get();
    }
    throw supplier.get();
  }

  default Optional<T> toOptional() {
    return fold(throwable -> Optional.empty(), Optional::ofNullable);
  }

  final class Success<T> implements Try<T>, Serializable {

    private static final long serialVersionUID = -3934628369477099278L;

    private final transient T value;

    private Success(T value) {
      Assert.checkNotNull(value);

      this.value = value;
    }

    @Override
    public boolean isFailure() {
      return false;
    }

    @Override
    public boolean isSuccess() {
      return true;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public Throwable getCause() {
      throw new NoSuchElementException("success doesn't have any cause");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Success<?> success = (Success<?>) o;
      return value.equals(success.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public String toString() {
      return "Success(" + value + ")";
    }
  }

  final class Failure<T> implements Try<T>, Serializable {

    private static final long serialVersionUID = -8155444386075553318L;

    private final Throwable cause;

    private Failure(Throwable cause) {
      Assert.checkNotNull(cause);

      this.cause = cause;
    }

    @Override
    public boolean isFailure() {
      return true;
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public T get() {
      throw new NoSuchElementException("failure doesn't have any value");
    }

    @Override
    public Throwable getCause() {
      return cause;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Failure<?> failure = (Failure<?>) o;
      return cause.getMessage().equals(failure.cause.getMessage());
    }

    @Override
    public int hashCode() {
      return Objects.hash(cause);
    }

    @Override
    public String toString() {
      return "Failure(" + cause + ")";
    }
  }
}
