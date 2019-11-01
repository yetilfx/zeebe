/*
 * Copyright © 2019  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package io.zeebe.logstreams.impl;

import com.netflix.concurrency.limits.limiter.AbstractLimiter;
import java.util.Optional;
import org.agrona.collections.Long2ObjectHashMap;

public class AppendEntryLimiter extends AbstractLimiter<Long> {

  private final Long2ObjectHashMap<Listener> appendedListeners = new Long2ObjectHashMap<>();
  private final int partitionId;
  private final AppendBackpressureMetrics metrics = new AppendBackpressureMetrics();

  protected AppendEntryLimiter(AppendEntryLimiterBuilder builder, int partitionId) {
    super(builder);
    this.partitionId = partitionId;
    metrics.setInflight(partitionId, 0);
    metrics.setNewLimit(partitionId, getLimit());
  }

  public Optional<Listener> acquire(Long position) {
    if (getInflight() >= getLimit()) {
      return createRejectedListener();
    }
    final Listener listener = createListener();
    return Optional.of(listener);
  }

  private void registerListener(long position, Listener listener) {
    // assumes the pair <streamId, requestId> is unique.
    appendedListeners.put(position, listener);
  }

  public boolean tryAcquire(Long position) {
    final Optional<Listener> acquired = acquire(position);
    return acquired
        .map(
            listener -> {
              registerListener(position, listener);
              metrics.incInflight(partitionId);
              return true;
            })
        .orElse(false);
  }

  public void onCommit(long position) {
    final Listener listener = appendedListeners.remove(position);
    if (listener != null) {
      listener.onSuccess();
      metrics.decInflight(partitionId);
    } else {
      // Ignore this message, if it happens immediately after failover. It can happen when a request
      // committed by the old leader is processed by the new leader.
      Loggers.LOGSTREAMS_LOGGER.warn(
          "Expected to have a rate limiter listener for pos {}, but none found. (This can happen during fail over.)",
          position);
    }
  }

  @Override
  protected void onNewLimit(int newLimit) {
    super.onNewLimit(newLimit);
    metrics.setNewLimit(partitionId, newLimit);
  }

  public static AppendEntryLimiterBuilder builder() {
    return new AppendEntryLimiterBuilder();
  }

  public static class AppendEntryLimiterBuilder
      extends AbstractLimiter.Builder<AppendEntryLimiterBuilder> {

    private int partitionId;

    @Override
    protected AppendEntryLimiterBuilder self() {
      return this;
    }

    public AppendEntryLimiterBuilder partitionId(int partition) {
      partitionId = partition;
      return this;
    }

    public AppendEntryLimiter build() {
      return new AppendEntryLimiter(this, partitionId);
    }
  }
}
