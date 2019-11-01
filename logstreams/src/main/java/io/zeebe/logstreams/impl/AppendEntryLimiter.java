/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import com.netflix.concurrency.limits.limiter.AbstractLimiter;
import java.util.Optional;
import org.agrona.collections.Long2ObjectHashMap;

public class AppendEntryLimiter extends AbstractLimiter<Long> {

  private final Long2ObjectHashMap<Listener> appendedListeners = new Long2ObjectHashMap<>();
  private final AppendBackpressureMetrics metrics;

  protected AppendEntryLimiter(AppendEntryLimiterBuilder builder, int partitionId) {
    super(builder);
    metrics = new AppendBackpressureMetrics(partitionId);
    metrics.setInflight(0);
    metrics.setNewLimit(getLimit());
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
              metrics.incInflight();
              return true;
            })
        .orElse(false);
  }

  public void onCommit(long position) {
    final Listener listener = appendedListeners.remove(position);
    if (listener != null) {
      listener.onSuccess();
      metrics.decInflight();
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
    metrics.setNewLimit(newLimit);
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
