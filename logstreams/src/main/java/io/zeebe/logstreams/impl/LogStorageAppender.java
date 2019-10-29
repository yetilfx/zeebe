/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.dispatcher.impl.PositionUtil;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

/** Consume the write buffer and append the blocks to the log */
public class LogStorageAppender extends Actor implements Service<LogStorageAppender> {
  public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

  private final AtomicBoolean isFailed = new AtomicBoolean(false);

  private final BlockPeek blockPeek = new BlockPeek();
  private final int maxBlockSize;
  private final LogStorage storage;

  private String name;
  private Dispatcher writeBuffer;
  private Subscription subscription;
  private ServiceStartContext serviceContext;

  public LogStorageAppender(final LogStorage storage, final int maxBlockSize) {
    this.storage = storage;
    this.maxBlockSize = maxBlockSize;
  }

  public Dispatcher getWriteBuffer() {
    return writeBuffer;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    serviceContext = startContext;
    name = serviceContext.getName();
    serviceContext.async(serviceContext.getScheduler().submitActor(this));
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    stopContext.async(actor.close());
  }

  @Override
  public LogStorageAppender get() {
    return this;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  protected void onActorStarting() {
    writeBuffer = createWriteBuffer();
    actor.runOnCompletion(writeBuffer.openSubscriptionAsync(getName()), this::onSubscriptionOpened);
  }

  @Override
  protected void onActorClosing() {
    actor.runOnCompletion(
        writeBuffer.closeAsync(),
        (nothing, error) -> {
          if (error != null) {
            LOG.error("Failed to close write buffer", error);
          }

          writeBuffer = null;
          subscription = null;
        });
  }

  public ActorFuture<Void> close() {
    return serviceContext.removeService(serviceContext.getServiceName());
  }

  public boolean isFailed() {
    return isFailed.get();
  }

  public long getCurrentAppenderPosition() {
    return subscription.getPosition();
  }

  private void onSubscriptionOpened(final Subscription subscription, final Throwable error) {
    if (error != null) {
      LOG.error("Failed to open write buffer subscription, closing appender", error);
      close();
      return;
    }

    this.subscription = subscription;
    this.actor.consume(this.subscription, this::peekBlock);
  }

  private void peekBlock() {
    if (subscription.peekBlock(blockPeek, maxBlockSize, true) > 0) {
      actor.runUntilDone(this::tryWrite);
    } else {
      actor.yield();
    }
  }

  /**
   * At the moment, the strategy is to run this forever until it succeeds; should be handled in a
   * more granular fashion, as only a certain class of error is potentially recoverable with time,
   * i.e. {@link java.io.IOException}. So, for example, an {@link IllegalStateException} will
   * probably not recover and we should trigger failover.
   */
  private void tryWrite() {
    try {
      final var index = storage.append(blockPeek.getRawBuffer().slice());
      LOG.trace("Appended entry {}", index);
      blockPeek.markCompleted();
      actor.done();
    } catch (final IOException | RuntimeException e) {
      LOG.error("Failed to append an entry to the log, retrying...", e);
      actor.yield();
    }
  }

  private Dispatcher createWriteBuffer() {
    final var dispatcherName = String.format("logstream.%s.writeBuffer", getName());
    return Dispatchers.create(dispatcherName)
        .maxFragmentLength(maxBlockSize)
        .initialPartitionId(determineInitialPartitionId() + 1)
        .actorScheduler(serviceContext.getScheduler())
        .build();
  }

  private int determineInitialPartitionId() {
    try (BufferedLogStreamReader logReader = new BufferedLogStreamReader()) {
      logReader.wrap(storage);

      // Get position of last entry
      final long lastPosition = logReader.seekToEnd();

      // dispatcher needs to generate positions greater than the last position
      int lastPartitionId = 0;

      if (lastPosition > 0) {
        lastPartitionId = PositionUtil.partitionId(lastPosition);
      }

      return lastPartitionId;
    }
  }
}
