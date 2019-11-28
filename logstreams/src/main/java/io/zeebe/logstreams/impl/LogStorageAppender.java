/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import com.netflix.concurrency.limits.limit.AbstractLimit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.logstreams.impl.backpressure.AlgorithmCfg;
import io.zeebe.logstreams.impl.backpressure.AppendBackpressureMetrics;
import io.zeebe.logstreams.impl.backpressure.AppendEntryLimiter;
import io.zeebe.logstreams.impl.backpressure.AppendLimiter;
import io.zeebe.logstreams.impl.backpressure.AppenderGradient2Cfg;
import io.zeebe.logstreams.impl.backpressure.AppenderVegasCfg;
import io.zeebe.logstreams.impl.backpressure.BackpressureConstants;
import io.zeebe.logstreams.impl.backpressure.NoopAppendLimiter;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.LogStorage.AppendListener;
import io.zeebe.util.Environment;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

/** Consume the write buffer and append the blocks to the distributedlog. */
public class LogStorageAppender extends Actor {

  public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;
  private static final Map<String, AlgorithmCfg> ALGORITHM_CFG =
      Map.of("vegas", new AppenderVegasCfg(), "gradient2", new AppenderGradient2Cfg());
  private final AtomicBoolean isFailed = new AtomicBoolean(false);

  private final OneToOneConcurrentArrayQueue<Listener> pendingListeners =
      new OneToOneConcurrentArrayQueue<>(1024);
  private final String name;
  private final Subscription writeBufferSubscription;
  private final int maxAppendBlockSize;
  private final LogStorage logStorage;
  private final AppendLimiter appendEntryLimiter;
  private final AppendBackpressureMetrics appendBackpressureMetrics;
  private final Environment env;
  private final LoggedEventImpl positionReader = new LoggedEventImpl();
  private final long threshold;
  private final boolean isAppending = false;

  private final int retries = 0;
  private Listener listener;

  public LogStorageAppender(
      final String name,
      final int partitionId,
      final LogStorage logStorage,
      final Subscription writeBufferSubscription,
      final int maxBlockSize) {
    this.env = new Environment();
    this.name = name;
    this.logStorage = logStorage;
    this.writeBufferSubscription = writeBufferSubscription;
    this.maxAppendBlockSize = maxBlockSize;
    threshold = maxAppendBlockSize / 2;

    appendBackpressureMetrics = new AppendBackpressureMetrics(partitionId);

    final boolean isBackpressureEnabled =
        env.getBool(BackpressureConstants.ENV_BP_APPENDER).orElse(true);
    appendEntryLimiter =
        isBackpressureEnabled ? initBackpressure(partitionId) : initNoBackpressure(partitionId);
  }

  private AppendLimiter initBackpressure(final int partitionId) {
    final String algorithmName =
        env.get(BackpressureConstants.ENV_BP_APPENDER_ALGORITHM).orElse("vegas").toLowerCase();
    final AlgorithmCfg algorithmCfg =
        ALGORITHM_CFG.getOrDefault(algorithmName, new AppenderVegasCfg());
    algorithmCfg.applyEnvironment(env);

    final AbstractLimit abstractLimit = algorithmCfg.get();
    final boolean windowedLimiter =
        env.getBool(BackpressureConstants.ENV_BP_APPENDER_WINDOWED).orElse(false);

    LOG.debug(
        "Configured log appender back pressure at partition {} as {}. Window limiting is {}",
        partitionId,
        algorithmCfg,
        windowedLimiter ? "enabled" : "disabled");
    return AppendEntryLimiter.builder()
        .limit(windowedLimiter ? WindowedLimit.newBuilder().build(abstractLimit) : abstractLimit)
        .partitionId(partitionId)
        .build();
  }

  private AppendLimiter initNoBackpressure(final int partition) {
    LOG.warn(
        "No back pressure for the log appender (partition = {}) configured! This might cause problems.",
        partition);
    return new NoopAppendLimiter();
  }

  private void appendBlock() {
    if (pendingListeners.isEmpty() || listener != null) {
      return;
    }

    listener = pendingListeners.peek();
    if (appendEntryLimiter.tryAcquire(listener.positions.highest)) {
      listener = pendingListeners.poll();
      appendToStorage(listener);
    } else {
      listener = null;
      appendBackpressureMetrics.deferred();
      LOG.trace(
          "Backpressure happens: in flight {} limit {}",
          appendEntryLimiter.getInflight(),
          appendEntryLimiter.getLimit());
      actor.submit(this::appendBlock);
    }
  }

  private void appendToStorage(final Listener listener) {
    logStorage.append(
        listener.positions.lowest, listener.positions.highest, listener.buffer, listener);
  }

  public ActorFuture<Void> close() {
    return actor.close();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  protected void onActorStarting() {
    actor.consume(writeBufferSubscription, this::onWriteBufferAvailable);
  }

  private void onWriteBufferAvailable() {
    final BlockPeek blockPeek = new BlockPeek();
    if (writeBufferSubscription.peekBlock(blockPeek, maxAppendBlockSize, true) > 0) {

      final ByteBuffer rawBuffer = blockPeek.getRawBuffer();
      final int bytes = rawBuffer.remaining();
      final ByteBuffer copiedBuffer = ByteBuffer.allocate(bytes).put(rawBuffer).flip();
      final Positions positions = readPositions(copiedBuffer);
      final var listener = new Listener(positions, copiedBuffer);

      if (pendingListeners.offer(listener)) {
        blockPeek.markCompleted();
        actor.submit(this::appendBlock);
      } else {
        actor.yield();
      }
      // this is called else where
      // appendBlock(blockPeek);
    } else {
      actor.yield();
    }
  }

  public boolean isFailed() {
    return isFailed.get();
  }

  private Positions readPositions(final ByteBuffer buffer) {
    final var view = new UnsafeBuffer(buffer);
    final var positions = new Positions();
    var offset = 0;
    do {
      positionReader.wrap(view, offset);
      positions.accept(positionReader.getPosition());
      offset += positionReader.getLength();
    } while (offset < view.capacity());

    return positions;
  }

  private static final class Positions {
    private long lowest = Long.MAX_VALUE;
    private long highest = Long.MIN_VALUE;

    private void accept(final long position) {
      lowest = Math.min(lowest, position);
      highest = Math.max(highest, position);
    }
  }

  private final class Listener implements AppendListener {
    private final Positions positions;
    private final ByteBuffer buffer;

    private Listener(final Positions positions, final ByteBuffer buffer) {
      this.positions = positions;
      this.buffer = buffer;
    }

    @Override
    public void onWrite(final long address) {
      listener = null;
      actor.run(LogStorageAppender.this::appendBlock);
    }

    @Override
    public void onWriteError(final Throwable error) {
      actor.run(() -> appendToStorage(this));
    }

    @Override
    public void onCommit(final long address) {
      //      LOG.error("Committed successfully {}", address);
      releaseBackPressure();
    }

    @Override
    public void onCommitError(final long address, final Throwable error) {
      //      LOG.error("Error on committing {}", address, error);
      releaseBackPressure();
    }

    private void releaseBackPressure() {
      //      LOG.error("Will release {}", positions.highest);
      actor.run(() -> appendEntryLimiter.onCommit(positions.highest));
    }
  }
}
