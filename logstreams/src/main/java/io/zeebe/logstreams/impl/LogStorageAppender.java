/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import com.netflix.concurrency.limits.limit.AbstractLimit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.GradientLimit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.distributedlog.impl.DistributedLogstreamPartition;
import io.zeebe.util.Environment;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

/** Consume the write buffer and append the blocks to the distributedlog. */
public class LogStorageAppender extends Actor {
  public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

  private final AtomicBoolean isFailed = new AtomicBoolean(false);

  private final BlockPeek blockPeek = new BlockPeek();
  private final String name;
  private final Subscription writeBufferSubscription;
  private final int maxAppendBlockSize;
  private final DistributedLogstreamPartition distributedLog;

  private long commitPosition;
  private final Runnable peekedBlockHandler = this::appendBlock;
  private long currentInFlightBytes;
  private final AppendEntryLimiter appendEntryLimiter;
  private final AppendBackpressureMetrics appendBackpressureMetrics;
  private final Environment environment;

  public LogStorageAppender(
      String name,
      DistributedLogstreamPartition distributedLog,
      Subscription writeBufferSubscription,
      int maxBlockSize) {

    environment = new Environment();

    this.name = name;
    this.distributedLog = distributedLog;
    this.writeBufferSubscription = writeBufferSubscription;
    this.maxAppendBlockSize = maxBlockSize;

    final int partitionId = distributedLog.getPartitionId();
    appendBackpressureMetrics = new AppendBackpressureMetrics(partitionId);

    final Boolean windowedLimiter =
        environment.getBool("ZEEBE_WINDOWED_APPEND_LIMITER").orElse(true);
    appendEntryLimiter =
        AppendEntryLimiter.builder()
            .limit(windowedLimiter ? WindowedLimit.newBuilder().build(buildLimit()) : buildLimit())
            .partitionId(partitionId)
            .build();
  }

  private AbstractLimit buildLimit() {
    final String algorithm = environment.get("ZEEBE_APPEND_LIMITER").orElse("vegas");

    final Map<String, Supplier<AbstractLimit>> algorithms =
        Map.of(
            "vegas",
            this::buildVegasLimit,
            "gradient",
            this::buildGradientLimit,
            "gradient2",
            this::buildGradient2Limit);

    Supplier<AbstractLimit> abstractLimitSupplier = algorithms.get(algorithm.toLowerCase());
    if (abstractLimitSupplier == null) {
      abstractLimitSupplier = this::buildVegasLimit;
    }

    return abstractLimitSupplier.get();
  }

  private AbstractLimit buildVegasLimit() {
    return VegasLimit.newBuilder()
        .initialLimit(environment.getInt("ZEEBE_INITIAL_APPEND_LIMIT").orElse(1024))
        .maxConcurrency(environment.getInt("ZEEBE_MAX_APPEND_CONCURRENCY").orElse(1024 * 32))
        .build();
  }

  private AbstractLimit buildGradientLimit() {
    return GradientLimit.newBuilder()
        .initialLimit(environment.getInt("ZEEBE_INITIAL_APPEND_LIMIT").orElse(1024))
        .maxConcurrency(environment.getInt("ZEEBE_MAX_APPEND_CONCURRENCY").orElse(1024 * 32))
        .build();
  }

  private AbstractLimit buildGradient2Limit() {
    return Gradient2Limit.newBuilder()
        .initialLimit(environment.getInt("ZEEBE_INITIAL_APPEND_LIMIT").orElse(1024))
        .maxConcurrency(environment.getInt("ZEEBE_MAX_APPEND_CONCURRENCY").orElse(1024 * 32))
        .queueSize(environment.getInt("ZEEBE_APPEND_QUEUE_SIZE").orElse(32))
        .minLimit(environment.getInt("ZEEBE_MIN_APPEND_LIMIT").orElse(256))
        .longWindow(environment.getInt("ZEEBE_APPEND_LONG_WINDOW").orElse(1200))
        //        .rttTolerance(environment.getDouble("ZEEBE_APPEND_RTT_TOLERANCE").orElse(1.5f))
        .build();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  protected void onActorStarting() {

    actor.consume(writeBufferSubscription, this::peekBlock);
  }

  private void peekBlock() {
    if (writeBufferSubscription.peekBlock(blockPeek, maxAppendBlockSize, true) > 0) {
      peekedBlockHandler.run();
    } else {
      actor.yield();
    }
  }

  private void appendBlock() {
    final ByteBuffer rawBuffer = blockPeek.getRawBuffer();
    final int bytes = rawBuffer.remaining();
    final var bytesToAppend = new byte[bytes];
    rawBuffer.get(bytesToAppend);

    // Commit position is the position of the last event. DistributedLogstream uses this position
    // to identify duplicate append requests during recovery.
    final long lastEventPosition = getLastEventPosition(bytesToAppend);
    commitPosition = lastEventPosition;

    appendBackpressureMetrics.newEntryToAppend();
    if (appendEntryLimiter.tryAcquire(lastEventPosition)) {
      currentInFlightBytes += bytes;
      appendToPrimitive(bytesToAppend, lastEventPosition);
      blockPeek.markCompleted();
    } else {
      appendBackpressureMetrics.deferred();
      LOG.trace(
          "Backpressure happens: inflight {} (in bytes {}) limit {}",
          appendEntryLimiter.getInflight(),
          currentInFlightBytes,
          appendEntryLimiter.getLimit());
      actor.yield();
    }
  }

  private void appendToPrimitive(byte[] bytesToAppend, long lastEventPosition) {
    actor.submit(
        () -> {
          distributedLog
              .asyncAppend(bytesToAppend, lastEventPosition)
              .whenComplete(
                  (appendPosition, error) -> {
                    if (error != null) {
                      LOG.error(
                          "Failed to append block with last event position {}, retry.",
                          lastEventPosition);
                      appendToPrimitive(bytesToAppend, lastEventPosition);
                    } else {
                      actor.call(
                          () -> {
                            appendEntryLimiter.onCommit(lastEventPosition);
                            currentInFlightBytes -= bytesToAppend.length;
                            actor.run(this::peekBlock);
                          });
                    }
                  });
        });
  }

  /* Iterate over the events in buffer and find the position of the last event */
  private long getLastEventPosition(byte[] buffer) {
    int bufferOffset = 0;
    final DirectBuffer directBuffer = new UnsafeBuffer(0, 0);

    directBuffer.wrap(buffer);
    long lastEventPosition = -1;

    final LoggedEventImpl nextEvent = new LoggedEventImpl();
    int remaining = buffer.length - bufferOffset;
    while (remaining > 0) {
      nextEvent.wrap(directBuffer, bufferOffset);
      bufferOffset += nextEvent.getFragmentLength();
      lastEventPosition = nextEvent.getPosition();
      remaining = buffer.length - bufferOffset;
    }
    return lastEventPosition;
  }

  public ActorFuture<Void> close() {
    return actor.close();
  }

  public boolean isFailed() {
    return isFailed.get();
  }

  public long getCurrentAppenderPosition() {
    return writeBufferSubscription.getPosition();
  }
}
