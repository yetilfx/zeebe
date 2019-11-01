/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.distributedlog.impl.DistributedLogstreamPartition;
import io.zeebe.util.ByteUnit;
import io.zeebe.util.Environment;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final long maxBytesInflight;

  private byte[] bytesToAppend;
  private long commitPosition;
  private final Runnable peekedBlockHandler = this::appendBlock;
  private long currentInFlightBytes;
  private final AppendEntryLimiter appendEntryLimiter;

  public LogStorageAppender(
      String name,
      DistributedLogstreamPartition distributedLog,
      Subscription writeBufferSubscription,
      int maxBlockSize) {

    maxBytesInflight =
        new Environment()
            .getLong("ZEEBE_MAX_BYTES_INFLIGHT")
            .orElse(ByteUnit.KILOBYTES.toBytes(32));

    LOG.info("Uses maxBytesInflight {} for backpressure.", maxBytesInflight);
    this.name = name;
    this.distributedLog = distributedLog;
    this.writeBufferSubscription = writeBufferSubscription;
    this.maxAppendBlockSize = maxBlockSize;
    appendEntryLimiter =
        AppendEntryLimiter.builder()
            .limit(WindowedLimit.newBuilder().build(VegasLimit.newDefault()))
            .partitionId(distributedLog.getPartitionId())
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
    this.bytesToAppend = new byte[bytes];
    rawBuffer.get(this.bytesToAppend);

    // Commit position is the position of the last event. DistributedLogstream uses this position
    // to identify duplicate append requests during recovery.
    final long lastEventPosition = getLastEventPosition(this.bytesToAppend);
    commitPosition = lastEventPosition;

    if (appendEntryLimiter.tryAcquire(lastEventPosition)) {
      currentInFlightBytes += bytes;

      actor.runUntilDone(
          () -> {
            distributedLog
                .asyncAppend(bytesToAppend, commitPosition)
                .thenAccept(
                    pos ->
                        actor.call(
                            () -> {
                              appendEntryLimiter.onCommit(pos);
                              currentInFlightBytes -= bytes;
                              actor.run(this::peekBlock);
                            }));
            blockPeek.markCompleted();
            actor.done();
          });
    } else {
      LOG.info(
          "Backpressure happens: inflight {} in bytes {} limit {}",
          appendEntryLimiter.getInflight(),
          currentInFlightBytes,
          appendEntryLimiter.getLimit());
      actor.yield();
    }
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
