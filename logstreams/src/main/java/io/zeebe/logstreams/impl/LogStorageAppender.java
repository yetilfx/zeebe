/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.protocols.raft.zeebe.ZeebeLogAppender;
import io.atomix.storage.journal.Indexed;
import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

/** Consume the write buffer and append the blocks to the log */
public class LogStorageAppender extends Actor {
  public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

  private final AtomicBoolean isFailed = new AtomicBoolean(false);

  private final BlockPeek blockPeek = new BlockPeek();
  private final String name;
  private final Subscription writeBufferSubscription;
  private final int maxAppendBlockSize;
  private final ZeebeLogAppender appender;
  private byte[] bytesToAppend;
  private final Runnable peekedBlockHandler = this::appendBlock;

  public LogStorageAppender(
      final String name,
      final ZeebeLogAppender appender,
      final Subscription writeBufferSubscription,
      final int maxBlockSize) {
    this.name = name;
    this.appender = appender;
    this.writeBufferSubscription = writeBufferSubscription;
    this.maxAppendBlockSize = maxBlockSize;
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
    bytesToAppend = new byte[rawBuffer.remaining()];
    rawBuffer.get(bytesToAppend);
    actor.runUntilDone(this::tryWrite);
  }

  /**
   * At the moment, the strategy is to run this forever until it succeeds; should be handled in a
   * more granular fashion, as only a certain class of error is potentially recoverable with time,
   * i.e. {@link java.io.IOException}. So, for example, an {@link IllegalStateException} will
   * probably not recover and we should trigger failover.
   */
  private void tryWrite() {
    final Indexed<ZeebeEntry> entry;
    try {
      entry = appender.appendEntry(bytesToAppend).join();
    } catch (final RuntimeException e) {
      LOG.error("Failed to append an entry to the log, retrying...", e);
      actor.yield();
      return;
    }

    LOG.trace("Appended entry {}", entry);
    blockPeek.markCompleted();
    actor.done();
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
