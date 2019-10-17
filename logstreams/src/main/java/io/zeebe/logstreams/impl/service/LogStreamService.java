/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl.service;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamServiceName;

import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.logstreams.impl.LogStorageAppender;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.agrona.concurrent.status.Position;
import org.slf4j.Logger;

public class LogStreamService implements LogStream, Service<LogStream> {
  private static final long INVALID_ADDRESS = -1L;
  private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

  private final ActorConditions onCommitPositionUpdatedConditions;
  private final String logName;
  private final int partitionId;
  private final int maxFrameLength;
  private final Position commitPosition;
  private final LogStorage logStorage;

  private BufferedLogStreamReader reader;
  private ServiceStartContext serviceContext;
  private ActorFuture<LogStorageAppender> appenderFuture;
  private LogStorageAppender appender;

  public LogStreamService(
      final ActorConditions onCommitPositionUpdatedConditions,
      final String logName,
      final int partitionId,
      final int maxFrameLength,
      final Position commitPosition,
      final LogStorage logStorage) {
    this.onCommitPositionUpdatedConditions = onCommitPositionUpdatedConditions;
    this.logName = logName;
    this.partitionId = partitionId;
    this.maxFrameLength = maxFrameLength;
    this.commitPosition = commitPosition;
    this.logStorage = logStorage;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    commitPosition.setVolatile(INVALID_ADDRESS);

    serviceContext = startContext;
    reader = new BufferedLogStreamReader(this);
    setCommitPosition(reader.seekToEnd());

    try {
      logStorage.open();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    logStorage.close();
  }

  @Override
  public LogStream get() {
    return this;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public String getLogName() {
    return logName;
  }

  @Override
  public void close() {
    closeAsync().join();
  }

  @Override
  public ActorFuture<Void> closeAsync() {
    // this is a weird way of requesting close...
    return serviceContext.removeService(logStreamServiceName(logName));
  }

  @Override
  public long getCommitPosition() {
    return commitPosition.get();
  }

  @Override
  public void setCommitPosition(final long commitPosition) {
    this.commitPosition.proposeMaxOrdered(commitPosition);
    onCommitPositionUpdatedConditions.signalConsumers();
  }

  @Override
  public long append(final long commitPosition, final ByteBuffer buffer) {
    long appendResult = -1;
    boolean notAppended = true;
    do {
      try {
        appendResult = logStorage.append(buffer);
        notAppended = false;
      } catch (final IOException ioe) {
        // we want to retry the append
        // we avoid recursion, otherwise we can get stack overflow exceptions
        LOG.error(
            "Expected to append new buffer, but caught IOException. Will retry this operation.",
            ioe);
      }
    } while (notAppended);

    setCommitPosition(commitPosition);

    return appendResult;
  }

  @Override
  public LogStorage getLogStorage() {
    return logStorage;
  }

  @Override
  public Dispatcher getWriteBuffer() {
    return getLogStorageAppender().getWriteBuffer();
  }

  @Override
  public LogStorageAppender getLogStorageAppender() {
    if (appender == null && appenderFuture != null) {
      appender = appenderFuture.join();
    }

    return appender;
  }

  @Override
  public ActorFuture<Void> closeAppender() {
    if (appenderFuture != null) {
      appenderFuture.cancel(true);
      appenderFuture = null;
    }

    if (appender != null) {
      return appender.close();
    } else {
      return CompletableActorFuture.completed(null);
    }
  }

  @Override
  public ActorFuture<LogStorageAppender> openAppender() {
    final var serviceName = logStorageAppenderServiceName(getLogName());
    final LogStorageAppender appenderService =
        new LogStorageAppender(getLogStorage(), maxFrameLength);
    appenderFuture =
        serviceContext
            .createService(serviceName, appenderService)
            .dependency(serviceContext.getServiceName())
            .install();

    return appenderFuture;
  }

  @Override
  public void delete(final long position) {
    final boolean positionNotExist = !reader.seek(position);
    if (positionNotExist) {
      LOG.debug(
          "Tried to delete from log stream, but found no corresponding address in the log block index for the given position {}.",
          position);
      return;
    }

    final long blockAddress = reader.lastReadAddress();
    LOG.debug(
        "Delete data from log stream until position '{}' (address: '{}').", position, blockAddress);

    logStorage.delete(blockAddress);
  }

  @Override
  public void registerOnCommitPositionUpdatedCondition(final ActorCondition condition) {
    onCommitPositionUpdatedConditions.registerConsumer(condition);
  }

  @Override
  public void removeOnCommitPositionUpdatedCondition(final ActorCondition condition) {
    onCommitPositionUpdatedConditions.removeConsumer(condition);
  }
}
