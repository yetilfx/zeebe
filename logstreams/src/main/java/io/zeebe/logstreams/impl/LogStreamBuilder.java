/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl;

import static io.zeebe.util.EnsureUtil.ensureGreaterThanOrEqual;

import io.zeebe.logstreams.impl.service.LogStreamService;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.util.sched.channel.ActorConditions;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.agrona.concurrent.status.AtomicLongPosition;

public class LogStreamBuilder {
  private final int partitionId;
  private final AtomicLongPosition commitPosition = new AtomicLongPosition();
  private final ActorConditions onCommitPositionUpdatedConditions = new ActorConditions();
  private String logName;
  private int maxFragmentSize = 1024 * 1024 * 4;
  private LogStorage logStorage;
  private Function<LogStorage, LogStorage> logStorageStubber = Function.identity();

  public LogStreamBuilder(final int partitionId) {
    this.partitionId = partitionId;
  }

  public LogStreamBuilder logStorage(final LogStorage storage) {
    this.logStorage = storage;
    return this;
  }

  public LogStreamBuilder logName(final String logName) {
    this.logName = logName;
    return this;
  }

  public LogStreamBuilder logStorageStubber(final UnaryOperator<LogStorage> logStorageStubber) {
    this.logStorageStubber = logStorageStubber;
    return this;
  }

  public LogStreamBuilder maxFragmentSize(final int maxFragmentSize) {
    this.maxFragmentSize = maxFragmentSize;
    return this;
  }

  public LogStreamService build() {
    validate();

    if (logStorageStubber != null) {
      logStorage = logStorageStubber.apply(logStorage);
    }

    return new LogStreamService(
        onCommitPositionUpdatedConditions,
        logName,
        partitionId,
        maxFragmentSize,
        commitPosition,
        logStorage);
  }

  private void validate() {
    Objects.requireNonNull(logName, "logName");
    Objects.requireNonNull(logStorage, "logStorage");
    ensureGreaterThanOrEqual("partitionId", partitionId, 0);
  }
}
