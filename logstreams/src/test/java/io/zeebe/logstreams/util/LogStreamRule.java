/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.util;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamServiceName;

import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.impl.service.LogStreamService;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import java.util.Objects;
import org.junit.rules.ExternalResource;

public final class LogStreamRule extends ExternalResource {
  private final ServiceContainerRule serviceContainer;
  private final AtomixLogStorageRule storage;
  private final boolean shouldStart;

  private LogStreamService logStream;
  private BufferedLogStreamReader logStreamReader;

  private LogStreamRule(
      final ServiceContainerRule serviceContainer,
      final AtomixLogStorageRule storage,
      final boolean shouldStart) {
    this.serviceContainer = serviceContainer;
    this.storage = storage;
    this.shouldStart = shouldStart;
  }

  public static LogStreamRule createStarted(
      final ServiceContainerRule serviceContainer, final AtomixLogStorageRule storage) {
    return new LogStreamRule(serviceContainer, storage, true);
  }

  public static LogStreamRule createStopped(
      final ServiceContainerRule serviceContainer, final AtomixLogStorageRule storage) {
    return new LogStreamRule(serviceContainer, storage, false);
  }

  @Override
  protected void before() {
    if (shouldStart) {
      start(buildDefaultLogStream(storage.get()));
    }
  }

  @Override
  protected void after() {
    stopLogStream();
  }

  public LogStream start(final LogStreamBuilder builder) {
    this.logStream = builder.build();
    start();

    return logStream;
  }

  public LogStream start() {
    getServiceContainer()
        .createService(logStreamServiceName(logStream.getLogName()), logStream)
        .install()
        .join();

    logStream.openAppender().join();
    return logStream;
  }

  public void stopLogStream() {
    if (logStream != null) {
      logStream.close();
    }

    if (logStreamReader != null) {
      logStreamReader.close();
      logStreamReader = null;
    }
  }

  public LogStreamReader getLogStreamReader() {
    Objects.requireNonNull(logStream, "logStream is not started yet");

    if (logStreamReader == null) {
      logStreamReader = new BufferedLogStreamReader();
    }

    logStreamReader.wrap(logStream);
    return logStreamReader;
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public ServiceContainer getServiceContainer() {
    return serviceContainer.get();
  }

  public static LogStreamBuilder buildDefaultLogStream(final LogStorage storage) {
    return new LogStreamBuilder(0).logStorage(storage);
  }
}
