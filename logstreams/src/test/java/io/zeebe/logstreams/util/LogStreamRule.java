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
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.Objects;
import java.util.function.Supplier;
import org.junit.rules.ExternalResource;

public final class LogStreamRule extends ExternalResource {
  private final ServiceContainerRule serviceContainer;
  private final Supplier<LogStorage> storage;
  private final boolean shouldStart;

  private LogStreamService logStream;
  private BufferedLogStreamReader logStreamReader;

  private LogStreamRule(
      final ServiceContainerRule serviceContainer,
      final Supplier<LogStorage> storage,
      final boolean shouldStart) {
    this.serviceContainer = serviceContainer;
    this.storage = storage;
    this.shouldStart = shouldStart;
  }

  private LogStreamRule(final Supplier<LogStorage> storage, final boolean shouldStart) {
    this(new ServiceContainerRule(new ActorSchedulerRule()), storage, shouldStart);
  }

  public static LogStreamRule createStarted(final Supplier<LogStorage> storage) {
    return new LogStreamRule(storage, true);
  }

  public static LogStreamRule createStopped(final Supplier<LogStorage> storage) {
    return new LogStreamRule(storage, false);
  }

  @Override
  public void before() {
    if (shouldStart) {
      start();
    }
  }

  @Override
  protected void after() {
    stopLogStream();
  }

  public LogStream start(final LogStreamBuilder builder) {
    serviceContainer.getActorSchedulerRule().before();
    serviceContainer.before();

    logStream = builder.build();
    getServiceContainer()
        .createService(logStreamServiceName(logStream.getLogName()), logStream)
        .install()
        .join();

    logStream.openAppender().join();
    return logStream;
  }

  public LogStream start() {
    return start(buildDefaultLogStream(storage.get()));
  }

  public void stopLogStream() {
    if (logStream != null) {
      logStream.close();
    }

    if (logStreamReader != null) {
      logStreamReader.close();
      logStreamReader = null;
    }

    serviceContainer.after();
    serviceContainer.getActorSchedulerRule().after();
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
    return new LogStreamBuilder(0).logName("0").maxBlockSize(512 * 1024).logStorage(storage);
  }
}
