/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl.service;

import io.atomix.protocols.raft.zeebe.ZeebeLogAppender;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.logstreams.impl.LogStorageAppender;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.SchedulingHints;

public class LogStorageAppenderService implements Service<LogStorageAppender> {
  private final Injector<Subscription> appenderSubscriptionInjector = new Injector<>();
  private final int maxAppendBlockSize;
  private final ZeebeLogAppender appender;

  private LogStorageAppender service;

  LogStorageAppenderService(final int maxAppendBlockSize, final ZeebeLogAppender appender) {
    this.maxAppendBlockSize = maxAppendBlockSize;
    this.appender = appender;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    final var subscription = appenderSubscriptionInjector.getValue();
    service =
        new LogStorageAppender(startContext.getName(), appender, subscription, maxAppendBlockSize);

    startContext.async(startContext.getScheduler().submitActor(service, SchedulingHints.ioBound()));
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    stopContext.async(service.close());
  }

  @Override
  public LogStorageAppender get() {
    return service;
  }

  Injector<Subscription> getAppenderSubscriptionInjector() {
    return appenderSubscriptionInjector;
  }
}
