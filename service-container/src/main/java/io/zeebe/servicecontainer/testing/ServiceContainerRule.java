/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.servicecontainer.testing;

import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.impl.ServiceContainerImpl;
import io.zeebe.util.LangUtil;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.rules.ExternalResource;

public class ServiceContainerRule extends ExternalResource {
  private final boolean shouldStop;
  private ServiceContainerImpl serviceContainer;
  private ActorSchedulerRule actorSchedulerRule;
  private ControlledActorSchedulerRule controlledActorSchedulerRule;

  public ServiceContainerRule(ActorSchedulerRule actorSchedulerRule, boolean shouldStop) {
    this.actorSchedulerRule = actorSchedulerRule;
    this.shouldStop = shouldStop;
  }

  public ServiceContainerRule(
      ControlledActorSchedulerRule controlledActorSchedulerRule, boolean shouldStop) {
    this.controlledActorSchedulerRule = controlledActorSchedulerRule;
    this.shouldStop = shouldStop;
  }

  public ServiceContainerRule(ActorSchedulerRule actorSchedulerRule) {
    this(actorSchedulerRule, true);
  }

  public ServiceContainerRule(ControlledActorSchedulerRule actorSchedulerRule) {
    this(actorSchedulerRule, false);
  }

  @Override
  public void before() throws Throwable {
    if (serviceContainer == null) {
      final ActorScheduler actorScheduler =
          actorSchedulerRule == null
              ? controlledActorSchedulerRule.get()
              : actorSchedulerRule.get();
      serviceContainer = new ServiceContainerImpl(actorScheduler);
      serviceContainer.start();
    }
  }

  @Override
  public void after() {
    if (serviceContainer != null) {
      if (shouldStop) {
        try {
          serviceContainer.close(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LangUtil.rethrowUnchecked(e);
        } catch (ExecutionException | TimeoutException e) {
          LangUtil.rethrowUnchecked(e);
        }
      }
    }
  }

  public ServiceContainer get() {
    return serviceContainer;
  }

  public ActorSchedulerRule getActorSchedulerRule() {
    return actorSchedulerRule;
  }
}
