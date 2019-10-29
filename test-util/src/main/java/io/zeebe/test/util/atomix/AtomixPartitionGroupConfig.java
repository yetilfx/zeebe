/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.util.atomix;

import io.atomix.primitive.partition.PartitionGroup.Type;
import io.atomix.protocols.raft.RaftStateMachineFactory;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;

public class AtomixPartitionGroupConfig extends RaftPartitionGroupConfig {
  private RaftStateMachineFactory stateMachineFactory = RaftServiceManager::new;

  @Override
  public Type getType() {
    return AtomixPartitionGroup.TYPE;
  }

  @Override
  public RaftStateMachineFactory getStateMachineFactory() {
    return stateMachineFactory;
  }

  public AtomixPartitionGroupConfig setStateMachineFactory(
      final RaftStateMachineFactory stateMachineFactory) {
    this.stateMachineFactory = stateMachineFactory;
    return this;
  }
}
