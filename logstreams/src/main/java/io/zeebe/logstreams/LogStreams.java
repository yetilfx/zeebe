/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams;

import io.atomix.protocols.raft.partition.RaftPartition;
import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.impl.storage.atomix.AtomixLogStorage;

public class LogStreams {
  public static LogStreamBuilder createAtomixLogStream(final RaftPartition partition) {
    return new LogStreamBuilder(partition.id().id())
        .logName(partition.name())
        .logStorage(new AtomixLogStorage(partition.getServer()));
  }

  public static LogStreamBuilder createFsLogStream(final int partitionId) {
    return new LogStreamBuilder(partitionId).logName(Integer.toString(partitionId));
  }
}
