/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams;

import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.log.LogStream;

public class LogStreams {
  public static LogStream createLogStream(final RaftPartitionServer partition) {

  }

  public static LogStreamBuilder createFsLogStream(final int partitionId) {
    return new LogStreamBuilder(partitionId).logName(Integer.toString(partitionId));
  }
}
