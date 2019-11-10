/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport.commandapi;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.transport.backpressure.PartitionAwareRequestLimiter;
import io.zeebe.broker.transport.backpressure.RequestLimiter;
import io.zeebe.engine.processor.CommandResponseWriter;
import io.zeebe.engine.processor.TypedRecord;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerTransport;
import java.util.function.Consumer;

public class CommandApiService implements Service<CommandApiService> {

  private final ServiceGroupReference<Partition> leaderPartitionsGroupReference;
  private final Injector<ServerTransport> serverTransportInjector = new Injector<>();
  private final CommandApiMessageHandler service;
  private final PartitionAwareRequestLimiter limiter;
  private final CommandTracer tracer;
  private ServerOutput serverOutput;

  public CommandApiService(
      final CommandApiMessageHandler commandApiMessageHandler,
      final PartitionAwareRequestLimiter limiter,
      final CommandTracer tracer) {
    this.limiter = limiter;
    this.service = commandApiMessageHandler;
    this.tracer = tracer;
    leaderPartitionsGroupReference =
        ServiceGroupReference.<Partition>create()
            .onAdd(this::addPartition)
            .onRemove(this::removePartition)
            .build();
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    serverOutput = serverTransportInjector.getValue().getOutput();
  }

  @Override
  public CommandApiService get() {
    return this;
  }

  public CommandResponseWriter newCommandResponseWriter() {
    return new CommandResponseWriterImpl(serverOutput, tracer);
  }

  public ServiceGroupReference<Partition> getLeaderParitionsGroupReference() {
    return leaderPartitionsGroupReference;
  }

  public Injector<ServerTransport> getServerTransportInjector() {
    return serverTransportInjector;
  }

  public CommandApiMessageHandler getCommandApiMessageHandler() {
    return service;
  }

  private void removePartition(
      final ServiceName<Partition> partitionServiceName, final Partition partition) {
    limiter.removePartition(partition.getPartitionId());
    service.removePartition(partition.getLogStream());
  }

  private void addPartition(
      final ServiceName<Partition> partitionServiceName, final Partition partition) {
    limiter.addPartition(partition.getPartitionId());
    service.addPartition(partition.getLogStream(), limiter.getLimiter(partition.getPartitionId()));
  }

  public Consumer<TypedRecord> getOnProcessedListener(final int partitionId) {
    final RequestLimiter<Intent> partitionLimiter = limiter.getLimiter(partitionId);
    return typedRecord -> {
      if (typedRecord.getRecordType() == RecordType.COMMAND && typedRecord.hasRequestMetadata()) {
        partitionLimiter.onResponse(typedRecord.getRequestStreamId(), typedRecord.getRequestId());
      }
    };
  }
}
