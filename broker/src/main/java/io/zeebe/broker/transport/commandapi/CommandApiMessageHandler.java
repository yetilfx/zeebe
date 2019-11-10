/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport.commandapi;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.transport.backpressure.BackpressureMetrics;
import io.zeebe.broker.transport.backpressure.RequestLimiter;
import io.zeebe.broker.transport.commandapi.CommandTracer.NoopCommandTracer;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.impl.encoding.ExecuteCommandRequest;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.zeebe.protocol.impl.record.value.variable.VariableDocumentRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.ExecuteCommandRequestDecoder;
import io.zeebe.protocol.record.MessageHeaderDecoder;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerMessageHandler;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerRequestHandler;
import java.util.EnumMap;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.slf4j.Logger;

public class CommandApiMessageHandler implements ServerMessageHandler, ServerRequestHandler {
  private static final Logger LOG = Loggers.TRANSPORT_LOGGER;

  private final ExecuteCommandRequest requestWrapper = new ExecuteCommandRequest();
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final ManyToOneConcurrentLinkedQueue<Runnable> cmdQueue =
      new ManyToOneConcurrentLinkedQueue<>();
  private final Consumer<Runnable> cmdConsumer = Runnable::run;
  private final Int2ObjectHashMap<LogStream> leadingStreams = new Int2ObjectHashMap<>();
  private final Int2ObjectHashMap<RequestLimiter<Intent>> partitionLimiters =
      new Int2ObjectHashMap<>();
  private final RecordMetadata eventMetadata = new RecordMetadata();
  private final LogStreamRecordWriter logStreamWriter = new LogStreamWriterImpl();
  private final EnumMap<ValueType, UnpackedObject> recordsByType = new EnumMap<>(ValueType.class);

  private final ErrorResponseWriter errorResponseWriter;
  private final BackpressureMetrics metrics;
  private final CommandTracer tracer;

  public CommandApiMessageHandler() {
    this(new NoopCommandTracer());
  }

  public CommandApiMessageHandler(final CommandTracer tracer) {
    this.metrics = new BackpressureMetrics();
    this.tracer = tracer;
    this.errorResponseWriter = new ErrorResponseWriter(tracer);
    initEventTypeMap();
  }

  private void initEventTypeMap() {
    recordsByType.put(ValueType.DEPLOYMENT, new DeploymentRecord());
    recordsByType.put(ValueType.JOB, new JobRecord());
    recordsByType.put(ValueType.WORKFLOW_INSTANCE, new WorkflowInstanceRecord());
    recordsByType.put(ValueType.MESSAGE, new MessageRecord());
    recordsByType.put(ValueType.JOB_BATCH, new JobBatchRecord());
    recordsByType.put(ValueType.INCIDENT, new IncidentRecord());
    recordsByType.put(ValueType.VARIABLE_DOCUMENT, new VariableDocumentRecord());
    recordsByType.put(ValueType.WORKFLOW_INSTANCE_CREATION, new WorkflowInstanceCreationRecord());
  }

  private boolean handleExecuteCommandRequest(
      final ServerOutput output,
      final RemoteAddress requestAddress,
      final long requestId,
      final RecordMetadata eventMetadata,
      final DirectBuffer buffer,
      final int messageOffset,
      final int messageLength) {
    requestWrapper.wrap(buffer, messageOffset, messageLength);

    final int partitionId = requestWrapper.getPartitionId();
    final long key = requestWrapper.getKey();
    final ValueType eventType = requestWrapper.getValueType();
    final Intent eventIntent = requestWrapper.getIntent();

    tracer.start(
        requestWrapper.getSpanContext(), requestAddress.getStreamId(), requestId, partitionId);

    final LogStream logStream = leadingStreams.get(partitionId);
    if (logStream == null) {
      return errorResponseWriter
          .partitionLeaderMismatch(partitionId)
          .tryWriteResponseOrLogFailure(output, requestAddress.getStreamId(), requestId);
    }

    final UnpackedObject event = recordsByType.get(eventType);
    if (event == null) {
      return errorResponseWriter
          .unsupportedMessage(eventType.name(), recordsByType.keySet().toArray())
          .tryWriteResponseOrLogFailure(output, requestAddress.getStreamId(), requestId);
    }

    event.reset();

    try {
      // verify that the event / command is valid
      event.wrap(requestWrapper.getValue(), 0, requestWrapper.getValue().capacity());
    } catch (final RuntimeException e) {
      LOG.error("Failed to deserialize message of type {} in client API", eventType.name(), e);

      return errorResponseWriter
          .malformedRequest(e)
          .tryWriteResponseOrLogFailure(output, requestAddress.getStreamId(), requestId);
    }

    eventMetadata.recordType(RecordType.COMMAND);
    eventMetadata.intent(eventIntent);
    eventMetadata.valueType(eventType);

    metrics.receivedRequest(partitionId);
    final RequestLimiter<Intent> limiter = partitionLimiters.get(partitionId);
    if (!limiter.tryAcquire(requestAddress.getStreamId(), requestId, eventIntent)) {
      metrics.dropped(partitionId);
      LOG.trace(
          "Partition-{} receiving too many requests. Current limit {} inflight {}, dropping request {} from gateway {}",
          partitionId,
          limiter.getLimit(),
          limiter.getInflightCount(),
          requestId,
          requestAddress.getAddress());
      return errorResponseWriter
          .resourceExhausted()
          .tryWriteResponse(output, requestAddress.getStreamId(), requestId);
    }

    boolean written = false;
    try {
      written = writeCommand(eventMetadata, key, logStream);
    } finally {
      if (!written) {
        tracer.finish(requestAddress.getStreamId(), requestId, true);
        limiter.onIgnore(requestAddress.getStreamId(), requestId);
      }
    }
    return written;
  }

  private boolean writeCommand(
      final RecordMetadata eventMetadata, final long key, final LogStream logStream) {
    logStreamWriter.wrap(logStream);

    if (key != ExecuteCommandRequestDecoder.keyNullValue()) {
      logStreamWriter.key(key);
    } else {
      logStreamWriter.keyNull();
    }

    final long eventPosition =
        logStreamWriter
            .metadataWriter(eventMetadata)
            .value(requestWrapper.getValue(), 0, requestWrapper.getValue().capacity())
            .tryWrite();

    return eventPosition >= 0;
  }

  public void addPartition(final LogStream logStream, final RequestLimiter<Intent> limiter) {
    cmdQueue.add(
        () -> {
          leadingStreams.put(logStream.getPartitionId(), logStream);
          partitionLimiters.put(logStream.getPartitionId(), limiter);
        });
  }

  public void removePartition(final LogStream logStream) {
    cmdQueue.add(
        () -> {
          leadingStreams.remove(logStream.getPartitionId());
          partitionLimiters.remove(logStream.getPartitionId());
        });
  }

  @Override
  public boolean onRequest(
      final ServerOutput output,
      final RemoteAddress remoteAddress,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final long requestId) {
    drainCommandQueue();

    messageHeaderDecoder.wrap(buffer, offset);

    final int templateId = messageHeaderDecoder.templateId();
    final int clientVersion = messageHeaderDecoder.version();

    if (clientVersion > Protocol.PROTOCOL_VERSION) {
      return errorResponseWriter
          .invalidClientVersion(Protocol.PROTOCOL_VERSION, clientVersion)
          .tryWriteResponse(output, remoteAddress.getStreamId(), requestId);
    }

    eventMetadata.reset();
    eventMetadata.protocolVersion(clientVersion);
    eventMetadata.requestId(requestId);
    eventMetadata.requestStreamId(remoteAddress.getStreamId());

    if (templateId == ExecuteCommandRequestDecoder.TEMPLATE_ID) {
      return handleExecuteCommandRequest(
          output, remoteAddress, requestId, eventMetadata, buffer, offset, length);
    }

    return errorResponseWriter
        .invalidMessageTemplate(templateId, ExecuteCommandRequestDecoder.TEMPLATE_ID)
        .tryWriteResponse(output, remoteAddress.getStreamId(), requestId);
  }

  @Override
  public boolean onMessage(
      final ServerOutput output,
      final RemoteAddress remoteAddress,
      final DirectBuffer buffer,
      final int offset,
      final int length) {
    // ignore; currently no incoming single-message client interactions
    return true;
  }

  private void drainCommandQueue() {
    while (!cmdQueue.isEmpty()) {
      final Runnable runnable = cmdQueue.poll();
      if (runnable != null) {
        cmdConsumer.accept(runnable);
      }
    }
  }
}
