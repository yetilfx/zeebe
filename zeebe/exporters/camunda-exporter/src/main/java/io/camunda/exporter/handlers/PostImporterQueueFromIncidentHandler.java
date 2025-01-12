/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.exporter.handlers;

import io.camunda.exporter.store.BatchRequest;
import io.camunda.webapps.schema.entities.operate.post.PostImporterActionType;
import io.camunda.webapps.schema.entities.operate.post.PostImporterQueueEntity;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import java.time.OffsetDateTime;
import java.util.List;

public class PostImporterQueueFromIncidentHandler
    implements ExportHandler<PostImporterQueueEntity, IncidentRecordValue> {

  private final String indexName;

  public PostImporterQueueFromIncidentHandler(final String indexName) {
    this.indexName = indexName;
  }

  @Override
  public ValueType getHandledValueType() {
    return ValueType.INCIDENT;
  }

  @Override
  public Class<PostImporterQueueEntity> getEntityType() {
    return PostImporterQueueEntity.class;
  }

  @Override
  public boolean handlesRecord(final Record<IncidentRecordValue> record) {
    return true;
  }

  @Override
  public List<String> generateIds(final Record<IncidentRecordValue> record) {
    String intent = record.getIntent().name();
    if (intent.equals(IncidentIntent.MIGRATED.name())) {
      intent = IncidentIntent.CREATED.name();
    }
    return List.of(String.format("%d-%s", record.getKey(), intent));
  }

  @Override
  public PostImporterQueueEntity createNewEntity(final String id) {
    return new PostImporterQueueEntity().setId(id);
  }

  @Override
  public void updateEntity(
      final Record<IncidentRecordValue> record, final PostImporterQueueEntity entity) {
    final IncidentRecordValue recordValue = record.getValue();
    String intent = record.getIntent().name();
    if (intent.equals(IncidentIntent.MIGRATED.name())) {
      intent = IncidentIntent.CREATED.name();
    }
    entity
        .setId(String.format("%d-%s", record.getKey(), intent))
        .setActionType(PostImporterActionType.INCIDENT)
        .setIntent(intent)
        .setKey(record.getKey())
        .setPosition(record.getPosition())
        .setCreationTime(OffsetDateTime.now())
        .setPartitionId(record.getPartitionId())
        .setProcessInstanceKey(recordValue.getProcessInstanceKey());
  }

  @Override
  public void flush(final PostImporterQueueEntity entity, final BatchRequest batchRequest) {
    batchRequest.add(indexName, entity);
  }

  @Override
  public String getIndexName() {
    return indexName;
  }
}
