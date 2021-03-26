/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.operate.zeebeimport.v1_0.processors;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.camunda.operate.entities.ErrorType;
import org.camunda.operate.entities.IncidentEntity;
import org.camunda.operate.entities.IncidentState;
import org.camunda.operate.entities.OperationType;
import org.camunda.operate.property.OperateProperties;
import org.camunda.operate.schema.templates.IncidentTemplate;
import org.camunda.operate.exceptions.PersistenceException;
import org.camunda.operate.util.ConversionUtils;
import org.camunda.operate.util.DateUtil;
import org.camunda.operate.util.ElasticsearchUtil;
import org.camunda.operate.zeebeimport.ElasticsearchManager;
import org.camunda.operate.zeebeimport.IncidentNotifier;
import org.camunda.operate.zeebeimport.v1_0.record.value.IncidentRecordValueImpl;
import org.camunda.operate.zeebeimport.v1_0.record.Intent;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.protocol.record.Record;

@Component
public class IncidentZeebeRecordProcessor {

  private static final Logger logger = LoggerFactory.getLogger(IncidentZeebeRecordProcessor.class);

  @Autowired
  private OperateProperties operateProperties;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private IncidentTemplate incidentTemplate;

  @Autowired
  private ElasticsearchManager elasticsearchManager;


  @Autowired
  private IncidentNotifier incidentNotifier;

  public void processIncidentRecord(List<Record> records, BulkRequest bulkRequest) throws PersistenceException {
    List<IncidentEntity> newIncidents = new ArrayList<>();
    for (Record record: records) {
      processIncidentRecord(record, bulkRequest, newIncidents::add);
    }
    if (operateProperties.getAlert().getWebhook() != null) {
      incidentNotifier.notifyOnIncidents(newIncidents);
    }
  }

  public void processIncidentRecord(Record record, BulkRequest bulkRequest,
      Consumer<IncidentEntity> newIncidentHandler) throws PersistenceException {
    IncidentRecordValueImpl recordValue = (IncidentRecordValueImpl)record.getValue();

    persistIncident(record, recordValue, bulkRequest, newIncidentHandler);

  }

  private void persistIncident(Record record, IncidentRecordValueImpl recordValue, BulkRequest bulkRequest,
      Consumer<IncidentEntity> newIncidentHandler) throws PersistenceException {
    final String intentStr = record.getIntent().name();
    final Long incidentKey = record.getKey();
    if (intentStr.equals(Intent.RESOLVED.toString())) {

      //resolve corresponding operation
      elasticsearchManager.completeOperation(null, recordValue.getProcessInstanceKey(), incidentKey, OperationType.RESOLVE_INCIDENT, bulkRequest);

      bulkRequest.add(getIncidentDeleteQuery(incidentKey));
    } else if (intentStr.equals(Intent.CREATED.toString())) {
      IncidentEntity incident = new IncidentEntity();
      incident.setId( ConversionUtils.toStringOrNull(incidentKey));
      incident.setKey(incidentKey);
      if (recordValue.getJobKey() > 0) {
        incident.setJobKey(recordValue.getJobKey());
      }
      if (recordValue.getProcessInstanceKey() > 0) {
        incident.setProcessInstanceKey(recordValue.getProcessInstanceKey());
      }
      if (recordValue.getProcessDefinitionKey() > 0) {
        incident.setProcessDefinitionKey(recordValue.getProcessDefinitionKey());
      }
      String errorMessage = StringUtils.trimWhitespace(recordValue.getErrorMessage());
      incident.setErrorMessage(errorMessage);
      incident.setErrorType(ErrorType.fromZeebeErrorType(recordValue.getErrorType() == null ? null : recordValue.getErrorType().name()));
      incident.setFlowNodeId(recordValue.getElementId());
      if (recordValue.getElementInstanceKey() > 0) {
        incident.setFlowNodeInstanceKey(recordValue.getElementInstanceKey());
      }
      incident.setState(IncidentState.ACTIVE);
      incident.setCreationTime(DateUtil.toOffsetDateTime(Instant.ofEpochMilli(record.getTimestamp())));
      bulkRequest.add(getIncidentInsertQuery(incident));
      newIncidentHandler.accept(incident);
    }
  }

  private IndexRequest getIncidentInsertQuery(IncidentEntity incident) throws PersistenceException {
    try {
      logger.debug("Index incident: id {}", incident.getId());
      return new IndexRequest(incidentTemplate.getFullQualifiedName(), ElasticsearchUtil.ES_INDEX_TYPE, String.valueOf(incident.getKey()))
        .source(objectMapper.writeValueAsString(incident), XContentType.JSON);
    } catch (IOException e) {
      logger.error("Error preparing the query to index incident", e);
      throw new PersistenceException(String.format("Error preparing the query to index incident [%s]", incident), e);
    }
  }

  private DeleteRequest getIncidentDeleteQuery(Long incidentKey) throws PersistenceException {
    logger.debug("Delete incident: key {}", incidentKey);
    return new DeleteRequest(incidentTemplate.getFullQualifiedName(), ElasticsearchUtil.ES_INDEX_TYPE, incidentKey.toString());
  }

}
