/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package io.camunda.operate.webapp.rest.dto.metadata;

import io.camunda.operate.entities.EventEntity;
import io.camunda.operate.entities.EventMetadataEntity;
import io.camunda.operate.entities.FlowNodeInstanceEntity;
import io.camunda.operate.entities.FlowNodeType;
import java.time.OffsetDateTime;
import java.util.Map;

public class FlowNodeInstanceMetadataDto {

  /**
   * Flow node data.
   */
  private String flowNodeId;
  private String flowNodeInstanceId;
  private FlowNodeType flowNodeType;
  private OffsetDateTime startDate;
  private OffsetDateTime endDate;
  private String calledProcessInstanceId;
  private String calledProcessDefinitionName;

  private String eventId;
  /**
   * Job data.
   */
  private String jobType;
  private Integer jobRetries;
  private String jobWorker;
  private OffsetDateTime jobDeadline;
  private Map<String, String> jobCustomHeaders;

  public String getFlowNodeId() {
    return flowNodeId;
  }

  public FlowNodeInstanceMetadataDto setFlowNodeId(final String flowNodeId) {
    this.flowNodeId = flowNodeId;
    return this;
  }

  public String getFlowNodeInstanceId() {
    return flowNodeInstanceId;
  }

  public FlowNodeInstanceMetadataDto setFlowNodeInstanceId(final String flowNodeInstanceId) {
    this.flowNodeInstanceId = flowNodeInstanceId;
    return this;
  }

  public FlowNodeType getFlowNodeType() {
    return flowNodeType;
  }

  public FlowNodeInstanceMetadataDto setFlowNodeType(
      final FlowNodeType flowNodeType) {
    this.flowNodeType = flowNodeType;
    return this;
  }

  public OffsetDateTime getStartDate() {
    return startDate;
  }

  public FlowNodeInstanceMetadataDto setStartDate(final OffsetDateTime startDate) {
    this.startDate = startDate;
    return this;
  }

  public OffsetDateTime getEndDate() {
    return endDate;
  }

  public FlowNodeInstanceMetadataDto setEndDate(final OffsetDateTime endDate) {
    this.endDate = endDate;
    return this;
  }

  public String getCalledProcessInstanceId() {
    return calledProcessInstanceId;
  }

  public FlowNodeInstanceMetadataDto setCalledProcessInstanceId(
      final String calledProcessInstanceId) {
    this.calledProcessInstanceId = calledProcessInstanceId;
    return this;
  }

  public String getCalledProcessDefinitionName() {
    return calledProcessDefinitionName;
  }

  public FlowNodeInstanceMetadataDto setCalledProcessDefinitionName(
      final String calledProcessDefinitionName) {
    this.calledProcessDefinitionName = calledProcessDefinitionName;
    return this;
  }

  public String getEventId() {
    return eventId;
  }

  public FlowNodeInstanceMetadataDto setEventId(final String eventId) {
    this.eventId = eventId;
    return this;
  }

  public String getJobType() {
    return jobType;
  }

  public void setJobType(String jobType) {
    this.jobType = jobType;
  }

  public Integer getJobRetries() {
    return jobRetries;
  }

  public void setJobRetries(Integer jobRetries) {
    this.jobRetries = jobRetries;
  }

  public String getJobWorker() {
    return jobWorker;
  }

  public void setJobWorker(String jobWorker) {
    this.jobWorker = jobWorker;
  }

  public OffsetDateTime getJobDeadline() {
    return jobDeadline;
  }

  public void setJobDeadline(OffsetDateTime jobDeadline) {
    this.jobDeadline = jobDeadline;
  }

  public Map<String, String> getJobCustomHeaders() {
    return jobCustomHeaders;
  }

  public void setJobCustomHeaders(Map<String, String> jobCustomHeaders) {
    this.jobCustomHeaders = jobCustomHeaders;
  }

  public static FlowNodeInstanceMetadataDto createFrom(FlowNodeInstanceEntity flowNodeInstance,
      EventEntity eventEntity, String calledProcessInstanceId, String calledProcessDefinitionName) {
    FlowNodeInstanceMetadataDto metadataDto = new FlowNodeInstanceMetadataDto();
    //flow node instance data
    metadataDto.setFlowNodeInstanceId(flowNodeInstance.getId());
    metadataDto.setFlowNodeId(flowNodeInstance.getFlowNodeId());
    metadataDto.setFlowNodeType(flowNodeInstance.getType());
    metadataDto.setStartDate(flowNodeInstance.getStartDate());
    metadataDto.setEndDate(flowNodeInstance.getEndDate());
    if (calledProcessInstanceId != null) {
      metadataDto.setCalledProcessInstanceId(calledProcessInstanceId);
    }
    if (calledProcessDefinitionName != null) {
      metadataDto.setCalledProcessDefinitionName(calledProcessDefinitionName);
    }

    //last event data
    metadataDto.setEventId(eventEntity.getId());
    EventMetadataEntity eventMetadataEntity = eventEntity.getMetadata();
    if (eventMetadataEntity != null) {
      metadataDto.setJobCustomHeaders(eventMetadataEntity.getJobCustomHeaders());
      metadataDto.setJobDeadline(eventMetadataEntity.getJobDeadline());
      metadataDto.setJobRetries(eventMetadataEntity.getJobRetries());
      metadataDto.setJobType(eventMetadataEntity.getJobType());
      metadataDto.setJobWorker(eventMetadataEntity.getJobWorker());
    }

    return metadataDto;
  }

  @Override
  public String toString() {
    return "FlowNodeInstanceMetadataDto{" +
        "flowNodeId='" + flowNodeId + '\'' +
        ", flowNodeInstanceId='" + flowNodeInstanceId + '\'' +
        ", flowNodeType=" + flowNodeType +
        ", startDate=" + startDate +
        ", endDate=" + endDate +
        ", calledProcessInstanceId='" + calledProcessInstanceId + '\'' +
        ", calledProcessDefinitionName='" + calledProcessDefinitionName + '\'' +
        ", eventId='" + eventId + '\'' +
        ", jobType='" + jobType + '\'' +
        ", jobRetries=" + jobRetries +
        ", jobWorker='" + jobWorker + '\'' +
        ", jobDeadline=" + jobDeadline +
        ", jobCustomHeaders=" + jobCustomHeaders +
        '}';
  }
}
