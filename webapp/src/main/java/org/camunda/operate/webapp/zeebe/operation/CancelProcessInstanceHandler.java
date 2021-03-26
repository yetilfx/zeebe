/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.operate.webapp.zeebe.operation;

import org.camunda.operate.entities.OperationEntity;
import org.camunda.operate.entities.OperationType;
import org.camunda.operate.entities.listview.ProcessInstanceForListViewEntity;
import org.camunda.operate.entities.listview.ProcessInstanceState;
import org.camunda.operate.webapp.es.reader.ProcessInstanceReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.zeebe.client.ZeebeClient;

/**
 * Operation handler to cancel process instances.
 */
@Component
public class CancelProcessInstanceHandler extends AbstractOperationHandler implements OperationHandler {

  @Autowired
  private ProcessInstanceReader processInstanceReader;

  @Autowired
  private ZeebeClient zeebeClient;

  @Override
  public void handleWithException(OperationEntity operation) throws Exception {
    if (operation.getProcessInstanceKey() == null) {
      failOperation(operation, "No process instance id is provided.");
      return;
    }
    final ProcessInstanceForListViewEntity processInstance = processInstanceReader.getProcessInstanceByKey(operation.getProcessInstanceKey());

    if (!processInstance.getState().equals(ProcessInstanceState.ACTIVE) && !processInstance.getState().equals(ProcessInstanceState.INCIDENT)) {
      //fail operation
      failOperation(operation,
          String.format("Unable to cancel %s process instance. Instance must be in ACTIVE or INCIDENT state.", processInstance.getState()));
      return;
    }
    zeebeClient.newCancelInstanceCommand(processInstance.getKey()).send().join();
    //mark operation as sent
    markAsSent(operation);
  }


  @Override
  public OperationType getType() {
    return OperationType.CANCEL_PROCESS_INSTANCE;
  }

  public void setZeebeClient(final ZeebeClient zeebeClient) {
    this.zeebeClient = zeebeClient;
  }
}
