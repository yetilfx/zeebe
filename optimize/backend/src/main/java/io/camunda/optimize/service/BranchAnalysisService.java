/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.optimize.service;

import io.camunda.optimize.dto.optimize.DefinitionType;
import io.camunda.optimize.dto.optimize.query.analysis.BranchAnalysisRequestDto;
import io.camunda.optimize.dto.optimize.query.analysis.BranchAnalysisResponseDto;
import io.camunda.optimize.service.db.reader.BranchAnalysisReader;
import io.camunda.optimize.service.security.util.definition.DataSourceDefinitionAuthorizationService;
import io.camunda.optimize.service.util.ValidationHelper;
import jakarta.ws.rs.ForbiddenException;
import java.time.ZoneId;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class BranchAnalysisService {

  private final DataSourceDefinitionAuthorizationService definitionAuthorizationService;
  private final BranchAnalysisReader branchAnalysisReader;

  public BranchAnalysisResponseDto branchAnalysis(
      final String userId, final BranchAnalysisRequestDto request, final ZoneId timezone) {
    ValidationHelper.validate(request);
    if (!definitionAuthorizationService.isAuthorizedToAccessDefinition(
        userId,
        DefinitionType.PROCESS,
        request.getProcessDefinitionKey(),
        request.getTenantIds())) {
      throw new ForbiddenException(
          "Current user is not authorized to access data of the provided process definition and tenant combination");
    }

    return branchAnalysisReader.branchAnalysis(request, timezone);
  }
}