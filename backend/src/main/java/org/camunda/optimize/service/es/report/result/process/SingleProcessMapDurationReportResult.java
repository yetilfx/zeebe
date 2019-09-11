/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.service.es.report.result.process;

import org.camunda.optimize.dto.optimize.query.report.ReportEvaluationResult;
import org.camunda.optimize.dto.optimize.query.report.single.process.ProcessReportDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.SingleProcessReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.duration.ProcessDurationReportMapResultDto;
import org.camunda.optimize.service.export.CSVUtils;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

public class SingleProcessMapDurationReportResult
  extends ReportEvaluationResult<ProcessDurationReportMapResultDto, SingleProcessReportDefinitionDto> {

  public SingleProcessMapDurationReportResult(@NotNull final ProcessDurationReportMapResultDto reportResult,
                                              @NotNull final SingleProcessReportDefinitionDto reportDefinition) {
    super(reportResult, reportDefinition);
  }

  @Override
  public List<String[]> getResultAsCsv(final Integer limit, final Integer offset, Set<String> excludedColumns) {
    final List<String[]> csvStrings = CSVUtils.map(reportResult.getData(), limit, offset);
    final ProcessReportDataDto reportData = reportDefinition.getData();

    final String normalizedCommandKey =
      reportData.getView().createCommandKey().replace("-", "_");
    final String[] operations =
      new String[]{"", CSVUtils.mapAggregationType(reportData.getConfiguration().getAggregationType())};
    csvStrings.add(0, operations);
    final String[] header =
      new String[]{reportData.getGroupBy().toString(), normalizedCommandKey};
    csvStrings.add(0, header);
    return csvStrings;
  }

}
