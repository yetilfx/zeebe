/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.service.es.report.result.process;

import org.camunda.optimize.dto.optimize.query.report.ReportEvaluationResult;
import org.camunda.optimize.dto.optimize.query.report.single.process.SingleProcessReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.duration.ProcessDurationReportNumberResultDto;
import org.camunda.optimize.service.es.report.result.NumberResult;
import org.camunda.optimize.service.export.CSVUtils;

import javax.validation.constraints.NotNull;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SingleProcessNumberDurationReportResult
  extends ReportEvaluationResult<ProcessDurationReportNumberResultDto, SingleProcessReportDefinitionDto>
  implements NumberResult {

  public SingleProcessNumberDurationReportResult(@NotNull final ProcessDurationReportNumberResultDto reportResult,
                                                 @NotNull final SingleProcessReportDefinitionDto reportDefinition) {
    super(reportResult, reportDefinition);
  }

  @Override
  public List<String[]> getResultAsCsv(final Integer limit, final Integer offset, Set<String> excludedColumns) {
    final List<String[]> csvStrings = new LinkedList<>();
    Long result = reportResult.getData();
    csvStrings.add(
      new String[]{
        result.toString()
      });

    final String normalizedCommandKey =
      reportDefinition.getData().getView().createCommandKey().replace("-", "_");

    final String[] operations =
      new String[]{CSVUtils.mapAggregationType(reportDefinition.getData().getConfiguration().getAggregationType())};
    csvStrings.add(0, operations);
    final String[] header = new String[]{normalizedCommandKey};
    csvStrings.add(0, header);
    return csvStrings;
  }

  @Override
  public long getResultAsNumber() {
    return reportResult.getData();
  }
}
