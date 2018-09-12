package org.camunda.optimize.service.es.report.util.creator.max;

import org.camunda.optimize.dto.optimize.query.report.single.SingleReportDataDto;
import org.camunda.optimize.service.es.report.util.creator.ReportDataCreator;

import static org.camunda.optimize.service.es.report.command.util.ReportConstants.DATE_UNIT_DAY;
import static org.camunda.optimize.test.util.ReportDataHelper.createMaxProcessInstanceDurationGroupByStartDateWithProcessPartReport;

public class MaxProcessInstanceDurationByStartDateWithProcessPartReportDataCreator implements ReportDataCreator {

  @Override
  public SingleReportDataDto create(String processDefinitionKey, String processDefinitionVersion, String... additional) {
    if (additional.length == 2) {
      String startFlowNodeId = additional[0];
      String endFlowNodeId = additional[1];
      return createMaxProcessInstanceDurationGroupByStartDateWithProcessPartReport(
        processDefinitionKey, processDefinitionVersion, DATE_UNIT_DAY, startFlowNodeId, endFlowNodeId);
    } else {
      String dateUnit = additional[0];
      String startFlowNodeId = additional[1];
      String endFlowNodeId = additional[2];
      return createMaxProcessInstanceDurationGroupByStartDateWithProcessPartReport(
        processDefinitionKey, processDefinitionVersion, dateUnit, startFlowNodeId, endFlowNodeId);
    }
  }
}
