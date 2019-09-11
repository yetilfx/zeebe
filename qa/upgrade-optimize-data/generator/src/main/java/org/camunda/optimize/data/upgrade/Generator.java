/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.data.upgrade;

import org.apache.commons.io.IOUtils;
import org.camunda.optimize.dto.engine.CredentialsDto;
import org.camunda.optimize.dto.engine.ProcessDefinitionEngineDto;
import org.camunda.optimize.dto.optimize.query.IdDto;
import org.camunda.optimize.dto.optimize.query.alert.AlertCreationDto;
import org.camunda.optimize.dto.optimize.query.alert.AlertInterval;
import org.camunda.optimize.dto.optimize.query.dashboard.DashboardDefinitionDto;
import org.camunda.optimize.dto.optimize.query.dashboard.DimensionDto;
import org.camunda.optimize.dto.optimize.query.dashboard.PositionDto;
import org.camunda.optimize.dto.optimize.query.dashboard.ReportLocationDto;
import org.camunda.optimize.dto.optimize.query.report.ReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.combined.CombinedReportDataDto;
import org.camunda.optimize.dto.optimize.query.report.combined.CombinedReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.combined.CombinedReportItemDto;
import org.camunda.optimize.dto.optimize.query.report.single.configuration.AggregationType;
import org.camunda.optimize.dto.optimize.query.report.single.filter.data.date.FixedDateFilterDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.filter.data.variable.BooleanVariableFilterDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.group.GroupByDateUnit;
import org.camunda.optimize.dto.optimize.query.report.single.process.ProcessReportDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.ProcessVisualization;
import org.camunda.optimize.dto.optimize.query.report.single.process.SingleProcessReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.ExecutedFlowNodeFilterDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.ProcessFilterDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.StartDateFilterDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.VariableFilterDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.filter.data.ExecutedFlowNodeFilterDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.group.ProcessGroupByType;
import org.camunda.optimize.dto.optimize.query.variable.VariableType;
import org.camunda.optimize.rest.providers.OptimizeObjectMapperContextResolver;
import org.camunda.optimize.test.util.ProcessReportDataBuilder;
import org.camunda.optimize.test.util.ProcessReportDataType;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.camunda.optimize.service.security.AuthCookieService.OPTIMIZE_AUTHORIZATION;
import static org.camunda.optimize.service.security.AuthCookieService.createOptimizeAuthCookieValue;

public class Generator {
  public static final String DEFAULT_USERNAME = "demo";
  private static Client client;
  private static String authCookie;
  private static String processDefinitionKey;
  private static String processDefinitionVersion;

  public static void main(String[] args) throws Exception {
    ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("optimizeDataUpgradeContext.xml");

    OptimizeObjectMapperContextResolver provider = ctx.getBean(OptimizeObjectMapperContextResolver.class);

    client = ClientBuilder.newClient().register(provider);

    validateAndStoreLicense();
    authenticateDemo();

    setDefaultProcessDefinition();

    List<String> reportIds = generateReports();
    generateDashboards(reportIds);
    generateAlerts();

    ctx.close();
    client.close();
  }

  private static void generateAlerts() {
    ProcessReportDataDto reportData = ProcessReportDataBuilder
      .createReportData()
      .setProcessDefinitionKey(processDefinitionKey)
      .setProcessDefinitionVersion(processDefinitionVersion)
      .setReportDataType(ProcessReportDataType.COUNT_PROC_INST_FREQ_GROUP_BY_NONE)
      .build();
    reportData.setVisualization(ProcessVisualization.NUMBER);

    WebTarget target = client.target("http://localhost:8090/api/report/process/single");
    List<String> reports = createAndUpdateReports(
      target,
      Collections.singletonList(reportData),
      new ArrayList<>()
    );

    String reportId = reports.get(0);

    AlertCreationDto alertCreation = prepareAlertCreation(reportId);
    createAlert(alertCreation);
  }

  private static AlertCreationDto prepareAlertCreation(String id) {
    AlertCreationDto alertCreation = new AlertCreationDto();

    alertCreation.setReportId(id);
    alertCreation.setThreshold(700L);
    alertCreation.setEmail("foo@gmail.bar");
    alertCreation.setName("alertFoo");
    alertCreation.setThresholdOperator("<");
    alertCreation.setFixNotification(true);

    AlertInterval interval = new AlertInterval();
    interval.setValue(17);
    interval.setUnit("Minutes");

    alertCreation.setCheckInterval(interval);
    alertCreation.setReminder(interval);

    return alertCreation;
  }

  private static void createAlert(AlertCreationDto alertCreation) {
    WebTarget target = client.target("http://localhost:8090/api/alert");
    target.request()
      .cookie(OPTIMIZE_AUTHORIZATION, authCookie)
      .post(Entity.json(alertCreation));
  }

  private static void setDefaultProcessDefinition() {
    ProcessDefinitionEngineDto processDefinitionEngineDto = getProcessDefinitions().get(0);
    processDefinitionKey = processDefinitionEngineDto.getKey();
    processDefinitionVersion = processDefinitionEngineDto.getVersionAsString();
  }

  private static void generateDashboards(List<String> reportIds) {
    DashboardDefinitionDto dashboard = prepareDashboard(reportIds);

    WebTarget target = client.target("http://localhost:8090/api/dashboard");

    String dashboardId = createEmptyDashboard(target);
    createEmptyDashboard(target);

    target = client.target("http://localhost:8090/api/dashboard/" + dashboardId);
    target.request()
      .cookie(OPTIMIZE_AUTHORIZATION, authCookie)
      .put(Entity.json(dashboard));
  }

  private static String createEmptyDashboard(WebTarget target) {
    return target.request()
      .cookie(OPTIMIZE_AUTHORIZATION, authCookie)
      .post(Entity.json(""))
      .readEntity(IdDto.class).getId();
  }

  private static DashboardDefinitionDto prepareDashboard(List<String> reportIds) {
    List<ReportLocationDto> reportLocations = reportIds.stream().map(reportId -> {
      ReportLocationDto report = new ReportLocationDto();
      report.setId(reportId);

      PositionDto position = new PositionDto();
      position.setX((reportIds.indexOf(reportId) % 3) * 6);
      position.setY((reportIds.indexOf(reportId) / 3) * 4);
      report.setPosition(position);

      DimensionDto dimensions = new DimensionDto();
      dimensions.setHeight(4);
      dimensions.setWidth(6);
      report.setDimensions(dimensions);

      return report;
    }).collect(Collectors.toList());

    DashboardDefinitionDto dashboard = new DashboardDefinitionDto();
    dashboard.setReports(reportLocations);

    return dashboard;
  }

  private static void validateAndStoreLicense() throws IOException {
    String license = readFileToString();

    client.target("http://localhost:8090/api/license/validate-and-store")
      .request().post(Entity.entity(license, MediaType.TEXT_PLAIN));
  }

  private static void authenticateDemo() {
    CredentialsDto credentials = new CredentialsDto();
    credentials.setUsername(DEFAULT_USERNAME);
    credentials.setPassword(DEFAULT_USERNAME);

    Response response = client.target("http://localhost:8090/api/authentication")
      .request().post(Entity.json(credentials));

    authCookie = createOptimizeAuthCookieValue(response.readEntity(String.class));
  }

  private static String readFileToString() throws IOException {
    return IOUtils.toString(Generator.class.getResourceAsStream("/ValidTestLicense.txt"), Charset.forName("UTF-8"));
  }

  private static List<String> generateReports() {
    WebTarget target = client.target("http://localhost:8090/api/report/process/single");

    List<ProcessReportDataDto> reportDefinitions = createDifferentReports();

    List<ProcessFilterDto> filters = prepareFilters();

    return createAndUpdateReports(target, reportDefinitions, filters);
  }

  private static List<String> createAndUpdateReports(WebTarget target, List<ProcessReportDataDto> reportDefinitions,
                                                     List<ProcessFilterDto> filters) {
    CombinedReportDataDto combinedReportData = new CombinedReportDataDto();
    combinedReportData.setReports(new ArrayList<>());
    List<String> reportIds = new ArrayList<>();
    for (ProcessReportDataDto reportData : reportDefinitions) {
      String id = createEmptySingleProcessReport(target);
      reportIds.add(id);
      // there are two reports expected matching this criteria
      if (reportData.getVisualization().equals(ProcessVisualization.BAR)
        && reportData.getGroupBy().getType().equals(ProcessGroupByType.START_DATE)) {
        combinedReportData.getReports().add(new CombinedReportItemDto(id));
      }
      reportData.setFilter(filters);

      SingleProcessReportDefinitionDto reportUpdate = prepareReportUpdate(reportData, id);
      updateReport(id, reportUpdate);
    }
    if (!combinedReportData.getReports().isEmpty()) {
      reportIds.add(createCombinedReport(combinedReportData));
    }
    return reportIds;
  }

  private static String createCombinedReport(CombinedReportDataDto data) {
    WebTarget target = client.target("http://localhost:8090/api/report/process/combined");
    Response response = target.request()
      .cookie(OPTIMIZE_AUTHORIZATION, authCookie)
      .post(Entity.json(new CombinedReportDefinitionDto(data)));
    String id = response.readEntity(IdDto.class).getId();

    CombinedReportDefinitionDto combinedReportDefinition = new CombinedReportDefinitionDto();
    combinedReportDefinition.setData(data);
    updateReport(id, combinedReportDefinition);

    return id;
  }

  private static SingleProcessReportDefinitionDto prepareReportUpdate(ProcessReportDataDto reportData, String id) {
    SingleProcessReportDefinitionDto report = new SingleProcessReportDefinitionDto();
    report.setData(reportData);
    report.setId(id);
    report.setName(reportData.createCommandKey());
    return report;
  }

  private static List<ProcessFilterDto> prepareFilters() {
    List<ProcessFilterDto> filters = new ArrayList<>();

    StartDateFilterDto dateFilter = prepareStartDateFilter();
    VariableFilterDto variableFilter = prepareBooleanVariableFilter();
    ExecutedFlowNodeFilterDto executedFlowNodeFilter = prepareFlowNodeFilter();

    filters.add(dateFilter);
    filters.add(variableFilter);
    filters.add(executedFlowNodeFilter);
    return filters;
  }

  private static StartDateFilterDto prepareStartDateFilter() {

    FixedDateFilterDataDto dateFilterData = new FixedDateFilterDataDto();
    dateFilterData.setStart(OffsetDateTime.now());
    dateFilterData.setEnd(OffsetDateTime.now().plusDays(1L));
    return new StartDateFilterDto(dateFilterData);
  }

  private static ExecutedFlowNodeFilterDto prepareFlowNodeFilter() {
    ExecutedFlowNodeFilterDto executedFlowNodeFilter = new ExecutedFlowNodeFilterDto();
    ExecutedFlowNodeFilterDataDto executedFlowNodeFilterData = new ExecutedFlowNodeFilterDataDto();

    executedFlowNodeFilterData.setOperator("in");

    List<String> values = new ArrayList<>();
    values.add("flowNode1");
    values.add("flowNode2");
    executedFlowNodeFilterData.setValues(values);

    executedFlowNodeFilter.setData(executedFlowNodeFilterData);
    return executedFlowNodeFilter;
  }

  private static VariableFilterDto prepareBooleanVariableFilter() {
    VariableFilterDto variableFilter = new VariableFilterDto();

    BooleanVariableFilterDataDto booleanVariableFilterDataDto = new BooleanVariableFilterDataDto("true");
    booleanVariableFilterDataDto.setName("var");

    variableFilter.setData(booleanVariableFilterDataDto);

    return variableFilter;
  }

  private static List<ProcessReportDataDto> createDifferentReports() {
    List<ProcessReportDataDto> reportDefinitions = new ArrayList<>();


    reportDefinitions.add(
      ProcessReportDataBuilder
        .createReportData()
        .setProcessDefinitionKey(processDefinitionKey)
        .setProcessDefinitionVersion(processDefinitionVersion)
        .setReportDataType(ProcessReportDataType.PROC_INST_DUR_GROUP_BY_NONE)
        .build()
    );
    reportDefinitions.add(
      ProcessReportDataBuilder
        .createReportData()
        .setProcessDefinitionKey(processDefinitionKey)
        .setProcessDefinitionVersion(processDefinitionVersion)
        .setReportDataType(ProcessReportDataType.FLOW_NODE_DUR_GROUP_BY_FLOW_NODE)
        .build()
    );
    final ProcessReportDataDto maxFlowNodeDurationGroupByFlowNodeHeatmapReport = ProcessReportDataBuilder
        .createReportData()
        .setProcessDefinitionKey(processDefinitionKey)
        .setProcessDefinitionVersion(processDefinitionVersion)
        .setReportDataType(ProcessReportDataType.FLOW_NODE_DUR_GROUP_BY_FLOW_NODE)
        .build();
    maxFlowNodeDurationGroupByFlowNodeHeatmapReport.getConfiguration().setAggregationType(AggregationType.MAX);
    reportDefinitions.add(maxFlowNodeDurationGroupByFlowNodeHeatmapReport);

    ProcessReportDataDto reportDataDto = ProcessReportDataBuilder
        .createReportData()
        .setProcessDefinitionKey(processDefinitionKey)
        .setProcessDefinitionVersion(processDefinitionVersion)
        .setReportDataType(ProcessReportDataType.PROC_INST_DUR_GROUP_BY_START_DATE)
        .setDateInterval(GroupByDateUnit.DAY)
        .build();
    reportDataDto.setVisualization(ProcessVisualization.BAR);
    reportDefinitions.add(reportDataDto);

    reportDataDto = ProcessReportDataBuilder
        .createReportData()
        .setProcessDefinitionKey(processDefinitionKey)
        .setProcessDefinitionVersion(processDefinitionVersion)
        .setReportDataType(ProcessReportDataType.PROC_INST_DUR_GROUP_BY_START_DATE)
        .setDateInterval(GroupByDateUnit.DAY)
      .build();
    reportDataDto.setVisualization(ProcessVisualization.BAR);
    reportDefinitions.add(reportDataDto);

    reportDataDto = ProcessReportDataBuilder
      .createReportData()
      .setProcessDefinitionKey(processDefinitionKey)
      .setProcessDefinitionVersion(processDefinitionVersion)
      .setReportDataType(ProcessReportDataType.PROC_INST_DUR_GROUP_BY_VARIABLE_WITH_PART)
      .setVariableType(VariableType.STRING)
      .setVariableName("var")
      .setStartFlowNodeId("startNode")
      .setEndFlowNodeId("endNode")
      .build();
    reportDataDto.setVisualization(ProcessVisualization.BAR);
    reportDefinitions.add(reportDataDto);

    return reportDefinitions;
  }

  private static String createEmptySingleProcessReport(WebTarget target) {
    Response response = target.request()
      .cookie(OPTIMIZE_AUTHORIZATION, authCookie)
      .post(Entity.json(new SingleProcessReportDefinitionDto()));
    return response.readEntity(IdDto.class).getId();
  }

  private static void

  updateReport(String id, ReportDefinitionDto report) {
    WebTarget target = client.target("http://localhost:8090/api/report/process/" +
                                       (report instanceof SingleProcessReportDefinitionDto
                                         ? "single" : "combined") + "/" + id);
    target.request()
      .cookie(OPTIMIZE_AUTHORIZATION, authCookie)
      .put(Entity.json(report));
  }

  private static List<ProcessDefinitionEngineDto> getProcessDefinitions() {
    WebTarget target = client.target("http://localhost:8080/engine-rest/process-definition");
    Response response = target.request()
      .get();

    return response.readEntity(new GenericType<List<ProcessDefinitionEngineDto>>() {
    });
  }
}
