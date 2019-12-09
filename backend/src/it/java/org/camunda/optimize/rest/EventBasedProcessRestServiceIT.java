/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.rest;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.camunda.optimize.AbstractIT;
import org.camunda.optimize.dto.optimize.DefinitionOptimizeDto;
import org.camunda.optimize.dto.optimize.ProcessDefinitionOptimizeDto;
import org.camunda.optimize.dto.optimize.query.event.EventMappingDto;
import org.camunda.optimize.dto.optimize.query.event.EventProcessDefinitionDto;
import org.camunda.optimize.dto.optimize.query.event.EventProcessMappingDto;
import org.camunda.optimize.dto.optimize.query.event.EventProcessState;
import org.camunda.optimize.dto.optimize.query.event.EventTypeDto;
import org.camunda.optimize.dto.optimize.rest.ErrorResponseDto;
import org.camunda.optimize.exception.OptimizeIntegrationTestException;
import org.camunda.optimize.service.security.util.LocalDateUtil;
import org.camunda.optimize.service.util.IdGenerator;
import org.camunda.optimize.util.FileReaderUtil;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.camunda.optimize.upgrade.es.ElasticsearchConstants.EVENT_PROCESS_DEFINITION_INDEX_NAME;

public class EventBasedProcessRestServiceIT extends AbstractIT {

  private static final String FULL_PROCESS_DEFINITION_XML_PATH = "/bpmn/leadQualification.bpmn";
  private static final String VALID_SCRIPT_TASK_ID = "ScriptTask_1";
  private static final String VALID_USER_TASK_ID = "UserTask_1d75hsy";
  private static final String VALID_SERVICE_TASK_ID = "ServiceTask_0j2w5af";

  @Test
  public void createEventProcessMappingWithoutAuthorization() {
    // when
    Response response = eventProcessClient
      .createCreateEventProcessMappingRequest(createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH))
      .withoutAuthentication()
      .execute();

    // then the status code is not authorized
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void createEventProcessMapping() {
    // when
    Response response = eventProcessClient
      .createCreateEventProcessMappingRequest(createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH))
      .execute();

    // then
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  public void createEventProcessMappingWithEventMappingCombinations() {
    // given event mappings with IDs existing in XML
    Map<String, EventMappingDto> eventMappings = new HashMap<>();
    eventMappings.put(VALID_SCRIPT_TASK_ID, createEventMappingsDto(createMappedEventDto(), createMappedEventDto()));
    eventMappings.put(VALID_USER_TASK_ID, createEventMappingsDto(createMappedEventDto(), null));
    eventMappings.put(VALID_SERVICE_TASK_ID, createEventMappingsDto(null, createMappedEventDto()));
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      eventMappings,
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );

    // when
    Response response = eventProcessClient.createCreateEventProcessMappingRequest(eventProcessMappingDto).execute();

    // then
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  public void createEventProcessMappingWithEventMappingIdNotExistInXml() {
    // given event mappings with ID does not exist in XML
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap("invalid_Id", createEventMappingsDto(createMappedEventDto(), createMappedEventDto())),
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );

    // when
    Response response = eventProcessClient.createCreateEventProcessMappingRequest(eventProcessMappingDto).execute();

    // then
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
  }

  @Test
  public void createEventProcessMappingWithEventMappingsAndXmlNotPresent() {
    // given event mappings but no XML provided
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap("some_task_id", createEventMappingsDto(createMappedEventDto(), createMappedEventDto())),
      "process name",
      null
    );

    // when
    Response response = eventProcessClient.createCreateEventProcessMappingRequest(eventProcessMappingDto).execute();

    // then
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
  }

  @ParameterizedTest(name = "Invalid mapped event: {0}")
  @MethodSource("createInvalidMappedEventDtos")
  public void createEventProcessMappingWithInvalidEventMappings(EventTypeDto invalidEventTypeDto) {
    // given event mappings but mapped events have fields missing
    invalidEventTypeDto.setGroup(null);
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap("some_task_id", createEventMappingsDto(invalidEventTypeDto, createMappedEventDto())),
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );

    // when
    Response response = eventProcessClient.createCreateEventProcessMappingRequest(eventProcessMappingDto).execute();

    // then a bad request exception is thrown
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
  }

  @Test
  public void getEventProcessMappingWithoutAuthorization() {
    // when
    Response response = eventProcessClient.createGetEventProcessMappingRequest(IdGenerator.getNextId())
      .withoutAuthentication()
      .execute();

    // then the status code is not authorized
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void getEventProcessMappingWithId() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    OffsetDateTime now = OffsetDateTime.parse("2019-11-25T10:00:00+01:00");
    LocalDateUtil.setCurrentTime(now);
    String expectedId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    EventProcessMappingDto actual = eventProcessClient.getEventProcessMapping(expectedId);

    // then the report is returned with expect
    assertThat(actual.getId()).isEqualTo(expectedId);
    assertThat(actual).isEqualToIgnoringGivenFields(
      eventProcessMappingDto,
      EventProcessMappingDto.Fields.id,
      EventProcessMappingDto.Fields.lastModified,
      EventProcessMappingDto.Fields.lastModifier,
      EventProcessMappingDto.Fields.state
    );
    assertThat(actual.getLastModified()).isEqualTo(now);
    assertThat(actual.getLastModifier()).isEqualTo("demo");
    assertThat(actual.getState()).isEqualTo(EventProcessState.MAPPED);
  }

  @Test
  public void getEventProcessMappingWithId_unmappedState() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      null, "process name", FULL_PROCESS_DEFINITION_XML_PATH
    );
    String expectedId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    EventProcessMappingDto actual = eventProcessClient.getEventProcessMapping(expectedId);

    // then the report is returned in state unmapped
    assertThat(actual.getState()).isEqualTo(EventProcessState.UNMAPPED);
  }

  @Test
  public void getEventProcessMappingWithIdNotExists() {
    // when
    Response response = eventProcessClient
      .createGetEventProcessMappingRequest(IdGenerator.getNextId()).execute();

    // then the report is returned with expect
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_NOT_FOUND);
  }

  @Test
  public void getAllEventProcessMappingWithoutAuthorization() {
    // when
    Response response = embeddedOptimizeExtension
      .getRequestExecutor()
      .withoutAuthentication()
      .buildGetAllEventProcessMappingsRequests()
      .execute();

    // then the status code is not authorized
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void getAllEventProcessMappings() {
    // given
    final Map<String, EventMappingDto> firstProcessMappings = Collections.singletonMap(
      VALID_SERVICE_TASK_ID,
      createEventMappingsDto(createMappedEventDto(), createMappedEventDto())
    );
    EventProcessMappingDto firstExpectedDto = createEventProcessMappingDtoWithMappings(
      firstProcessMappings,
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );
    OffsetDateTime now = OffsetDateTime.parse("2019-11-25T10:00:00+01:00");
    LocalDateUtil.setCurrentTime(now);
    String firstExpectedId = eventProcessClient.createEventProcessMapping(firstExpectedDto);
    EventProcessMappingDto secondExpectedDto = createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH);
    String secondExpectedId = eventProcessClient.createEventProcessMapping(secondExpectedDto);

    // when
    List<EventProcessMappingDto> response = eventProcessClient.getAllEventProcessMappings();

    // then the response contains expected processes with xml omitted
    assertThat(response).extracting(
      EventProcessMappingDto.Fields.id, EventProcessMappingDto.Fields.name,
      EventProcessMappingDto.Fields.xml, EventProcessMappingDto.Fields.lastModified,
      EventProcessMappingDto.Fields.lastModifier, EventProcessMappingDto.Fields.mappings,
      EventProcessMappingDto.Fields.state
    )
      .containsExactlyInAnyOrder(
        tuple(
          firstExpectedId,
          firstExpectedDto.getName(),
          null,
          now,
          "demo",
          firstProcessMappings,
          EventProcessState.MAPPED
        ),
        tuple(secondExpectedId, secondExpectedDto.getName(), null, now, "demo", null, EventProcessState.UNMAPPED)
      );
  }

  @Test
  public void updateEventProcessMappingWithoutAuthorization() {
    // when
    EventProcessMappingDto updateDto = createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH);
    Response response = eventProcessClient
      .createUpdateEventProcessMappingRequest("doesNotMatter", updateDto)
      .withoutAuthentication()
      .execute();

    // then the status code is not authorized
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void updateEventProcessMappingWithMappingsAdded() {
    // given
    OffsetDateTime createdTime = OffsetDateTime.parse("2019-11-24T18:00:00+01:00");
    LocalDateUtil.setCurrentTime(createdTime);
    String storedEventProcessMappingId = eventProcessClient.createEventProcessMapping(createEventProcessMappingDto(
      FULL_PROCESS_DEFINITION_XML_PATH));

    // when
    EventProcessMappingDto updateDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap(
        VALID_SERVICE_TASK_ID,
        createEventMappingsDto(createMappedEventDto(), createMappedEventDto())
      ),
      "new process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );
    OffsetDateTime updatedTime = OffsetDateTime.parse("2019-11-25T10:00:00+01:00");
    LocalDateUtil.setCurrentTime(updatedTime);
    Response response = eventProcessClient
      .createUpdateEventProcessMappingRequest(storedEventProcessMappingId, updateDto).execute();

    // then the update response code is correct
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_NO_CONTENT);

    // then the fields have been updated
    EventProcessMappingDto storedDto = eventProcessClient.getEventProcessMapping(storedEventProcessMappingId);
    assertThat(storedDto)
      .isEqualToIgnoringGivenFields(
        updateDto,
        EventProcessMappingDto.Fields.id,
        EventProcessMappingDto.Fields.lastModified,
        EventProcessMappingDto.Fields.lastModifier,
        EventProcessMappingDto.Fields.state
      )
      .extracting("id").isEqualTo(storedEventProcessMappingId);
    assertThat(storedDto.getLastModified()).isEqualTo(updatedTime);
    assertThat(storedDto.getLastModifier()).isEqualTo("demo");
  }

  @Test
  public void updateEventProcessMappingWithIdNotExists() {
    // when
    Response response = eventProcessClient.createUpdateEventProcessMappingRequest(
      "doesNotExist",
      createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH)
    ).execute();

    // then the report is returned with expect
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_NOT_FOUND);
  }

  @Test
  public void updateEventProcessMappingWithEventMappingIdNotExistInXml() {
    // given
    String storedEventProcessMappingId = eventProcessClient.createEventProcessMapping(
      createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH)
    );

    // when update event mappings with ID does not exist in XML
    EventProcessMappingDto updateDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap("invalid_Id", createEventMappingsDto(createMappedEventDto(), createMappedEventDto())),
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );
    Response response = eventProcessClient
      .createUpdateEventProcessMappingRequest(storedEventProcessMappingId, updateDto).execute();

    // then the update response code is correct
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
  }

  @ParameterizedTest(name = "Invalid mapped event: {0}")
  @MethodSource("createInvalidMappedEventDtos")
  public void updateEventProcessMappingWithInvalidEventMappings(EventTypeDto invalidEventTypeDto) {
    // given existing event based process
    String storedEventProcessMappingId = eventProcessClient.createEventProcessMapping(
      createEventProcessMappingDto(FULL_PROCESS_DEFINITION_XML_PATH)
    );

    // when update event mappings with a mapped event with missing fields
    EventProcessMappingDto updateDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap(
        VALID_SERVICE_TASK_ID,
        createEventMappingsDto(invalidEventTypeDto, createMappedEventDto())
      ),
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );
    Response response = eventProcessClient
      .createUpdateEventProcessMappingRequest(storedEventProcessMappingId, updateDto)
      .execute();

    // then a bad request exception is thrown
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
  }

  @Test
  public void updateEventProcessMappingWithEventMappingAndNoXmlPresent() {
    // given
    String storedEventProcessMappingId = eventProcessClient.createEventProcessMapping(
      createEventProcessMappingDto(null)
    );

    // when update event mappings and no XML present
    EventProcessMappingDto updateDto = createEventProcessMappingDtoWithMappings(
      Collections.singletonMap("some_task_id", createEventMappingsDto(createMappedEventDto(), createMappedEventDto())),
      "process name",
      null
    );
    Response response = eventProcessClient
      .createUpdateEventProcessMappingRequest(storedEventProcessMappingId, updateDto)
      .execute();

    // then the update response code is correct
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
  }

  @Test
  public void publishMappedEventProcessMapping() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    LocalDateUtil.setCurrentTime(OffsetDateTime.now());
    eventProcessClient.publishEventProcessMapping(eventProcessId);

    final EventProcessMappingDto storedEventProcessMapping = eventProcessClient.getEventProcessMapping(
      eventProcessId
    );

    // then
    assertThat(storedEventProcessMapping.getState()).isEqualTo(EventProcessState.PUBLISH_PENDING);
    assertThat(storedEventProcessMapping.getPublishingProgress()).isEqualTo(0.0D);

    assertThat(getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)).get()
      .isEqualToIgnoringGivenFields(
        EventProcessDefinitionDto.eventProcessBuilder()
          .id(storedEventProcessMapping.getId())
          .key(storedEventProcessMapping.getId())
          .name(storedEventProcessMapping.getName())
          .version("1")
          .bpmn20Xml(storedEventProcessMapping.getXml())
          .flowNodeNames(Collections.emptyMap())
          .userTaskNames(Collections.emptyMap())
          .createdDateTime(LocalDateUtil.getCurrentDateTime())
          .build(),
        ProcessDefinitionOptimizeDto.Fields.flowNodeNames,
        ProcessDefinitionOptimizeDto.Fields.userTaskNames
      )
      .satisfies(definitionDto -> {
        assertThat(definitionDto.getFlowNodeNames()).isNotEmpty();
        assertThat(definitionDto.getUserTaskNames()).isNotEmpty();
      });
  }

  @Test
  public void publishUnpublishedChangesEventProcessMapping() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    eventProcessClient.publishEventProcessMapping(eventProcessId);

    final EventProcessMappingDto updateDto = createEventProcessMappingDtoWithMappings(
      eventProcessMappingDto.getMappings(),
      "new process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );
    eventProcessClient.updateEventProcessMapping(eventProcessId, updateDto);

    LocalDateUtil.setCurrentTime(OffsetDateTime.now().plusSeconds(1));
    eventProcessClient.publishEventProcessMapping(eventProcessId);

    final EventProcessMappingDto republishedEventProcessMapping = eventProcessClient.getEventProcessMapping(
      eventProcessId
    );

    // then
    assertThat(republishedEventProcessMapping.getState()).isEqualTo(EventProcessState.PUBLISH_PENDING);
    assertThat(republishedEventProcessMapping.getPublishingProgress()).isEqualTo(0.0D);

    assertThat(getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)).get()
      .hasFieldOrPropertyWithValue(DefinitionOptimizeDto.Fields.name, updateDto.getName())
      .hasFieldOrPropertyWithValue(DefinitionOptimizeDto.Fields.version, "1")
      .hasFieldOrPropertyWithValue(ProcessDefinitionOptimizeDto.Fields.bpmn20Xml, updateDto.getXml())
      .hasFieldOrPropertyWithValue(
        EventProcessDefinitionDto.Fields.createdDateTime,
        LocalDateUtil.getCurrentDateTime()
      )
      .satisfies(definitionDto -> {
        assertThat(definitionDto.getFlowNodeNames()).isNotEmpty();
        assertThat(definitionDto.getUserTaskNames()).isNotEmpty();
      });
  }

  @NonNull
  private OffsetDateTime getPublishedDateForEventProcessMappingOrFail(final String eventProcessId) {
    return getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)
      .orElseThrow(() -> new OptimizeIntegrationTestException("Failed reading first publish date"))
      .getCreatedDateTime();
  }

  @Test
  public void publishUnmappedEventProcessMapping_fails() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      null, "unmapped", FULL_PROCESS_DEFINITION_XML_PATH
    );
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    final ErrorResponseDto errorResponse = eventProcessClient
      .createPublishEventProcessMappingRequest(eventProcessId)
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    final EventProcessMappingDto actual = eventProcessClient.getEventProcessMapping(eventProcessId);

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo("invalidEventProcessState");

    assertThat(actual.getState()).isEqualTo(EventProcessState.UNMAPPED);
    assertThat(actual.getPublishingProgress()).isEqualTo(null);

    assertThat(getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)).isEmpty();
  }

  @Test
  public void publishPublishPendingEventProcessMapping_fails() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    eventProcessClient.publishEventProcessMapping(eventProcessId);
    final OffsetDateTime firstPublishDate = getPublishedDateForEventProcessMappingOrFail(eventProcessId);

    // when
    final ErrorResponseDto errorResponse = eventProcessClient
      .createPublishEventProcessMappingRequest(eventProcessId)
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    final EventProcessMappingDto actual = eventProcessClient.getEventProcessMapping(eventProcessId);

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo("invalidEventProcessState");

    assertThat(actual.getState()).isEqualTo(EventProcessState.PUBLISH_PENDING);
    assertThat(actual.getPublishingProgress()).isEqualTo(0.0D);

    assertThat(getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)).get()
      .hasFieldOrPropertyWithValue(EventProcessDefinitionDto.Fields.createdDateTime, firstPublishDate);
  }

  @Test
  public void publishedEventProcessMapping_failsIfNotExists() {
    // given

    // when
    final ErrorResponseDto errorResponse = eventProcessClient
      .createPublishEventProcessMappingRequest("notExistingId")
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_NOT_FOUND);

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo("notFoundError");
  }

  @Test
  public void cancelPublishPendingEventProcessMapping() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    eventProcessClient.publishEventProcessMapping(eventProcessId);

    // when
    eventProcessClient.cancelPublishEventProcessMapping(eventProcessId);

    final EventProcessMappingDto actual = eventProcessClient.getEventProcessMapping(eventProcessId);

    // then
    assertThat(actual.getState()).isEqualTo(EventProcessState.MAPPED);
    assertThat(actual.getPublishingProgress()).isEqualTo(null);

    assertThat(getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)).isEmpty();
  }

  @Test
  public void cancelPublishUnmappedEventProcessMapping_fails() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithMappings(
      null, "unmapped", FULL_PROCESS_DEFINITION_XML_PATH
    );
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    final ErrorResponseDto errorResponse = eventProcessClient
      .createPublishEventProcessMappingRequest(eventProcessId)
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo("invalidEventProcessState");
  }

  @Test
  public void cancelPublishMappedEventProcessMapping_fails() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    // when
    final ErrorResponseDto errorResponse = eventProcessClient
      .createCancelPublishEventProcessMappingRequest(eventProcessId)
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_BAD_REQUEST);

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo("invalidEventProcessState");
  }

  @Test
  public void cancelPublishedEventProcessMapping_failsIfNotExists() {
    // given

    // when
    final ErrorResponseDto errorResponse = eventProcessClient
      .createCancelPublishEventProcessMappingRequest("notExistingId")
      .execute(ErrorResponseDto.class, HttpServletResponse.SC_NOT_FOUND);

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo("notFoundError");
  }

  @Test
  public void deleteEventProcessMappingWithoutAuthorization() {
    // when
    Response response = eventProcessClient
      .createDeleteEventProcessMappingRequest("doesNotMatter")
      .withoutAuthentication()
      .execute();

    // then the status code is not authorized
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void deleteEventProcessMapping() {
    // given
    String storedEventProcessMappingId = eventProcessClient.createEventProcessMapping(createEventProcessMappingDto(
      FULL_PROCESS_DEFINITION_XML_PATH));

    // when
    Response response = eventProcessClient
      .createDeleteEventProcessMappingRequest(storedEventProcessMappingId).execute();

    // then the delete response code is correct
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_NO_CONTENT);

    // then the process no longer exists
    Response getResponse = eventProcessClient
      .createGetEventProcessMappingRequest(storedEventProcessMappingId).execute();
    assertThat(getResponse.getStatus()).isEqualTo(HttpServletResponse.SC_NOT_FOUND);
  }

  @Test
  public void deletePublishedEventProcessMapping() {
    // given
    EventProcessMappingDto eventProcessMappingDto = createEventProcessMappingDtoWithSimpleMappings();
    String eventProcessId = eventProcessClient.createEventProcessMapping(eventProcessMappingDto);

    eventProcessClient.publishEventProcessMapping(eventProcessId);

    // when
    eventProcessClient.deleteEventProcessMapping(eventProcessId);

    // then
    assertThat(getEventProcessMappingDefinitionFromElasticsearch(eventProcessId)).isEmpty();
  }

  @Test
  public void deleteEventProcessMappingNotExists() {
    // when
    Response response = eventProcessClient
      .createDeleteEventProcessMappingRequest("doesNotMatter")
      .execute();

    // then
    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_NOT_FOUND);
  }

  @SneakyThrows
  private Optional<EventProcessDefinitionDto> getEventProcessMappingDefinitionFromElasticsearch(final String eventProcessId) {
    final GetResponse getEventProcessDefinitionResponse = elasticSearchIntegrationTestExtension.getOptimizeElasticClient()
      .get(new GetRequest(EVENT_PROCESS_DEFINITION_INDEX_NAME).id(eventProcessId), RequestOptions.DEFAULT);
    return Optional.ofNullable(getEventProcessDefinitionResponse.getSourceAsString())
      .map(value -> {
        try {
          return embeddedOptimizeExtension.getObjectMapper().readValue(value, EventProcessDefinitionDto.class);
        } catch (IOException e) {
          throw new OptimizeIntegrationTestException(e);
        }
      });
  }

  private EventProcessMappingDto createEventProcessMappingDtoWithSimpleMappings() {
    return createEventProcessMappingDtoWithMappings(
      Collections.singletonMap(
        VALID_SERVICE_TASK_ID,
        createEventMappingsDto(createMappedEventDto(), createMappedEventDto())
      ),
      "process name",
      FULL_PROCESS_DEFINITION_XML_PATH
    );
  }

  private EventProcessMappingDto createEventProcessMappingDto(final String xmlPath) {
    return createEventProcessMappingDto(null, xmlPath);
  }

  private EventProcessMappingDto createEventProcessMappingDto(final String name, final String xmlPath) {
    return createEventProcessMappingDtoWithMappings(null, name, xmlPath);
  }

  @SneakyThrows
  public EventProcessMappingDto createEventProcessMappingDtoWithMappings(
    final Map<String, EventMappingDto> flowNodeEventMappingsDto,
    final String name, final String xmlPath) {
    return EventProcessMappingDto.builder()
      .name(Optional.ofNullable(name).orElse(RandomStringUtils.randomAlphanumeric(10)))
      .mappings(flowNodeEventMappingsDto)
      .xml(Optional.ofNullable(xmlPath).map(FileReaderUtil::readFile).orElse(null))
      .build();
  }

  private EventMappingDto createEventMappingsDto(EventTypeDto startEventDto, EventTypeDto endEventDto) {
    return EventMappingDto.builder()
      .start(startEventDto)
      .end(endEventDto)
      .build();
  }

  private static Stream<EventTypeDto> createInvalidMappedEventDtos() {
    return Stream.of(
      EventTypeDto.builder()
        .group(null)
        .source(IdGenerator.getNextId())
        .eventName(IdGenerator.getNextId())
        .build(),
      EventTypeDto.builder()
        .group(IdGenerator.getNextId())
        .source(null)
        .eventName(IdGenerator.getNextId())
        .build(),
      EventTypeDto.builder()
        .group(IdGenerator.getNextId())
        .source(IdGenerator.getNextId())
        .eventName(null)
        .build()
    );
  }

  private static EventTypeDto createMappedEventDto() {
    return EventTypeDto.builder()
      .group(IdGenerator.getNextId())
      .source(IdGenerator.getNextId())
      .eventName(IdGenerator.getNextId())
      .build();
  }

}
