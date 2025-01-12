/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.search.clients.auth;

import static io.camunda.search.clients.query.SearchQueryBuilders.and;
import static io.camunda.search.clients.query.SearchQueryBuilders.longTerms;
import static io.camunda.search.clients.query.SearchQueryBuilders.stringTerms;
import static io.camunda.zeebe.protocol.record.value.AuthorizationResourceType.PROCESS_DEFINITION;
import static io.camunda.zeebe.protocol.record.value.PermissionType.CREATE;
import static io.camunda.zeebe.protocol.record.value.PermissionType.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.camunda.search.clients.DocumentBasedSearchClient;
import io.camunda.search.clients.core.SearchQueryHit;
import io.camunda.search.clients.core.SearchQueryRequest;
import io.camunda.search.clients.core.SearchQueryResponse;
import io.camunda.search.clients.query.SearchMatchNoneQuery;
import io.camunda.search.clients.query.SearchQuery;
import io.camunda.search.clients.transformers.ServiceTransformers;
import io.camunda.search.entities.AuthorizationEntity;
import io.camunda.search.query.ProcessDefinitionQuery;
import io.camunda.search.query.SearchQueryBase;
import io.camunda.security.auth.SecurityContext;
import io.camunda.security.entity.Permission;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocumentAuthorizationQueryStrategyTest {

  @Mock private DocumentBasedSearchClient searchClient;

  private DocumentAuthorizationQueryStrategy queryStrategy;

  @BeforeEach
  void setUp() {
    queryStrategy =
        new DocumentAuthorizationQueryStrategy(
            searchClient, ServiceTransformers.newInstance(false));
  }

  @Test
  void shouldReturnRequestUnchangedWhenAuthorizationNotRequired() {
    // given
    final var originalRequest = mock(SearchQueryRequest.class);
    final var securityContext = SecurityContext.of(s -> s.withAuthentication(a -> a.user(123L)));

    // when
    final SearchQueryRequest result =
        queryStrategy.applyAuthorizationToQuery(
            originalRequest, securityContext, SearchQueryBase.class);

    // then
    assertThat(result).isSameAs(originalRequest);
  }

  @Test
  void shouldReturnRequestUnchangedWhenNoAuthentication() {
    // given
    final var originalRequest = mock(SearchQueryRequest.class);
    final var securityContext =
        SecurityContext.of(
            s -> s.withAuthorization(a -> a.resourceType(PROCESS_DEFINITION).permissionType(READ)));

    // when
    final SearchQueryRequest result =
        queryStrategy.applyAuthorizationToQuery(
            originalRequest, securityContext, ProcessDefinitionQuery.class);

    // then
    assertThat(result).isSameAs(originalRequest);
  }

  @Test
  void shouldReturnRequestUnchangedWhenAuthorizedResourceContainsWildcard() {
    // given
    final SearchQueryRequest originalRequest = mock(SearchQueryRequest.class);
    final var securityContext =
        SecurityContext.of(
            s ->
                s.withAuthentication(a -> a.user(123L))
                    .withAuthorization(
                        a -> a.permissionType(READ).resourceType(PROCESS_DEFINITION)));
    when(searchClient.findAll(any(SearchQueryRequest.class), eq(AuthorizationEntity.class)))
        .thenReturn(
            List.of(
                new AuthorizationEntity(
                    null,
                    null,
                    null,
                    List.of(
                        new Permission(READ, List.of("foo", "*")),
                        new Permission(CREATE, List.of("bar"))))));

    // when
    final SearchQueryRequest result =
        queryStrategy.applyAuthorizationToQuery(
            originalRequest, securityContext, ProcessDefinitionQuery.class);

    // then
    assertThat(result).isSameAs(originalRequest);
  }

  @Test
  void shouldReturnMatchNoneQueryWhenNoAuthorizedResourcesFound() {
    // given
    final SearchQueryRequest originalRequest = mock(SearchQueryRequest.class);
    final var securityContext =
        SecurityContext.of(
            s ->
                s.withAuthentication(a -> a.user(123L))
                    .withAuthorization(
                        a -> a.permissionType(READ).resourceType(PROCESS_DEFINITION)));
    when(searchClient.findAll(any(SearchQueryRequest.class), eq(AuthorizationEntity.class)))
        .thenReturn(List.of());

    // when
    final SearchQueryRequest result =
        queryStrategy.applyAuthorizationToQuery(
            originalRequest, securityContext, ProcessDefinitionQuery.class);

    // then
    assertThat(result.query().queryOption()).isInstanceOf(SearchMatchNoneQuery.class);
  }

  @Test
  void shouldApplyAuthorizationFilterToQuery() {
    // given
    final SearchQueryRequest originalRequest = mock(SearchQueryRequest.class);
    when(originalRequest.query()).thenReturn(mock(SearchQuery.class));
    final var securityContext =
        SecurityContext.of(
            s ->
                s.withAuthentication(a -> a.user(123L))
                    .withAuthorization(
                        a -> a.permissionType(READ).resourceType(PROCESS_DEFINITION)));
    final var authorizationsSearchQueryCaptor = ArgumentCaptor.forClass(SearchQueryRequest.class);
    when(searchClient.findAll(
            authorizationsSearchQueryCaptor.capture(), eq(AuthorizationEntity.class)))
        .thenReturn(
            List.of(
                new AuthorizationEntity(
                    null,
                    null,
                    null,
                    List.of(
                        new Permission(READ, List.of("foo")),
                        new Permission(CREATE, List.of("bar"))))));

    // when
    final SearchQueryRequest result =
        queryStrategy.applyAuthorizationToQuery(
            originalRequest, securityContext, ProcessDefinitionQuery.class);

    // then
    assertThat(result.query())
        .isEqualTo(and(originalRequest.query(), stringTerms("bpmnProcessId", List.of("foo"))));
    assertThat(authorizationsSearchQueryCaptor.getValue().query().queryOption().toSearchQuery())
        .isEqualTo(
            and(
                longTerms("ownerKey", List.of(123L)),
                stringTerms("resourceType", List.of("PROCESS_DEFINITION")),
                stringTerms("permissions.type", List.of("READ"))));
  }

  private SearchQueryResponse<AuthorizationEntity> buildSearchQueryResponse(
      final AuthorizationEntity authorizationEntity) {
    return SearchQueryResponse.of(
        r ->
            r.hits(
                List.of(
                    new SearchQueryHit.Builder<AuthorizationEntity>()
                        .source(authorizationEntity)
                        .build())));
  }
}
