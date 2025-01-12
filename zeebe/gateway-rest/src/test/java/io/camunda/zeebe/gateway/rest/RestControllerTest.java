/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.gateway.rest;

import io.camunda.security.auth.Authentication;
import java.util.function.Function;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.test.web.reactive.server.WebTestClient.ResponseSpec;

@TestPropertySource(
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration"
    })
public abstract class RestControllerTest {

  @Autowired protected WebTestClient webClient;

  public ResponseSpec withMultiTenancy(
      final String tenantId, final Function<WebTestClient, ResponseSpec> function) {
    try (final MockedStatic<RequestMapper> mockRequestMapper =
        Mockito.mockStatic(RequestMapper.class, Mockito.CALLS_REAL_METHODS)) {
      mockRequestMapper
          .when(RequestMapper::getAuthentication)
          .thenReturn(
              Authentication.of(a -> a.user(123L).group(456L).tenant(tenantId).token("token")));
      return function.apply(webClient);
    }
  }
}
