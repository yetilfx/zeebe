/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.spring.client.annotation.processor;

import static org.springframework.util.ReflectionUtils.doWithMethods;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.customizer.ZeebeWorkerValueCustomizer;
import io.camunda.zeebe.spring.client.annotation.value.ZeebeWorkerValue;
import io.camunda.zeebe.spring.client.bean.ClassInfo;
import io.camunda.zeebe.spring.client.bean.MethodInfo;
import io.camunda.zeebe.spring.client.jobhandling.JobWorkerManager;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

/**
 * Always created by {@link AnnotationProcessorConfiguration}
 *
 * <p>Triggered by {@link ZeebeAnnotationProcessorRegistry#postProcessAfterInitialization(Object,
 * String)} to add Handler subscriptions for {@link JobWorker} method-annotations.
 */
public class ZeebeWorkerAnnotationProcessor extends AbstractZeebeAnnotationProcessor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final JobWorkerManager jobWorkerManager;

  private final List<ZeebeWorkerValue> zeebeWorkerValues = new ArrayList<>();
  private final List<ZeebeWorkerValueCustomizer> zeebeWorkerValueCustomizers;

  public ZeebeWorkerAnnotationProcessor(
      final JobWorkerManager jobWorkerFactory,
      final List<ZeebeWorkerValueCustomizer> zeebeWorkerValueCustomizers) {
    jobWorkerManager = jobWorkerFactory;
    this.zeebeWorkerValueCustomizers = zeebeWorkerValueCustomizers;
  }

  @Override
  public boolean isApplicableFor(final ClassInfo beanInfo) {
    return beanInfo.hasMethodAnnotation(JobWorker.class);
  }

  @Override
  public void configureFor(final ClassInfo beanInfo) {
    final List<ZeebeWorkerValue> newZeebeWorkerValues = new ArrayList<>();

    doWithMethods(
        beanInfo.getTargetClass(),
        method ->
            readJobWorkerAnnotationForMethod(beanInfo.toMethodInfo(method))
                .ifPresent(newZeebeWorkerValues::add),
        ReflectionUtils.USER_DECLARED_METHODS);

    LOGGER.info(
        "Configuring {} Zeebe worker(s) of bean '{}': {}",
        newZeebeWorkerValues.size(),
        beanInfo.getBeanName(),
        newZeebeWorkerValues);
    zeebeWorkerValues.addAll(newZeebeWorkerValues);
  }

  @Override
  public void start(final ZeebeClient client) {
    zeebeWorkerValues.stream()
        .peek(
            zeebeWorkerValue ->
                zeebeWorkerValueCustomizers.forEach(
                    customizer -> customizer.customize(zeebeWorkerValue)))
        .filter(ZeebeWorkerValue::getEnabled)
        .forEach(
            zeebeWorkerValue -> {
              jobWorkerManager.openWorker(client, zeebeWorkerValue);
            });
  }

  @Override
  public void stop(final ZeebeClient zeebeClient) {
    jobWorkerManager.closeAllOpenWorkers();
  }

  public Optional<ZeebeWorkerValue> readJobWorkerAnnotationForMethod(final MethodInfo methodInfo) {
    final Optional<JobWorker> methodAnnotation = methodInfo.getAnnotation(JobWorker.class);
    if (methodAnnotation.isPresent()) {
      final JobWorker annotation = methodAnnotation.get();
      return Optional.of(
          new ZeebeWorkerValue(
              annotation.type(),
              annotation.name(),
              annotation.timeout(),
              annotation.maxJobsActive(),
              annotation.requestTimeout(),
              annotation.pollInterval(),
              annotation.autoComplete(),
              annotation.fetchVariables(),
              annotation.enabled(),
              methodInfo,
              Arrays.asList(annotation.tenantIds()),
              annotation.fetchAllVariables()));
    }
    return Optional.empty();
  }
}
