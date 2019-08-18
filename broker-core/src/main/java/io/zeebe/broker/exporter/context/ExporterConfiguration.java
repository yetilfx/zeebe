/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.exporter.context;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigObject;
import io.zeebe.exporter.api.context.Configuration;
import java.util.Map;

public class ExporterConfiguration implements Configuration {
  private final String id;
  private final ConfigObject arguments;

  public ExporterConfiguration(final String id, final ConfigObject arguments) {
    this.id = id;
    this.arguments = arguments;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public Map<String, Object> getArguments() {
    return arguments.unwrapped();
  }

  @Override
  public <T> T instantiate(Class<T> configClass) {
    return ConfigBeanFactory.create(arguments.toConfig(), configClass);
  }
}
