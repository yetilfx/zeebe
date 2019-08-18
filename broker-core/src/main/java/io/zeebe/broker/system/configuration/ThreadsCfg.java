/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import com.typesafe.config.Optional;

public class ThreadsCfg implements ConfigurationEntry {
  private int cpuThreadCount = 2;
  private int ioThreadCount = 2;

  @Optional public int getCpuThreadCount() {
    return cpuThreadCount;
  }
  @Optional public void setCpuThreadCount(int cpuThreads) {
    this.cpuThreadCount = cpuThreads;
  }
  @Optional public int getIoThreadCount() {
    return ioThreadCount;
  }
  @Optional public void setIoThreadCount(int ioThreads) {
    this.ioThreadCount = ioThreads;
  }

  @Override
  public String toString() {
    return "ThreadsCfg{"
        + "cpuThreadCount="
        + cpuThreadCount
        + ", ioThreadCount="
        + ioThreadCount
        + '}';
  }
}
