/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import io.zeebe.util.Environment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataCfg implements ConfigurationEntry {
  public static final String DEFAULT_DIRECTORY = "data";

  // Hint: do not use Collections.singletonList as this does not support replaceAll
  @Optional private List<String> directories = Arrays.asList(DEFAULT_DIRECTORY);
  @Optional private ConfigMemorySize logSegmentSize = ConfigMemorySize.ofBytes(512 * 1024 * 1024);
  @Optional private Duration snapshotPeriod = Duration.ofMinutes(15);
  @Optional private Duration snapshotReplicationPeriod = Duration.ofMinutes(5);
  @Optional private ConfigMemorySize raftSegmentSize;
  @Optional private int maxSnapshots = 3;

  @Override
  public void init(
      final BrokerCfg globalConfig, final String brokerBase, final Environment environment) {
    applyEnvironment(environment);
    directories.replaceAll(d -> ConfigurationUtil.toAbsolutePath(d, brokerBase));
  }

  private void applyEnvironment(final Environment environment) {
    environment.getList(EnvironmentConstants.ENV_DIRECTORIES).ifPresent(v -> directories = v);
  }

  public List<String> getDirectories() {
    return directories;
  }

  public void setDirectories(final List<String> directories) {
    this.directories = directories;
  }

  public ConfigMemorySize getLogSegmentSize() {
    return logSegmentSize;
  }

  public void setLogSegmentSize(final ConfigMemorySize logSegmentSize) {
    this.logSegmentSize = logSegmentSize;
  }

  public Duration getSnapshotPeriod() {
    return snapshotPeriod;
  }

  public void setSnapshotPeriod(final Duration snapshotPeriod) {
    this.snapshotPeriod = snapshotPeriod;
  }

  public Duration getSnapshotReplicationPeriod() {
    return snapshotReplicationPeriod;
  }

  public void setSnapshotReplicationPeriod(final Duration snapshotReplicationPeriod) {
    this.snapshotReplicationPeriod = snapshotReplicationPeriod;
  }

  public int getMaxSnapshots() {
    return maxSnapshots;
  }

  public void setMaxSnapshots(final int maxSnapshots) {
    this.maxSnapshots = maxSnapshots;
  }

  public ConfigMemorySize getRaftSegmentSize() {
    return raftSegmentSize;
  }

  public void setRaftSegmentSize(final ConfigMemorySize raftSegmentSize) {
    this.raftSegmentSize = raftSegmentSize;
  }

  @Override
  public String toString() {
    return "DataCfg{"
        + "directories="
        + directories
        + ", logSegmentSize='"
        + logSegmentSize
        + '\''
        + ", snapshotPeriod='"
        + snapshotPeriod
        + '\''
        + ", snapshotReplicationPeriod='"
        + snapshotReplicationPeriod
        + '\''
        + ", maxSnapshots='"
        + maxSnapshots
        + '\''
        + '}';
  }
}
