/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.test;

import static org.mockito.Mockito.spy;

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.storage.atomix.AtomixLogStorage;
import io.zeebe.test.util.atomix.AtomixTestNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * Provides a LogStream which is backed by an AtomixLogStorage instance; in order to properly test,
 * this means a single node Raft Atomix instance is started.
 *
 * <p>This creates a single node, single partition Raft only.
 */
public class AtomixSingleNodeLogStorageRule extends ExternalResource
    implements Supplier<LogStorage> {
  private final TemporaryFolder temporaryFolder;
  private final boolean shouldStart;

  private AtomixTestNode testNode;
  private AtomixLogStorage logStorage;
  private LogStream logStream;

  public AtomixSingleNodeLogStorageRule(final TemporaryFolder temporaryFolder) {
    this(temporaryFolder, true);
  }

  public AtomixSingleNodeLogStorageRule(
      final TemporaryFolder temporaryFolder, final boolean shouldStart) {
    this.temporaryFolder = temporaryFolder;
    this.shouldStart = shouldStart;
  }

  @Override
  public void before() throws Throwable {
    if (shouldStart) {
      start();
    }
  }

  @Override
  public void after() {
    if (testNode != null) {
      testNode.stop().join();
    }
  }

  public void stop() {
    if (logStorage != null) {
      logStorage.close();
      logStorage = null;
    }

    if (testNode != null) {
      testNode.stop().join();
      testNode = null;
    }
  }

  public void start() {
    if (testNode == null) {
      testNode = new AtomixTestNode(0, newFolder(temporaryFolder), UnaryOperator.identity());
      testNode.setMembers(Collections.singleton(testNode));
      testNode.start().join();
    }

    if (logStorage == null) {
      logStorage = spy(new AtomixLogStorage(testNode.getPartitionServer(0)));
    }
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public void setLogStream(final LogStream logStream) {
    this.logStream = logStream;
  }

  public AtomixTestNode getTestNode() {
    return testNode;
  }

  public AtomixLogStorage getLogStorage() {
    return logStorage;
  }

  @Override
  public AtomixLogStorage get() {
    return getLogStorage();
  }

  private File newFolder(final TemporaryFolder provider) {
    try {
      return provider.newFolder("atomix");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
