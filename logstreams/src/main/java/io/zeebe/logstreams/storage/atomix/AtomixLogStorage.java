/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.storage.atomix;

import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.StorageReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class AtomixLogStorage implements LogStorage {
  private final AtomixReaderFactory readerFactory;
  private final AtomixLogCompacter logCompacter;
  private final AtomixAppenderSupplier appenderSupplier;

  private boolean opened;

  public AtomixLogStorage(final RaftPartitionServer partition) {
    this(partition::openReader, new Compacter(partition), partition::getAppender);
  }

  public AtomixLogStorage(
      final AtomixReaderFactory readerFactory,
      final AtomixLogCompacter logCompacter,
      final AtomixAppenderSupplier appenderSupplier) {
    this.readerFactory = readerFactory;
    this.logCompacter = logCompacter;
    this.appenderSupplier = appenderSupplier;
  }

  @Override
  public StorageReader newReader() {
    return new AtomixStorageReader(readerFactory.create());
  }

  @Override
  public long append(final ByteBuffer blockBuffer) throws IOException {
    final var appender = appenderSupplier.getAppender().orElseThrow();
    final var data = blockBuffer.isDirect() ? copy(blockBuffer) : blockBuffer.array();
    return appender.appendEntry(data).join().index();
  }

  @Override
  public void delete(final long index) {
    logCompacter.compact(index, 0);
  }

  @Override
  public void open() throws IOException {
    opened = true;
  }

  @Override
  public void close() {
    opened = false;
  }

  @Override
  public boolean isOpen() {
    return opened;
  }

  @Override
  public boolean isClosed() {
    return !opened;
  }

  @Override
  public void flush() throws Exception {
    // does nothing as append guarantees blocks are appended immediately
  }

  private byte[] copy(final ByteBuffer buffer) {
    final var bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static final class Compacter implements AtomixLogCompacter {
    private final RaftPartitionServer partition;

    private Compacter(final RaftPartitionServer partition) {
      this.partition = partition;
    }

    @Override
    public CompletableFuture<Void> compact(final long index, final long term) {
      partition.setCompactablePosition(index, term);
      return partition.snapshot();
    }
  }
}
