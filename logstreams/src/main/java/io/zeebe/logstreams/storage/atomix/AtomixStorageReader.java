/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.storage.atomix;

import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.ReadResultProcessor;
import io.zeebe.logstreams.spi.StorageReader;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.LongUnaryOperator;

public class AtomixStorageReader implements StorageReader {
  private static final ReadResultProcessor DEFAULT_READ_PROCESSOR =
      (buffer, readResult) -> readResult;

  private final RaftLogReader reader;

  public AtomixStorageReader(final RaftLogReader reader) {
    this.reader = reader;
  }

  @Override
  public long getFirstBlockAddress() {
    return reader.getFirstIndex();
  }

  @Override
  public long read(final ByteBuffer readBuffer, final long address) {
    return read(readBuffer, address, DEFAULT_READ_PROCESSOR);
  }

  @Override
  public long read(
      final ByteBuffer readBuffer, final long address, final ReadResultProcessor processor) {
    if (address < reader.getFirstIndex()) {
      return LogStorage.OP_RESULT_INVALID_ADDR;
    }

    if (address > reader.getLastIndex()) {
      return LogStorage.OP_RESULT_NO_DATA;
    }

    final var result =
        findEntry(address)
            .map(indexed -> copyEntryData(indexed, readBuffer, processor))
            .orElse(LogStorage.OP_RESULT_NO_DATA);

    if (result < 0) {
      return result;
    } else if (result == 0) {
      return LogStorage.OP_RESULT_NO_DATA;
    }

    return reader.getNextIndex();
  }

  @Override
  public long readLastBlock(final ByteBuffer readBuffer, final ReadResultProcessor processor) {
    final var result = read(readBuffer, reader.getLastIndex(), processor);

    // if reading the last index returns invalid address, this means the log is empty
    if (result == LogStorage.OP_RESULT_INVALID_ADDR) {
      return LogStorage.OP_RESULT_NO_DATA;
    }

    return result;
  }

  /**
   * Performs binary search over all known Atomix entries to find the entry containing our position.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public long lookUpApproximateAddress(
      final long position, final LongUnaryOperator positionReader) {
    var low = reader.getFirstIndex();
    var high = reader.getLastIndex();

    // when the log is empty, last index is defined as first index - 1
    if (low >= high) {
      // need a better way to figure out how to know if its empty
      if (findEntry(low).isEmpty()) {
        return LogStorage.OP_RESULT_INVALID_ADDR;
      }

      return low;
    }

    // stupid optimization
    if (position < 0) {
      return low;
    }

    // binary search over index range, assuming we have no missing indexes
    while (low <= high) {
      final var pivotIndex = (low + high) >>> 1;
      final var pivotPosition = positionReader.applyAsLong(pivotIndex);

      if (position > pivotPosition) {
        low = pivotIndex + 1;
      } else if (position < pivotPosition) {
        high = pivotIndex - 1;
      } else {
        return pivotIndex;
      }
    }

    return Math.max(high, reader.getFirstIndex());
  }

  /**
   * Looks up the entry whose index is either the given index, or the closest lower index.
   *
   * @param index index to seek to
   */
  private Optional<Indexed<ZeebeEntry>> findEntry(final long index) {
    if (reader.getNextIndex() != index) {
      reader.reset(index);
    }

    while (reader.hasNext()) {
      final var entry = reader.next();
      if (entry.type().equals(ZeebeEntry.class)) {
        return Optional.of(entry.cast());
      }
    }

    return Optional.empty();
  }

  @Override
  public void close() {
    reader.close();
  }

  private long copyEntryData(
      final Indexed<ZeebeEntry> entry, final ByteBuffer dest, final ReadResultProcessor processor) {
    final var data = entry.entry().getData();
    if (dest.remaining() < data.length) {
      return LogStorage.OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY;
    }

    dest.put(data);
    return processor.process(dest, data.length);
  }
}
