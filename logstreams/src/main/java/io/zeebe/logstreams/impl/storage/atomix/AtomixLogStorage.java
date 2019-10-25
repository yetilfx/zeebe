package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.ReadResultProcessor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongUnaryOperator;

public class AtomixLogStorage implements LogStorage {
  private static final ReadResultProcessor DEFAULT_READ_PROCESSOR =
      (buffer, readResult) -> readResult;

  private final AtomixReaderFactory readerFactory;
  private final AtomixLogCompacter logCompacter;
  private final AtomixAppenderSupplier appenderSupplier;

  private AtomixLogReader reader;

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
  public long read(final ByteBuffer readBuffer, final long addr) {
    return read(readBuffer, addr, DEFAULT_READ_PROCESSOR);
  }

  @Override
  public long read(
      final ByteBuffer readBuffer, final long index, final ReadResultProcessor processor) {
    if (index < reader.getFirstIndex()) {
      return OP_RESULT_INVALID_ADDR;
    }

    if (index > reader.getLastIndex()) {
      return OP_RESULT_NO_DATA;
    }

    return reader
        .read(index)
        .map(indexed -> put(indexed, readBuffer, processor))
        .orElse(OP_RESULT_INVALID_ADDR);
  }

  @Override
  public long readLastBlock(final ByteBuffer readBuffer, final ReadResultProcessor processor) {
    final var result = read(readBuffer, reader.getLastIndex(), processor);

    // if reading the last index returns invalid address, this means the log is empty
    if (result == OP_RESULT_INVALID_ADDR) {
      return OP_RESULT_NO_DATA;
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
      if (reader.read(low).isEmpty()) {
        return OP_RESULT_NO_DATA;
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

  @Override
  public boolean isByteAddressable() {
    return false;
  }

  @Override
  public void open() throws IOException {
    if (reader == null) {
      reader = new AtomixLogReader(readerFactory);
    }
  }

  @Override
  public void close() {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  @Override
  public boolean isOpen() {
    return reader != null;
  }

  @Override
  public boolean isClosed() {
    return reader == null;
  }

  @Override
  public long getFirstBlockAddress() {
    return reader.getFirstIndex();
  }

  @Override
  public void flush() throws Exception {
    // does nothing as append guarantees blocks are appended immediately
  }

  /**
   * Returns the either a special purpose negative value, or the next entry index. This is obviously
   * not a very accurate measure, as the next entry may not be a ZeebeEntry, but it should be good
   * enough.
   */
  private long put(
      final Indexed<ZeebeEntry> entry, final ByteBuffer dest, final ReadResultProcessor processor) {
    final var data = entry.entry().getData();
    if (dest.remaining() < data.length) {
      return OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY;
    }

    dest.put(data);
    final var bytesRead = processor.process(dest, data.length);

    if (bytesRead < 0) {
      return bytesRead;
    } else if (bytesRead == 0) {
      return OP_RESULT_NO_DATA;
    }

    return entry.index() + 1;
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
