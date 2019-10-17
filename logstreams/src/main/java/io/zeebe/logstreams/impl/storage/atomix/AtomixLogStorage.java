package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.JournalReader.Mode;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.ReadResultProcessor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongUnaryOperator;
import org.agrona.concurrent.UnsafeBuffer;

public class AtomixLogStorage implements LogStorage {
  private final RaftPartitionServer partition;

  private AtomixLogReader reader;

  public AtomixLogStorage(final RaftPartitionServer partition) {
    this.partition = partition;
  }

  @Override
  public long append(final ByteBuffer blockBuffer) throws IOException {
    final var appender = partition.getAppender().orElseThrow();
    final var data = blockBuffer.isDirect() ? copy(blockBuffer) : blockBuffer.array();
    return appender.appendEntry(data).join().index();
  }

  @Override
  public void delete(final long index) {
    partition.setCompactablePosition(index, 0);
    partition.snapshot(); // forces compaction
  }

  @Override
  public long read(final ByteBuffer readBuffer, final long addr) {
    return 0;
  }

  @Override
  public long read(
      final ByteBuffer readBuffer, final long index, final ReadResultProcessor processor) {
    reader.reset(index);
    if (reader.hasNext()) {
      return put(reader.next(), readBuffer, processor);
    }

    return OP_RESULT_INVALID_ADDR;
  }

  @Override
  public long readLastBlock(final ByteBuffer readBuffer, final ReadResultProcessor processor) {
    return read(readBuffer, reader.getLastIndex(), processor);
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

    while (low <= high) {
      final var pivotIndex = (low + high) >>> 1;
      final var pivotPosition = positionReader.applyAsLong(pivotIndex);

      if (pivotPosition < position) {
        low = pivotIndex;
      } else if (pivotPosition > position) {
        high = pivotIndex;
      } else {
        return pivotIndex;
      }
    }

    return LogStorage.OP_RESULT_INVALID_ADDR;
  }

  @Override
  public boolean isByteAddressable() {
    return false;
  }

  @Override
  public void open() throws IOException {
    reader = new AtomixLogReader(partition.openReader(0, Mode.COMMITS));
  }

  @Override
  public void close() {
    reader.close();
    reader = null;
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

  private long put(
      final Indexed<ZeebeEntry> entry, final ByteBuffer dest, final ReadResultProcessor processor) {
    final var data = entry.entry().getData();
    if (dest.remaining() < data.length) {
      return OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY;
    }

    dest.put(data);
    final var bytesRead = processor.process(dest, data.length);
    return bytesRead < 0 ? bytesRead : reader.getNextIndex();
  }

  private byte[] copy(final ByteBuffer buffer) {
    final var bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private long readPosition(final byte[] data) {
    final var event = new LoggedEventImpl();
    event.wrap(new UnsafeBuffer(data), 0);
    return event.getPosition();
  }
}
