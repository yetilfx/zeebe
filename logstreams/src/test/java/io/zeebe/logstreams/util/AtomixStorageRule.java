package io.zeebe.logstreams.util;

import io.atomix.protocols.raft.partition.impl.RaftNamespaces;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.RaftLog;
import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotStore;
import io.atomix.protocols.raft.storage.system.MetaStore;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.protocols.raft.zeebe.ZeebeLogAppender;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.JournalReader.Mode;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.impl.storage.atomix.AtomixAppenderSupplier;
import io.zeebe.logstreams.impl.storage.atomix.AtomixLogCompacter;
import io.zeebe.logstreams.impl.storage.atomix.AtomixLogStorage;
import io.zeebe.logstreams.impl.storage.atomix.AtomixReaderFactory;
import io.zeebe.logstreams.spi.LogStorage;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/** This thing really does everything :+1: */
public class AtomixStorageRule extends ExternalResource
    implements AtomixLogCompacter, AtomixReaderFactory, AtomixAppenderSupplier, ZeebeLogAppender,
  Supplier<LogStorage> {
  private final LoggedEventImpl event = new LoggedEventImpl();
  private final TemporaryFolder temporaryFolder;
  private final int partitionId;
  private final RaftStorage.Builder builder;

  private RaftStorage raftStorage;
  private RaftLog raftLog;
  private SnapshotStore snapshotStore;
  private MetaStore metaStore;

  private AtomixLogStorage storage;
  private LongConsumer positionListener;

  public AtomixStorageRule(final TemporaryFolder temporaryFolder) {
    this(temporaryFolder, 0);
  }

  public AtomixStorageRule(final TemporaryFolder temporaryFolder, final int partitionId) {
    this(temporaryFolder, partitionId, UnaryOperator.identity());
  }

  public AtomixStorageRule(
      final TemporaryFolder temporaryFolder,
      final int partitionId,
      final UnaryOperator<RaftStorage.Builder> builder) {
    this.temporaryFolder = temporaryFolder;
    this.partitionId = partitionId;
    this.builder = builder.apply(buildDefaultStorage());
  }

  @Override
  public void before() throws Throwable {
    open();
  }

  @Override
  public void after() {
    close();
  }

  @Override
  public CompletableFuture<Indexed<ZeebeEntry>> appendEntry(final byte[] data) {
    final Indexed<ZeebeEntry> entry =
        raftLog.writer().append(new ZeebeEntry(0, System.currentTimeMillis(), data));
    raftLog.writer().commit(entry.index());

    if (positionListener != null) {
      positionListener.accept(findGreatestPosition(entry));
    }

    return CompletableFuture.completedFuture(entry);
  }

  @Override
  public AtomixLogStorage get() {
    return storage;
  }

  @Override
  public Optional<ZeebeLogAppender> getAppender() {
    return Optional.of(this);
  }

  @Override
  public CompletableFuture<Void> compact(final long index, final long term) {
    raftLog.compact(index);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public RaftLogReader create(final long index, final Mode mode) {
    return raftLog.openReader(index, mode);
  }

  public void setPositionListener(final LongConsumer positionListener) {
    this.positionListener = positionListener;
  }

  public void open() throws IOException {
    close();

    final var directory = temporaryFolder.newFolder(String.format("atomix-%d", partitionId));
    raftStorage = builder.withDirectory(directory).build();
    raftLog = raftStorage.openLog();
    snapshotStore = raftStorage.openSnapshotStore();
    metaStore = raftStorage.openMetaStore();

    storage = new AtomixLogStorage(this, this, this);
  }

  public void close() {
    Optional.ofNullable(raftLog).ifPresent(RaftLog::close);
    Optional.ofNullable(snapshotStore).ifPresent(SnapshotStore::close);
    Optional.ofNullable(raftStorage).ifPresent(ignored -> deleteStorage());
  }

  public int getPartitionId() {
    return partitionId;
  }

  public AtomixLogStorage getStorage() {
    return storage;
  }

  public RaftStorage getRaftStorage() {
    return raftStorage;
  }

  public RaftLog getRaftLog() {
    return raftLog;
  }

  public SnapshotStore getSnapshotStore() {
    return snapshotStore;
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  private RaftStorage.Builder buildDefaultStorage() {
    return RaftStorage.builder()
        .withFlushOnCommit()
        .withStorageLevel(StorageLevel.DISK)
        .withNamespace(RaftNamespaces.RAFT_STORAGE)
        .withRetainStaleSnapshots();
  }

  private void deleteStorage() {
    raftStorage.deleteMetaStore();
    raftStorage.deleteLog();
    raftStorage.deleteSnapshotStore();
    raftStorage.directory().delete();
  }

  private long findGreatestPosition(final Indexed<ZeebeEntry> indexed) {
    final var entry = indexed.entry();
    final var data = new UnsafeBuffer(entry.getData());

    var offset = 0;
    do {
      event.wrap(data, offset);
      offset += event.getLength();
    } while (offset < data.capacity());

    return event.getPosition();
  }
}
