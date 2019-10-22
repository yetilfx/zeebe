package io.zeebe.logstreams.impl.atomix;

import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.storage.journal.index.SparseJournalIndex;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.logstreams.impl.LogStorageAppender;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Service;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.nio.ByteBuffer;

public class AtomixLogStream {
//  private final RaftPartition partition;
//
//  // when in read-write mode, these fields are non-null
//  private Dispatcher dispatcher;
//  private LogStorageAppender appender;
//
//  public AtomixLogStream(final RaftPartition partition) {
//    this.partition = partition;
//  }
//
//  @Override
//  public LogStream get() {
//    return this;
//  }
//
//  @Override
//  public int getPartitionId() {
//    return partition.id().id();
//  }
//
//  @Override
//  public String getLogName() {
//    return partition.name();
//  }
//
//  @Override
//  public void close() {
//    // nothing to be done as the RaftPartition itself is closed elsewhere
//  }
//
//  @Override
//  public ActorFuture<Void> closeAsync() {
//    return CompletableActorFuture.completed(null);
//  }
//
//  @Override
//  public LogStreamReader openReader(final long position) {
//    return null;
//  }
//
//  @Override
//  public long getCommitPosition() {
//    return -1;
//  }
//
//  @Override
//  public long append(final long commitPosition, final ByteBuffer buffer) {
//    throw new UnsupportedOperationException(
//        "Appending to the log stream does not make sense with the new approach");
//  }
//
//  @Override
//  public LogStorage getLogStorage() {
//    throw new UnsupportedOperationException(
//        "Should not have to access the log storage directly anymore");
//  }
//
//  @Override
//  public Dispatcher getWriteBuffer() {
//    return dispatcher;
//  }
//
//  @Override
//  public LogStorageAppender getLogStorageAppender() {
//    return appender;
//  }
//
//  @Override
//  public ActorFuture<Void> closeAppender() {
//    if (appender == null) {
//      return CompletableActorFuture.completed(null);
//    }
//
//    return appender.close();
//  }
//
//  @Override
//  public ActorFuture<LogStorageAppender> openAppender() {
//    if (appender != null) {
//      return CompletableActorFuture.completed(appender);
//    }
//
//    appender = new LogStorageAppender(getLogName(), null, null, 1024);
//    return null;
//  }
//
//  @Override
//  public void delete(final long position) {}
//
//  @Override
//  public void registerOnCommitPositionUpdatedCondition(final ActorCondition condition) {}
//
//  @Override
//  public void removeOnCommitPositionUpdatedCondition(final ActorCondition condition) {}
}
