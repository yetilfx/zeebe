package io.zeebe.logstreams.util;

import static org.mockito.Mockito.spy;

import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.impl.storage.atomix.AtomixLogStorage;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.test.util.atomix.AtomixTestNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * Provides a LogStream which is backed by an AtomixLogStorage instance; in order to properly test,
 * this means a single node Raft Atomix instance is started.
 *
 * <p>This creates a single node, single partition Raft only.
 */
public class AtomixLogStorageRule extends ExternalResource implements Supplier<LogStorage> {
  private final TemporaryFolder temporaryFolder;
  private final boolean shouldStart;

  private AtomixTestNode testNode;
  private AtomixLogStorage logStorage;
  private LogStream logStream;

  public AtomixLogStorageRule(final TemporaryFolder temporaryFolder) {
    this(temporaryFolder, true);
  }

  public AtomixLogStorageRule(final TemporaryFolder temporaryFolder, final boolean shouldStart) {
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
      testNode =
          new AtomixTestNode(
              0, newFolder(temporaryFolder), b -> b.withStateMachineFactory(StateMachine::new));
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

  /**
   * Simple controllable state machine that updates the LogStream's commit position when called to
   * apply a new ZeebeEntry.
   *
   * <p>A potential optimization is to encode the first/last position as metadata in the entry.
   */
  class StateMachine extends RaftServiceManager {
    private final LoggedEventImpl event = new LoggedEventImpl();

    private volatile long compactableIndex;
    private volatile long compactableTerm;

    StateMachine(
        final RaftContext raft,
        final ThreadContext stateContext,
        final ThreadContextFactory threadContextFactory) {
      super(raft, stateContext, threadContextFactory);
    }

    @Override
    public void setCompactablePosition(final long index, final long term) {
      if (term > compactableTerm) {
        compactableIndex = index;
        compactableTerm = term;
      } else if (term == compactableTerm && index > compactableIndex) {
        compactableIndex = index;
      }
    }

    @Override
    public long getCompactableIndex() {
      return compactableIndex;
    }

    @Override
    public long getCompactableTerm() {
      return compactableTerm;
    }

    @Override
    public <T> CompletableFuture<T> apply(final Indexed<? extends RaftLogEntry> entry) {
      if (!entry.type().equals(ZeebeEntry.class)) {
        return super.apply(entry);
      }

      // it's possible the log stream would be null because the log storage rule has to be started
      // before the log stream rule (so the storage is provided), but if we actually apply an entry
      // before the log stream rule is started, then it will be null here - should be safe-ish to
      // to wait
      if (logStream != null) {
        logStream.setCommitPosition(findGreatestPosition(entry.cast()));
      }

      return CompletableFuture.completedFuture(null);
    }

    @Override
    protected Duration getSnapshotCompletionDelay() {
      return Duration.ZERO;
    }

    @Override
    protected Duration getCompactDelay() {
      return Duration.ZERO;
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
}
