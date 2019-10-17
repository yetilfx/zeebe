package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.JournalReader;
import java.util.NoSuchElementException;

public class AtomixLogReader implements JournalReader<ZeebeEntry> {
  private final RaftLogReader delegate;

  private Indexed<ZeebeEntry> currentEntry;

  public AtomixLogReader(final RaftLogReader delegate) {
    this.delegate = delegate;
    readNextEntry();
  }

  @Override
  public long getFirstIndex() {
    return delegate.getFirstIndex();
  }

  @Override
  public long getLastIndex() {
    return delegate.getLastIndex();
  }

  @Override
  public long getCurrentIndex() {
    return currentEntry == null ? delegate.getCurrentIndex() : currentEntry.index();
  }

  /**
   * Can be null if next() hasn't been called yet.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public Indexed<ZeebeEntry> getCurrentEntry() {
    return currentEntry;
  }

  /**
   * This is not accurate, as we don't actually know what the index of the next ZeebeEntry will be,
   * so use carefully.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public long getNextIndex() {
    return currentEntry.index() + 1;
  }

  @Override
  public boolean hasNext() {
    return currentEntry != null || readNextEntry();
  }

  @Override
  public Indexed<ZeebeEntry> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return currentEntry;
  }

  @Override
  public void reset() {
    delegate.reset();
    readNextEntry();
  }

  @Override
  public void reset(final long index) {
    delegate.reset(index);
    readNextEntry();
  }

  @Override
  public void close() {
    currentEntry = null;
    delegate.close();
  }

  private boolean readNextEntry() {
    Indexed<RaftLogEntry> entry = delegate.getCurrentEntry();
    if (entry == null) {
      if (delegate.hasNext()) {
        entry = delegate.next();
      } else {
        return false; // reached end of log
      }
    }

    while (!entry.type().equals(ZeebeEntry.class) && delegate.hasNext()) {
      entry = delegate.next();
    }

    if (entry.type().equals(ZeebeEntry.class)) {
      currentEntry = entry.cast();
      return true;
    }

    return false;
  }
}
