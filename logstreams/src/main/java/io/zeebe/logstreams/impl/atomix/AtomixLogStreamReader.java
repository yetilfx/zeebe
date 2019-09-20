package io.zeebe.logstreams.impl.atomix;

import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;

public class AtomixLogStreamReader implements LogStreamReader {
  private final RaftLogReader reader;

  public AtomixLogStreamReader(final RaftLogReader reader) {
    this.reader = reader;
  }

  @Override
  public void wrap(final LogStream log) {

  }

  @Override
  public void wrap(final LogStream log, final long position) {

  }

  @Override
  public boolean seekToNextEvent(final long position) {
    return false;
  }

  @Override
  public boolean seek(final long position) {
    return false;
  }

  @Override
  public void seekToFirstEvent() {

  }

  @Override
  public long seekToEnd() {
    return 0;
  }

  @Override
  public long getPosition() {
    return 0;
  }

  @Override
  public long lastReadAddress() {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public LoggedEvent next() {
    return null;
  }
}
