package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.log.LoggedEvent;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

class AtomixLogEntry implements Iterator<LoggedEvent> {
  private final Indexed<ZeebeEntry> entry;
  private final LoggedEventImpl event;
  private final DirectBuffer eventBufferView;

  private boolean hasNext;
  private int offset;

  public AtomixLogEntry(final Indexed<ZeebeEntry> entry) {
    this.entry = entry;
    this.eventBufferView = new UnsafeBuffer(entry.entry().getData());
    this.event = new LoggedEventImpl();

    this.event.wrap(eventBufferView, 0);
    this.hasNext = true;
  }

  public long getIndex() {
    return entry.index();
  }

  @Override
  public boolean hasNext() {
    return hasNext || readNextEvent();
  }

  @Override
  public LoggedEvent next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    hasNext = false;
    return event;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private boolean readNextEvent() {
    final var nextOffset = offset + event.getLength();
    if (nextOffset < entry.size()) {
      offset = nextOffset;
      event.wrap(eventBufferView, offset);
      hasNext = true;
    }

    return hasNext;
  }
}
