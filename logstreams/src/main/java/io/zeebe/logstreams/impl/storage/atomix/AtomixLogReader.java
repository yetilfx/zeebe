package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import java.util.Optional;

public class AtomixLogReader {
  // naive optimization would be to pool the readers we create
  private final AtomixReaderFactory factory;

  // use a fixed reader strictly for first/last index isn't create, but works for now
  private final RaftLogReader reader;

  AtomixLogReader(final AtomixReaderFactory factory) {
    this.factory = factory;
    this.reader = factory.create();
  }

  long getFirstIndex() {
    return reader.getFirstIndex();
  }

  long getLastIndex() {
    return reader.getLastIndex();
  }

  /**
   * Looks up the entry whose index is either the given index, or the closest lower index.
   *
   * @param index index to seek to
   */
  Optional<Indexed<ZeebeEntry>> read(final long index) {
    try (final var reader = factory.create(index)) {
      while (reader.hasNext()) {
        final var entry = reader.next();
        if (entry.type().equals(ZeebeEntry.class)) {
          return Optional.of(entry.cast());
        }
      }
    }

    return Optional.empty();
  }

  public void close() {
    reader.close();
  }
}
