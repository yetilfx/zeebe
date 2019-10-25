package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.storage.journal.JournalReader.Mode;

@FunctionalInterface
public interface AtomixReaderFactory {
  RaftLogReader create(long index, final Mode mode);

  default RaftLogReader create(final long index) {
    return create(index, Mode.COMMITS);
  }

  default RaftLogReader create() {
    return create(0);
  }
}
