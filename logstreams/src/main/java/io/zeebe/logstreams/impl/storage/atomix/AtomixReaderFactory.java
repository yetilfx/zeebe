package io.zeebe.logstreams.impl.storage.atomix;

import io.atomix.protocols.raft.storage.log.RaftLogReader;

@FunctionalInterface
public interface AtomixReaderFactory {
  RaftLogReader create(long index);

  default RaftLogReader create() {
    return create(0);
  }
}
