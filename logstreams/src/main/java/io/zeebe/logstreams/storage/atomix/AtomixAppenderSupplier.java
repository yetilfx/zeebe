package io.zeebe.logstreams.storage.atomix;

import io.atomix.protocols.raft.zeebe.ZeebeLogAppender;
import java.util.Optional;

@FunctionalInterface
public interface AtomixAppenderSupplier {
  Optional<ZeebeLogAppender> getAppender();
}
