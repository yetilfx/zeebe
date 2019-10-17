package io.zeebe.logstreams.impl.atomix;

import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.protocols.raft.zeebe.ZeebeLogAppender;
import io.atomix.storage.journal.Indexed;
import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Subscription;
import java.util.Optional;

class AtomixBufferedWriter {
  private final ZeebeLogAppender appender;
  private final int bufferMaxFrameLength;
  private final Dispatcher buffer;
  private final Subscription bufferSubscription;
  private final BlockPeek peekedBlock;

  AtomixBufferedWriter(
      final ZeebeLogAppender appender,
      final Dispatcher buffer,
      final Subscription bufferSubscription,
      final int bufferMaxFrameLength) {
    this.appender = appender;
    this.buffer = buffer;
    this.bufferMaxFrameLength = bufferMaxFrameLength;
    this.bufferSubscription = bufferSubscription;
    this.peekedBlock = new BlockPeek();
  }

  Optional<BlockPeek> peekSubscription() {
    final int bytesRead = bufferSubscription.peekBlock(peekedBlock, bufferMaxFrameLength, true);
    if (bytesRead > 0) {
      return Optional.of(peekedBlock);
    }

    return Optional.empty();
  }

  Indexed<ZeebeEntry> write(final byte[] data) {
    final Indexed<ZeebeEntry> entry = appender.appendEntry(data).join();
    peekedBlock.markCompleted();
    return entry;
  }
}
