package io.zeebe.logstreams.impl.atomix;

import io.atomix.protocols.raft.zeebe.ZeebeLogAppender;
import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.List;

class AtomixBufferedWriterService extends Actor implements Service<AtomixBufferedWriterService> {
  private final String logName;
  private final int maxFragmentLength;
  private final ZeebeLogAppender appender;
  private final BlockPeek block;

  // created on start
  private Dispatcher buffer;
  private Subscription bufferSubscription;

  // transient state
  private byte[] bytesToAppend;

  public AtomixBufferedWriterService(
      final String logName, final int maxFragmentLength, final ZeebeLogAppender appender) {
    this.logName = logName;
    this.maxFragmentLength = maxFragmentLength;
    this.appender = appender;

    this.block = new BlockPeek();
  }

  public Dispatcher getBuffer() {
    return buffer;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    buffer = createBuffer();
    bufferSubscription = createBufferSubscription();
    startContext.async(startContext.getScheduler().submitActor(this), true);
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    final CompletableActorFuture<Void> stopped = new CompletableActorFuture<>();
    actor.runOnCompletion(List.of(buffer.closeAsync(), actor.close()), stopped::accept);
    stopContext.async(stopped);
  }

  @Override
  public AtomixBufferedWriterService get() {
    return this;
  }

  @Override
  protected void onActorStarting() {
    actor.consume(bufferSubscription, this::peekSubscription);
  }

  @Override
  protected void onActorClosing() {
    buffer.closeAsync();
  }

  private void peekSubscription() {
    final int bytesRead = bufferSubscription.peekBlock(block, maxFragmentLength, true);
    if (bytesRead > 0) {
      bytesToAppend = BufferUtil.bufferAsArray(block.getBuffer());
      actor.runUntilDone(this::write);
    } else {
      actor.yield();
    }
  }

  private void write() {
    try {
      appender.appendEntry(bytesToAppend).join();
      block.markCompleted();
      bytesToAppend = null;
      actor.done();
    } catch (final RuntimeException e) {
      // TODO: log exception
      actor.yield();
    }
  }

  private Dispatcher createBuffer() {
    final var name = String.format("logstream.%s.writeBuffer", logName);
    return Dispatchers.create(name).maxFragmentLength(maxFragmentLength).build();
  }

  private Subscription createBufferSubscription() {
    final var name = String.format("%s.atomixWriter", buffer.getName());
    return buffer.getSubscription(name);
  }
}
