/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.clustering.atomix;

import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.agrona.concurrent.UnsafeBuffer;

public class AtomixStateMachine extends RaftServiceManager {
  private final LoggedEventImpl event;
  private final AtomixPositionBroadcaster positionBroadcaster;
  private final String raftName;

  private volatile long compactableIndex;
  private volatile long compactableTerm;

  // only a single listener is expected for now, the logstream itself

  public AtomixStateMachine(
      final RaftContext raft,
      final ThreadContext stateContext,
      final ThreadContextFactory threadContextFactory,
      final AtomixPositionBroadcaster positionBroadcaster) {
    super(raft, stateContext, threadContextFactory);

    this.raftName = raft.getName();
    this.positionBroadcaster = positionBroadcaster;
    this.event = new LoggedEventImpl();
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

    // update the logstream with the newest commit position
    final var commitPosition = findGreatestPosition(entry.cast());
    positionBroadcaster.notifyPositionListener(raftName, commitPosition);

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

  // not ideal as we have to get to the latest event in order to get the correct position
  // one possible optimization is adding first/last position in each ZeebeEntry as metadata
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
