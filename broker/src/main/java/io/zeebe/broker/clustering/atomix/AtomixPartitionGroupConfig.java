package io.zeebe.broker.clustering.atomix;

import io.atomix.primitive.partition.PartitionGroup.Type;
import io.atomix.protocols.raft.RaftStateMachineFactory;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;

public class AtomixPartitionGroupConfig extends RaftPartitionGroupConfig {
  private RaftStateMachineFactory stateMachineFactory = RaftServiceManager::new;

  @Override
  public Type getType() {
    return AtomixPartitionGroup.TYPE;
  }

  @Override
  public RaftStateMachineFactory getStateMachineFactory() {
    return stateMachineFactory;
  }

  public AtomixPartitionGroupConfig setStateMachineFactory(
    final RaftStateMachineFactory stateMachineFactory) {
    this.stateMachineFactory = stateMachineFactory;
    return this;
  }

}
