package io.zeebe.broker.clustering.atomix;

import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.protocols.raft.RaftStateMachineFactory;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

public class AtomixPartitionGroup extends RaftPartitionGroup {
  public static final AtomixPartitionGroup.Type TYPE = new AtomixPartitionGroup.Type();

  public AtomixPartitionGroup(final RaftPartitionGroupConfig config) {
    super(config);
  }

  /**
   * Returns a new Raft partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(final String name) {
    return new Builder((AtomixPartitionGroupConfig) new AtomixPartitionGroupConfig().setName(name));
  }

  /** Raft partition group type. */
  public static class Type implements PartitionGroup.Type<RaftPartitionGroupConfig> {
    private static final String NAME = "zeebe-raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Namespace namespace() {
      return Namespace.builder()
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 100)
        .register(RaftPartitionGroup.TYPE.namespace())
        .build();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(final RaftPartitionGroupConfig config) {
      return new AtomixPartitionGroup(config);
    }

    @Override
    public RaftPartitionGroupConfig newConfig() {
      return new AtomixPartitionGroupConfig();
    }
  }

  public static class Builder extends RaftPartitionGroup.Builder {
    private final AtomixPartitionGroupConfig atomixConfig;

    protected Builder(final AtomixPartitionGroupConfig config) {
      super(config);
      this.atomixConfig = config;
    }

    public Builder withStateMachineFactory(final RaftStateMachineFactory stateMachineFactory) {
      atomixConfig.setStateMachineFactory(stateMachineFactory);
      return this;
    }
  }
}
