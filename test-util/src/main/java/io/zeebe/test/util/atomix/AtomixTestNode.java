package io.zeebe.test.util.atomix;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.impl.ClasspathScanningPrimitiveTypeRegistry;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionGroupTypeRegistry;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.atomix.storage.StorageLevel;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class AtomixTestNode {
  private static final String CLUSTER_ID = "zeebe";
  private static final String DATA_PARTITION_GROUP_NAME = "data";
  private static final String SYSTEM_PARTITION_GROUP_NAME = "system";
  private static final String HOST = "localhost";
  private static final int BASE_PORT = 10_000;
  private final Member member;
  private final Node node;
  private final File directory;
  private final UnaryOperator<AtomixPartitionGroup.Builder> builder;

  private RaftPartitionGroup dataPartitionGroup;
  private RaftPartitionGroup systemPartitionGroup;
  private ManagedPartitionService partitionService;
  private AtomixCluster cluster;

  public AtomixTestNode(final int id, final File directory) {
    this(id, directory, UnaryOperator.identity());
  }

  public AtomixTestNode(
      final int id,
      final File directory,
      final UnaryOperator<AtomixPartitionGroup.Builder> builder) {
    final var textualId = String.valueOf(id);

    this.directory = directory;
    this.builder = builder;
    this.node = Node.builder().withId(textualId).withHost(HOST).withPort(BASE_PORT + id).build();
    this.member = Member.member(MemberId.from(textualId), node.address());
  }

  public MemberId getMemberId() {
    return member.id();
  }

  public Member getMember() {
    return member;
  }

  public Node getNode() {
    return node;
  }

  public AtomixCluster getCluster() {
    return cluster;
  }

  public RaftPartitionServer getPartitionServer(final int id) {
    return getPartition(id).getServer();
  }

  public RaftPartition getPartition(final int id) {
    return (RaftPartition) getDataPartitionGroup().getPartition(String.valueOf(id));
  }

  public RaftPartitionGroup getDataPartitionGroup() {
    return (RaftPartitionGroup) partitionService.getPartitionGroup(DATA_PARTITION_GROUP_NAME);
  }

  public void setMembers(final Collection<AtomixTestNode> nodes) {
    cluster = buildCluster(nodes);
    systemPartitionGroup =
        buildPartitionGroup(RaftPartitionGroup.builder(SYSTEM_PARTITION_GROUP_NAME), nodes).build();
    dataPartitionGroup =
        builder
            .apply(
                buildPartitionGroup(AtomixPartitionGroup.builder(DATA_PARTITION_GROUP_NAME), nodes))
            .build();
    partitionService =
        createPartitionService(cluster.getMembershipService(), cluster.getCommunicationService());
  }

  public CompletableFuture<Void> startCluster() {
    return cluster.start();
  }

  public CompletableFuture<PartitionService> startPartitionService() {
    return partitionService.start();
  }

  public CompletableFuture<Void> start() {
    return startCluster()
        .thenCompose(ignored -> startPartitionService())
        .thenApply(ignored -> null);
  }

  public CompletableFuture<Void> stop() {
    return systemPartitionGroup
        .close()
        .thenCompose(ignored -> dataPartitionGroup.close())
        .thenCompose(ignored -> cluster.stop());
  }

  private AtomixCluster buildCluster(final Collection<AtomixTestNode> nodes) {
    return AtomixCluster.builder()
        .withAddress(node.address())
        .withClusterId(CLUSTER_ID)
        .withMembershipProvider(buildDiscoveryProvider(nodes))
        .withMemberId(getMemberId())
        .build();
  }

  private NodeDiscoveryProvider buildDiscoveryProvider(final Collection<AtomixTestNode> nodes) {
    return BootstrapDiscoveryProvider.builder()
        .withNodes(nodes.stream().map(AtomixTestNode::getNode).collect(Collectors.toList()))
        .build();
  }

  @SuppressWarnings("unchecked")
  private <T extends RaftPartitionGroup.Builder> T buildPartitionGroup(
      final T builder, final Collection<AtomixTestNode> nodes) {
    final Set<Member> members =
        nodes.stream().map(AtomixTestNode::getMember).collect(Collectors.toSet());
    members.add(member);

    return (T)
        builder
            .withDataDirectory(directory)
            .withMembers(members.toArray(new Member[0]))
            .withNumPartitions(1)
            .withPartitionSize(members.size())
            .withFlushOnCommit()
            .withStorageLevel(StorageLevel.DISK);
  }

  private ManagedPartitionService createPartitionService(
      final ClusterMembershipService clusterMembershipService,
      final ClusterCommunicationService messagingService) {
    final ClasspathScanningPrimitiveTypeRegistry registry =
        new ClasspathScanningPrimitiveTypeRegistry(this.getClass().getClassLoader());
    final List<ManagedPartitionGroup> partitionGroups =
        Collections.singletonList(dataPartitionGroup);

    return new DefaultPartitionService(
        clusterMembershipService,
        messagingService,
        new DefaultPrimitiveTypeRegistry(registry.getPrimitiveTypes()),
        systemPartitionGroup,
        partitionGroups,
        new DefaultPartitionGroupTypeRegistry(Collections.singleton(RaftPartitionGroup.TYPE)));
  }
}
