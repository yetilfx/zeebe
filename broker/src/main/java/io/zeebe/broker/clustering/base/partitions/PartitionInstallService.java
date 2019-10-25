/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.clustering.base.partitions;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.ATOMIX_JOIN_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.FOLLOWER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADERSHIP_SERVICE_GROUP;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.raftInstallServiceName;
import static io.zeebe.broker.clustering.base.partitions.PartitionServiceNames.followerPartitionServiceName;
import static io.zeebe.broker.clustering.base.partitions.PartitionServiceNames.leaderOpenLogStreamServiceName;
import static io.zeebe.broker.clustering.base.partitions.PartitionServiceNames.leaderPartitionServiceName;
import static io.zeebe.broker.clustering.base.partitions.PartitionServiceNames.partitionLeaderElectionServiceName;

import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.atomix.AtomixPositionBroadcaster;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.impl.service.LeaderOpenLogStreamAppenderService;
import io.zeebe.logstreams.impl.service.LogStreamServiceNames;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.slf4j.Logger;

/**
 * Service used to install the necessary services for creating a partition, namely logstream and
 * raft. Also listens to raft state changes (Leader, Follower) and installs the corresponding {@link
 * Partition} service(s) into the broker for other components (like client api or stream processing)
 * to attach to.
 */
public class PartitionInstallService extends Actor
    implements Service<Void>, PartitionRoleChangeListener {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private final Injector<Atomix> atomixInjector = new Injector<>();
  private final Injector<AtomixPositionBroadcaster> positionBroadcasterInjector = new Injector<>();
  private final StorageConfiguration configuration;
  private final int partitionId;
  private final BrokerCfg brokerCfg;
  private final RaftPartition partition;

  private ServiceStartContext startContext;
  private ServiceName<LogStream> logStreamServiceName;
  private ServiceName<Partition> leaderPartitionServiceName;
  private ServiceName<Partition> followerPartitionServiceName;
  private ServiceName<Void> leaderInstallRootServiceName;
  private String logName;
  private ActorFuture<PartitionLeaderElection> leaderElectionInstallFuture;
  private PartitionLeaderElection leaderElection;
  private ActorFuture<Void> transitionFuture;

  public PartitionInstallService(
      final RaftPartition partition,
      final StorageConfiguration configuration,
      final BrokerCfg brokerCfg) {
    this.partition = partition;
    this.configuration = configuration;
    this.partitionId = configuration.getPartitionId();
    this.brokerCfg = brokerCfg;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  public Injector<AtomixPositionBroadcaster> getPositionBroadcasterInjector() {
    return positionBroadcasterInjector;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    this.startContext = startContext;

    final int partitionId = configuration.getPartitionId();
    logName = Partition.getPartitionName(partitionId);

    // TODO: rename/remove?
    final ServiceName<Void> raftInstallServiceName = raftInstallServiceName(partitionId);

    final CompositeServiceBuilder partitionInstall =
        startContext.createComposite(raftInstallServiceName);

    logStreamServiceName = LogStreamServiceNames.logStreamServiceName(logName);
    leaderInstallRootServiceName = PartitionServiceNames.leaderInstallServiceRootName(logName);

    leaderElection = new PartitionLeaderElection(partition);
    final ServiceName<PartitionLeaderElection> partitionLeaderElectionServiceName =
        partitionLeaderElectionServiceName(logName);
    leaderElectionInstallFuture =
        partitionInstall
            .createService(partitionLeaderElectionServiceName, leaderElection)
            .dependency(atomixInjector.getInjectedServiceName(), leaderElection.getAtomixInjector())
            .dependency(ATOMIX_JOIN_SERVICE)
            .group(LEADERSHIP_SERVICE_GROUP)
            .install();
    final var logStreamService =
        LogStreams.createAtomixLogStream(partition)
            .logName(logName)
            .maxBlockSize(configuration.getMaxFragmentSize())
            .build();
    positionBroadcasterInjector
        .getValue()
        .setPositionListener(partition.name(), logStreamService::setCommitPosition);
    partitionInstall
        .createService(logStreamServiceName, logStreamService)
        .dependency(ATOMIX_JOIN_SERVICE)
        .install();

    partitionInstall.install();

    leaderPartitionServiceName = leaderPartitionServiceName(logName);
    followerPartitionServiceName = followerPartitionServiceName(logName);

    startContext.getScheduler().submitActor(this);
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    leaderElection.removeListener(this);
  }

  @Override
  public Void get() {
    return null;
  }

  @Override
  protected void onActorStarted() {
    actor.runOnCompletion(
        leaderElectionInstallFuture,
        (election, e) -> {
          if (e == null) {
            election.addListener(this);
          } else {
            LOG.error("Could not install leader election for partition {}", partitionId, e);
          }
        });
  }

  @Override
  protected void onActorClosing() {
    positionBroadcasterInjector.getValue().removePositionListener(partition.name());
  }

  private void transitionToLeader(final CompletableActorFuture<Void> transitionComplete) {
    actor.runOnCompletion(
        removeFollowerPartitionService(),
        (nothing, error) ->
            actor.runOnCompletion(
                installLeaderPartition(), (v, e) -> transitionComplete.complete(null)));
  }

  @Override
  public void onTransitionToFollower(final int partitionId) {
    actor.call(
        () -> {
          final CompletableActorFuture<Void> nextTransitionFuture = new CompletableActorFuture<>();
          if (transitionFuture != null && !transitionFuture.isDone()) {
            // wait until previous transition is complete
            actor.runOnCompletion(
                transitionFuture, (r, e) -> transitionToFollower(nextTransitionFuture));

          } else {
            transitionToFollower(nextTransitionFuture);
          }
          transitionFuture = nextTransitionFuture;
        });
  }

  @Override
  public void onTransitionToLeader(final int partitionId, final long term) {
    actor.call(
        () -> {
          final CompletableActorFuture<Void> nextTransitionFuture = new CompletableActorFuture<>();
          if (transitionFuture != null && !transitionFuture.isDone()) {
            // wait until previous transition is complete
            actor.runOnCompletion(
                transitionFuture, (r, e) -> transitionToLeader(nextTransitionFuture));

          } else {
            transitionToLeader(nextTransitionFuture);
          }
          transitionFuture = nextTransitionFuture;
        });
  }

  private void transitionToFollower(final CompletableActorFuture<Void> transitionComplete) {
    actor.runOnCompletion(
        removeLeaderPartitionService(),
        (nothing, error) ->
            actor.runOnCompletion(
                installFollowerPartition(), (ignored, err) -> transitionComplete.complete(null)));
  }

  private ActorFuture<Void> removeLeaderPartitionService() {
    LOG.debug("Removing leader partition services for partition {}", partitionId);
    return startContext.removeService(leaderInstallRootServiceName);
  }

  private ActorFuture<Void> installLeaderPartition() {
    LOG.debug("Installing leader partition service for partition {}", partitionId);
    final Partition leaderPartition =
        new Partition(configuration, brokerCfg, partitionId, RaftState.LEADER);

    final CompositeServiceBuilder composite =
        startContext.createComposite(leaderInstallRootServiceName);

    composite
        .createService(leaderPartitionServiceName, leaderPartition)
        .dependency(atomixInjector.getInjectedServiceName(), leaderPartition.getAtomixInjector())
        .dependency(logStreamServiceName, leaderPartition.getLogStreamInjector())
        .group(LEADER_PARTITION_GROUP_NAME)
        .install();

    final var openAppenderService = new LeaderOpenLogStreamAppenderService();
    composite
        .createService(leaderOpenLogStreamServiceName(logName), openAppenderService)
        .dependency(logStreamServiceName, openAppenderService.getLogStreamInjector())
        .dependency(leaderPartitionServiceName)
        .install();

    return composite.install();
  }

  private ActorFuture<Partition> installFollowerPartition() {
    LOG.debug("Installing follower partition service for partition {}", partitionId);
    final Partition followerPartition =
        new Partition(configuration, brokerCfg, partitionId, RaftState.FOLLOWER);

    return startContext
        .createService(followerPartitionServiceName, followerPartition)
        .dependency(atomixInjector.getInjectedServiceName(), followerPartition.getAtomixInjector())
        .dependency(logStreamServiceName, followerPartition.getLogStreamInjector())
        .group(FOLLOWER_PARTITION_GROUP_NAME)
        .install();
  }

  private ActorFuture<Void> removeFollowerPartitionService() {
    LOG.debug("Removing follower partition service for partition {}", partitionId);
    return startContext.removeService(followerPartitionServiceName);
  }
}
