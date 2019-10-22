package io.zeebe.logstreams.impl.atomix;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamRootServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamServiceName;
import static io.zeebe.util.EnsureUtil.ensureGreaterThanOrEqual;

import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.impl.log.fs.FsLogStorage;
import io.zeebe.logstreams.impl.log.fs.FsLogStorageConfiguration;
import io.zeebe.logstreams.impl.service.FsLogStorageService;
import io.zeebe.logstreams.impl.service.LogStreamService;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.File;
import java.util.Objects;
import java.util.function.Function;
import org.agrona.concurrent.status.AtomicLongPosition;

public class AtomixLogStreamBuilder {
//  private final AtomicLongPosition commitPosition = new AtomicLongPosition();
//  private final ActorConditions onCommitPositionUpdatedConditions = new ActorConditions();
//
//  private String name;
//  private RaftPartition partition;
//  private ServiceContainer serviceContainer;
//
//  public AtomixLogStreamBuilder name(String name) {
//    this.name = name;
//    return this;
//  }
//
//  public AtomixLogStreamBuilder partition(final RaftPartition partition) {
//    this.partition = partition;
//    return this;
//  }
//
//  public AtomixLogStreamBuilder serviceContainer(final ServiceContainer serviceContainer) {
//    this.serviceContainer = serviceContainer;
//    return this;
//  }
//
//  public ActorFuture<LogStream> build() {
//    validate();
//
//    final var serviceName = logStreamServiceName(name);
//    final var logStream = new AtomixLogStream(partition);
//
//    return serviceContainer.createService(serviceName, logStream).install();
//  }
//
//  private void validate() {
//    Objects.requireNonNull(name, "name");
//    Objects.requireNonNull(partition, "partition");
////    Objects.requireNonNull(dispatcher, "dispatcher");
////
////    if (logSegmentSize < maxFragmentSize) {
////      throw new IllegalArgumentException(
////        String.format(
////          "Expected the log segment size greater than the max fragment size of %s, but was %s.",
////          ByteValue.ofBytes(maxFragmentSize), ByteValue.ofBytes(logSegmentSize)));
////    }
//  }
}
