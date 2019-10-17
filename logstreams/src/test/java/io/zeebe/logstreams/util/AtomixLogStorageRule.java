package io.zeebe.logstreams.util;

import io.atomix.protocols.raft.partition.RaftPartitionGroup.Builder;
import io.zeebe.logstreams.impl.storage.atomix.AtomixLogStorage;
import io.zeebe.test.util.atomix.AtomixTestNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.function.UnaryOperator;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class AtomixLogStorageRule extends ExternalResource {
  private final File directory;
  private final AtomixTestNode node;

  private AtomixLogStorage storage;

  public AtomixLogStorageRule(final TemporaryFolder temporaryFolder) {
    this(temporaryFolder, UnaryOperator.identity());
  }

  public AtomixLogStorageRule(
      final TemporaryFolder temporaryFolder, final UnaryOperator<Builder> builder) {
    this.directory = newFolder(temporaryFolder);
    this.node = new AtomixTestNode(0, directory, builder);
    this.node.setMembers(Collections.singleton(this.node));
  }

  @Override
  protected void before() throws Throwable {
    node.startCluster().join();
  }

  @Override
  protected void after() {
    node.stop();
  }

  public AtomixLogStorage get() {
    return storage;
  }

  public File getDirectory() {
    return directory;
  }

  public AtomixTestNode getNode() {
    return node;
  }

  private File newFolder(final TemporaryFolder provider) {
    try {
      return provider.newFolder("node-0");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
