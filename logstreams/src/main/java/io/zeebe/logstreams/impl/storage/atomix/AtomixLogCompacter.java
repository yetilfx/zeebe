package io.zeebe.logstreams.impl.storage.atomix;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface AtomixLogCompacter {
  CompletableFuture<Void> compact(final long index, final long term);
}
