package io.zeebe.logstreams.storage.atomix;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface AtomixLogCompacter {
  CompletableFuture<Void> compact(final long index, final long term);
}
