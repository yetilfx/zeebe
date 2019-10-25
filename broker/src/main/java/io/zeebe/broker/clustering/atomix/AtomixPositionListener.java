package io.zeebe.broker.clustering.atomix;

@FunctionalInterface
public interface AtomixPositionListener {
  void acceptPosition(final long position);
}
