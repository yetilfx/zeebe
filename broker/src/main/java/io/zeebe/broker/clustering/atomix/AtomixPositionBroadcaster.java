package io.zeebe.broker.clustering.atomix;

public interface AtomixPositionBroadcaster {
  void setPositionListener(final String raftName, final AtomixPositionListener listener);

  void removePositionListener(final String raftName);

  void removeAllPositionListeners();

  void notifyPositionListener(final String raftName, final long position);
}
