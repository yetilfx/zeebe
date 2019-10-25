package io.zeebe.broker.clustering.atomix;

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStopContext;

/** Stupid value holder service so it can be shared and used across services. */
public class AtomixPositionBroadcasterService implements Service<AtomixPositionBroadcaster> {
  private final AtomixPositionBroadcaster positionBroadcaster;

  public AtomixPositionBroadcasterService(final AtomixPositionBroadcaster positionBroadcaster) {
    this.positionBroadcaster = positionBroadcaster;
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    positionBroadcaster.removeAllPositionListeners();
  }

  @Override
  public AtomixPositionBroadcaster get() {
    return positionBroadcaster;
  }
}
