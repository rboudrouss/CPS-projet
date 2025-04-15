package com.rboud.cps;

import com.rboud.cps.components.Client;
import com.rboud.cps.components.Facade;
import com.rboud.cps.components.Node;
import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;
import com.rboud.cps.connections.endpoints.NodeNode.NodeNodeCompositeEndpoint;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.helpers.CVMDebugModes;

public class CVM extends AbstractCVM {

  public CVM() throws Exception {
    super();
  }

  @Override
  public void deploy() throws Exception {
    // AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
    // AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
    // AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
    // AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
    // AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
    // AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);

    FacadeClientDHTServicesEndpoint dhtServicesEndpoint = new FacadeClientDHTServicesEndpoint();
    NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint = new NodeFacadeCompositeEndpoint();

    NodeFacadeCompositeEndpoint nullendpoint = new NodeFacadeCompositeEndpoint();

    NodeNodeCompositeEndpoint nodeEndpoint12 = new NodeNodeCompositeEndpoint();
    NodeNodeCompositeEndpoint nodeEndpoint21 = new NodeNodeCompositeEndpoint();

    String node1URI = AbstractComponent.createComponent(
        Node.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), nodeEndpoint12.copyWithSharable(),
            nodeEndpoint21.copyWithSharable(), Integer.MIN_VALUE, 5 });

    String node2URI = AbstractComponent.createComponent(
        Node.class.getCanonicalName(),
        new Object[] { nullendpoint.copyWithSharable(), nodeEndpoint21.copyWithSharable(),
            nodeEndpoint12.copyWithSharable(), 6, Integer.MAX_VALUE });

    String facadeURI = AbstractComponent.createComponent(
        Facade.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), dhtServicesEndpoint.copyWithSharable() });

    String clientURI = AbstractComponent.createComponent(
        Client.class.getCanonicalName(),
        new Object[] { dhtServicesEndpoint.copyWithSharable() });

    super.deploy();
  }

  public static void main(String[] args) {
    try {
      CVM c = new CVM();
      c.startStandardLifeCycle(10000L);
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

}
