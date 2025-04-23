package com.rboud.cps;

import com.rboud.cps.components.ExampleClientTester;
import com.rboud.cps.components.SyncFacade;
import com.rboud.cps.components.SyncNode;
import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;
import com.rboud.cps.connections.endpoints.NodeNode.NodeNodeCompositeEndpoint;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.helpers.CVMDebugModes;

public class CVM extends AbstractCVM {

  private final int NODES = 2;

  public CVM() throws Exception {
    super();
  }

  // ------------------------------------------------------------------------
  // Main method
  // ------------------------------------------------------------------------

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

  // ------------------------------------------------------------------------
  // life cycle methods
  // ------------------------------------------------------------------------

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

    String[] nodeURIs = createAndConnectNodes(NODES, nodeFacadeCompositeEndpoint);

    String facadeURI = AbstractComponent.createComponent(
        SyncFacade.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), dhtServicesEndpoint.copyWithSharable() });

    String clientURI = AbstractComponent.createComponent(
        ExampleClientTester.class.getCanonicalName(),
        new Object[] { dhtServicesEndpoint.copyWithSharable() });

    super.deploy();
  }

  // ------------------------------------------------------------------------
  // Helper methods
  // ------------------------------------------------------------------------

  private String[] createAndConnectNodes(int n, NodeFacadeCompositeEndpoint facadeEndpoint) throws Exception {
    if (n <= 1) {
      NodeNodeCompositeEndpoint endpoint = new NodeNodeCompositeEndpoint();
      String nodeURI = AbstractComponent.createComponent(
          SyncNode.class.getCanonicalName(),
          new Object[] { facadeEndpoint.copyWithSharable(), endpoint.copyWithSharable(), endpoint.copyWithSharable(),
              Integer.MIN_VALUE, Integer.MAX_VALUE });
      return new String[] { nodeURI };
    }

    int step = (int) (((long) Integer.MAX_VALUE) * 2L / (long) n);

    NodeFacadeCompositeEndpoint currentFacadeEndpoint = facadeEndpoint;

    NodeNodeCompositeEndpoint firstEndpoint = new NodeNodeCompositeEndpoint();

    NodeNodeCompositeEndpoint oldEndpoint = firstEndpoint, newEndpoint = new NodeNodeCompositeEndpoint();

    String[] nodeURIs = new String[n];

    for (int i = 0; i < n - 1; i++) {
      nodeURIs[i] = AbstractComponent.createComponent(
          SyncNode.class.getCanonicalName(),
          new Object[] { currentFacadeEndpoint.copyWithSharable(), oldEndpoint.copyWithSharable(),
              newEndpoint.copyWithSharable(), Integer.MIN_VALUE + (step * i) + (i == 0 ? 0 : 1),
              Integer.MIN_VALUE + step * (i + 1) });

      oldEndpoint = newEndpoint;
      newEndpoint = new NodeNodeCompositeEndpoint();

      // Creating fake empty endpoint since we cannot pass null
      currentFacadeEndpoint = new NodeFacadeCompositeEndpoint();
    }

    // last node must connect to the first node
    nodeURIs[n - 1] = AbstractComponent.createComponent(
        SyncNode.class.getCanonicalName(),
        new Object[] { currentFacadeEndpoint.copyWithSharable(), oldEndpoint.copyWithSharable(),
            firstEndpoint.copyWithSharable(), Integer.MIN_VALUE + (step * (n - 1)) + 1, Integer.MAX_VALUE });

    return nodeURIs;
  }

}
