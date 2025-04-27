package com.rboud.cps;

import com.rboud.cps.components.AsyncFacade;
import com.rboud.cps.components.AsyncNode;
import com.rboud.cps.components.ExampleClientTester;
import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.NodeAsync.NodeFacadeCompositeEndpoint;
import com.rboud.cps.connections.endpoints.NodeNode.Async.NodeNodeCompositeEndpoint;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.helpers.CVMDebugModes;

/**
 * Component Virtual Machine (CVM) implementation for the DHT system.
 * This class handles the deployment and lifecycle management of all components:
 * - Multiple DHT nodes arranged in a ring topology
 * - A facade component that provides access to the DHT
 * - A client component for testing the DHT functionality
 *
 * The CVM creates and connects components using endpoints to establish
 * the communication channels between them.
 */
public class CVM extends AbstractCVM {

  /** Number of DHT nodes to create in the ring topology */
  private final int NODES = 2;

  /**
   * Creates a new CVM instance.
   * 
   * @throws Exception if initialization fails
   */
  public CVM() throws Exception {
    super();
  }

  /**
   * Entry point of the application.
   * Creates and starts the CVM with a standard component lifecycle.
   *
   * @param args command line arguments (not used)
   */
  public static void main(String[] args) {
    try {
      CVM c = new CVM();
      c.startStandardLifeCycle(100000L);
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  /**
   * Deploys all components of the DHT system.
   * Creates and connects:
   * - DHT nodes in a ring topology
   * - A facade component for DHT access
   * - A client component for testing
   *
   * @throws Exception if component deployment fails
   */
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
        AsyncFacade.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), dhtServicesEndpoint.copyWithSharable() });

    String clientURI = AbstractComponent.createComponent(
        ExampleClientTester.class.getCanonicalName(),
        new Object[] { dhtServicesEndpoint.copyWithSharable() });

    super.deploy();
  }

  /**
   * Creates and connects multiple DHT nodes in a ring topology.
   * Each node is assigned a range of hash values and is connected to its
   * neighbors in the ring.
   *
   * @param n              number of nodes to create
   * @param facadeEndpoint endpoint for connecting nodes to the facade
   * @return array of created node URIs
   * @throws Exception if node creation or connection fails
   */
  private String[] createAndConnectNodes(int n, NodeFacadeCompositeEndpoint facadeEndpoint) throws Exception {
    if (n <= 1) {
      NodeNodeCompositeEndpoint endpoint = new NodeNodeCompositeEndpoint();
      String nodeURI = AbstractComponent.createComponent(
          AsyncNode.class.getCanonicalName(),
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
          AsyncNode.class.getCanonicalName(),
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
        AsyncNode.class.getCanonicalName(),
        new Object[] { currentFacadeEndpoint.copyWithSharable(), oldEndpoint.copyWithSharable(),
            firstEndpoint.copyWithSharable(), Integer.MIN_VALUE + (step * (n - 1)) + 1, Integer.MAX_VALUE });

    return nodeURIs;
  }

}
