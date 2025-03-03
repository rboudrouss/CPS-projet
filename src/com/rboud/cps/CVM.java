package com.rboud.cps;

import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;
import com.rboud.cps.connections.endpoints.NodeNode.NodeNodeCompositeEndpoint;
import com.rboud.cps.core.Client;
import com.rboud.cps.core.DHTFacade;
import com.rboud.cps.core.DHTNode;

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
    NodeNodeCompositeEndpoint nodeNodeCompositeEndpoint = new NodeNodeCompositeEndpoint();

    String nodeURI = AbstractComponent.createComponent(
        DHTNode.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), nodeNodeCompositeEndpoint.copyWithSharable() });

    String facadeURI = AbstractComponent.createComponent(
        DHTFacade.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), dhtServicesEndpoint.copyWithSharable() });

    String clientURI = AbstractComponent.createComponent(
        Client.class.getCanonicalName(),
        new Object[] { dhtServicesEndpoint.copyWithSharable() });
    
    // assert nodeFacadeCompositeEndpoint.serverSideInitialised();
    // assert dhtServicesEndpoint.serverSideInitialised();
    // assert dhtServicesEndpoint.clientSideInitialised();
    // assert nodeFacadeCompositeEndpoint.clientSideInitialised();

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
