package com.rboud.cps;

import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.connections.endpoints.NodeFacade.NodeFacadeCompositeEndpoint;
import com.rboud.cps.core.Client;
import com.rboud.cps.core.DHTFacade;
import com.rboud.cps.core.DHTNode;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;

public class CVM extends AbstractCVM {

  public CVM() throws Exception {
    super();
  }

  @Override
  public void deploy() throws Exception {

    FacadeClientDHTServicesEndpoint dhtServicesEndpoint = new FacadeClientDHTServicesEndpoint();
    NodeFacadeCompositeEndpoint nodeFacadeCompositeEndpoint = new NodeFacadeCompositeEndpoint();

    String nodeURI = AbstractComponent.createComponent(
        DHTNode.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable() });

    String facadeURI = AbstractComponent.createComponent(
        DHTFacade.class.getCanonicalName(),
        new Object[] { nodeFacadeCompositeEndpoint.copyWithSharable(), dhtServicesEndpoint.copyWithSharable() });

    String clientURI = AbstractComponent.createComponent(
        Client.class.getCanonicalName(),
        new Object[] { dhtServicesEndpoint.copyWithSharable() });
    

    
    assert dhtServicesEndpoint.clientSideInitialised() && dhtServicesEndpoint.serverSideInitialised();
    assert nodeFacadeCompositeEndpoint.clientSideInitialised() && nodeFacadeCompositeEndpoint.serverSideInitialised();

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
