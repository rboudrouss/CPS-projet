package com.rboud.cps;

import com.rboud.cps.connections.connectors.DHTContentAccessConnector;
import com.rboud.cps.connections.connectors.DHTMapReduceConnector;
import com.rboud.cps.core.DHTNode;
import com.rboud.cps.core.Client;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;

public class CVM extends AbstractCVM {

  public CVM() throws Exception {
    super();
  }

  @Override
  public void deploy() throws Exception {
    String DHTNodeURI = AbstractComponent.createComponent(DHTNode.class.getCanonicalName(), new Object[] {});
    String clientURI = AbstractComponent.createComponent(Client.class.getCanonicalName(), new Object[] {});

    this.doPortConnection(
        clientURI,
        Client.CONTENT_ACCESS_URI,
        DHTNode.CONTENT_ACCESS_INBOUND_PORT_URI,
        DHTContentAccessConnector.class.getCanonicalName() // formatting hack
    );

    this.doPortConnection(
        clientURI,
        Client.MAP_REDUCE_URI,
        DHTNode.MAP_REDUCE_INBOUND_PORT_URI,
        DHTMapReduceConnector.class.getCanonicalName() // formatting hack
    );

    super.deploy();
  }

  public static void main(String[] args) {
    try {
      CVM c = new CVM();
      c.startStandardLifeCycle(10000L);
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
