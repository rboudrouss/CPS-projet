package com.rboud.cps;

import com.rboud.cps.client.LoadTester;
import com.rboud.cps.connections.DHTContentAccessConnector;
import com.rboud.cps.core.DHTNode;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;

public class CVM extends AbstractCVM {

  public CVM() throws Exception {
    super();
  }


  @Override
  public void deploy() throws Exception {
    String DHTNodeURI = AbstractComponent.createComponent(DHTNode.class.getCanonicalName(), new Object[] {});
    String clientURI = AbstractComponent.createComponent(LoadTester.class.getCanonicalName(), new Object[] {});

    this.doPortConnection(
      clientURI,
      LoadTester.CONTENT_ACCESS_URI,
      DHTNode.CONTENT_ACCESS_INBOUND_PORT_URI,
      DHTContentAccessConnector.class.getCanonicalName()
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
