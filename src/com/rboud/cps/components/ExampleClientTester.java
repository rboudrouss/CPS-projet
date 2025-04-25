package com.rboud.cps.components;

import com.rboud.cps.tests.Tester;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;

@RequiredInterfaces(required = { DHTServicesCI.class })
public class ExampleClientTester extends AbstractComponent {

  EndPointI<DHTServicesI> dhtServicesEndpoint;

  protected ExampleClientTester(EndPointI<DHTServicesI> dhtServicesEndpoint) throws Exception {
    super(1, 0);

    this.dhtServicesEndpoint = dhtServicesEndpoint;

    this.toggleLogging();
    this.toggleTracing();
    this.getTracer().setTitle("Client");
  }

  // ------------------------------------------------------------------------
  // Component lifecycle methods
  // ------------------------------------------------------------------------

  @Override
  public synchronized void start() throws ComponentStartException {
    this.logMessage("[CLIENT] Starting client component.");
    try {
      this.dhtServicesEndpoint.initialiseClientSide(this);
    } catch (Exception e) {
      throw new ComponentStartException(e);
    }
    super.start();
  }

  @Override
  public synchronized void execute() throws Exception {
    super.execute();

    Tester tester = new Tester(this.getDHTServices(), this::logMessage);

    tester.disableRandomTests();

    tester.allTesting();
  }

  @Override
  public synchronized void finalise() throws Exception {
    this.logMessage("[CLIENT] Finalising client component.");
    this.printExecutionLogOnFile("logs/client");

    this.dhtServicesEndpoint.cleanUpClientSide();

    super.finalise();
  }

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------

  private DHTServicesI getDHTServices() {
    return this.dhtServicesEndpoint.getClientSideReference();
  }
}
