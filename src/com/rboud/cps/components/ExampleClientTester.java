package com.rboud.cps.components;

import com.rboud.cps.tests.Tester;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;

/**
 * A test client component for the DHT service that runs various tests to verify
 * the functionality of the distributed hash table.
 * 
 * This component implements a test suite for the DHT services by connecting
 * to a DHT endpoint and executing various test scenarios.
 */
@RequiredInterfaces(required = { DHTServicesCI.class })
public class ExampleClientTester extends AbstractComponent {

  /** The endpoint to access the facade */
  EndPointI<DHTServicesI> dhtServicesEndpoint;

  /**
   * Creates a new ExampleClientTester component.
   * 
   * @param dhtServicesEndpoint The endpoint providing access to DHT services
   * @throws Exception if the component initialization fails
   */
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

  /**
   * Starts the client component by initializing the connection to the DHT
   * service.
   * 
   * @throws ComponentStartException if the component fails to start properly
   */
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

  /**
   * Executes the test suite on the DHT service.
   * This method runs a series of tests to verify the DHT functionality,
   * with random testing disabled.
   * 
   * @throws Exception if any test execution fails
   */
  @Override
  public synchronized void execute() throws Exception {
    super.execute();

    Tester tester = new Tester(this.getDHTServices(), this::logMessage);

    tester.disableRandomTests();

    tester.allTesting();
  }

  /**
   * Finalizes the component by cleaning up resources and connections.
   * Writes execution logs to a file before shutting down.
   * 
   * @throws Exception if cleanup operations fail
   */
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

  /**
   * Helper method to get the DHT services reference.
   * 
   * @return the client-side reference to the DHT services
   */
  private DHTServicesI getDHTServices() {
    return this.dhtServicesEndpoint.getClientSideReference();
  }
}
