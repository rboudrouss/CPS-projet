package com.rboud.cps.core;

import com.rboud.cps.connections.endpoints.FacadeClient.FacadeClientDHTServicesEndpoint;
import com.rboud.cps.utils.Id;
import com.rboud.cps.utils.Personne;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

@RequiredInterfaces(required = { DHTServicesCI.class })
public class Client extends AbstractComponent {

  FacadeClientDHTServicesEndpoint dhtServicesEndpoint;

  protected Client(FacadeClientDHTServicesEndpoint dhtServicesEndpoint) {
    super(1, 0);

    this.dhtServicesEndpoint = dhtServicesEndpoint;

    this.toggleLogging();
    this.toggleTracing();
  }

  private DHTServicesCI getDHTServices() {
    return this.dhtServicesEndpoint.getClientSideReference();
  }

  @Override
  public synchronized void start() throws ComponentStartException {
    this.logMessage("[CLIENT] Starting client component.");
    assert this.dhtServicesEndpoint.serverSideInitialised();

    try {
      this.dhtServicesEndpoint.initialiseClientSide(this);
    } catch (Exception e) {
      throw new ComponentStartException(e);
    }
    this.logMessage("[CLIENT] Client component started.");
    super.start();
  }

  @Override
  public synchronized void execute() throws Exception {
    super.execute();

    for (int i = 0; i < 10; i++) {
      Personne temp = Personne.getRandomPersonne();
      this.logMessage("[CLIENT] Putting Personne: " + temp);
      System.out.println(this.getDHTServices());
      this.getDHTServices().put(temp.getId(), temp);
      this.logMessage("[CLIENT] Personne put.");
    }

    this.logMessage("[CLIENT] Getting Personne with id 2.");
    Personne temp = (Personne) this.getDHTServices().get(new Id(2));
    this.logMessage("[CLIENT] got personne with id 2: " + temp);

    this.logMessage("[CLIENT] getting sum of ages using mapReduce.");
    int out = this.getDHTServices().mapReduce(
        (a) -> true,
        (a) -> a.getValue("AGE"),
        (a, b) -> a + (int) b,
        (a, b) -> a + b,
        0);

    this.logMessage("[CLIENT] got Sum of ages: " + out);
  }

  @Override
  public synchronized void finalise() throws Exception {
    this.logMessage("[CLIENT] Finalising client component.");
    this.printExecutionLogOnFile("logs/client");

    this.dhtServicesEndpoint.cleanUpClientSide();

    super.finalise();
  }

  
}
