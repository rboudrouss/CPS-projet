package com.rboud.cps.components;

import com.rboud.cps.utils.Id;
import com.rboud.cps.utils.Personne;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;

@RequiredInterfaces(required = { DHTServicesCI.class })
public class Client extends AbstractComponent {

  EndPointI<DHTServicesI> dhtServicesEndpoint;

  protected Client(EndPointI<DHTServicesI> dhtServicesEndpoint) throws Exception {
    super(1, 0);

    this.dhtServicesEndpoint = dhtServicesEndpoint;

    this.toggleLogging();
    this.toggleTracing();
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

    final boolean USE_INT_ID = true;

    Personne temp = Personne.getRandomPersonne();
    for (int i = 0; i < 10; i++) {
      temp = Personne.getRandomPersonne();
      this.logMessage("[CLIENT] Putting Personne: " + temp);
      if (USE_INT_ID)
        this.getDHTServices().put(temp.getId(), temp);
      else
        this.getDHTServices().put(temp.getNameId(), temp);
      this.logMessage("[CLIENT] Personne put.");
    }

    this.logMessage("[CLIENT] Getting Personne with id 2.");
    Personne temp2 = (Personne) this.getDHTServices().get(new Id(2));
    this.logMessage("[CLIENT] got personne with id 2: " + temp2);

    this.logMessage("[CLIENT] Getting personne with nameid " + temp.getNameId());
    temp2 = (Personne) this.getDHTServices().get(temp.getNameId());
    this.logMessage("[CLIENT] got personne with nameid " + temp.getNameId() + ": " + temp2);

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

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------

  private DHTServicesI getDHTServices() {
    return this.dhtServicesEndpoint.getClientSideReference();
  }
}
