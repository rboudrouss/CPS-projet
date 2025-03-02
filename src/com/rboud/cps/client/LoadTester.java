package com.rboud.cps.client;

import com.rboud.cps.ports.ClientContentAccessOutboudPort;
import com.rboud.cps.ports.ClientMapReduceOutboundPort;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class LoadTester extends AbstractComponent {

  protected ClientContentAccessOutboudPort contentAccessOutboundPort;
  public static final String CONTENT_ACCESS_URI = "content-access-uri";

  protected ClientMapReduceOutboundPort mapReduceOutboundPort;
  public static final String MAP_REDUCE_URI = "map-reduce-uri";

  protected LoadTester() {
    super(1, 0);
    try {
      this.contentAccessOutboundPort = new ClientContentAccessOutboudPort(CONTENT_ACCESS_URI, this);
      this.contentAccessOutboundPort.publishPort();

      this.mapReduceOutboundPort = new ClientMapReduceOutboundPort(MAP_REDUCE_URI, this);
      this.mapReduceOutboundPort.publishPort();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    this.toggleLogging();
    this.toggleTracing();
  }

  @Override
  public synchronized void execute() throws Exception {
    super.execute();

    for (int i = 0; i < 10; i++) {
      Personne temp = Personne.getRandomPersonne();
      this.contentAccessOutboundPort.putSync(CONTENT_ACCESS_URI, temp.getId(), temp);
    }

    Personne temp = (Personne) this.contentAccessOutboundPort.getSync(CONTENT_ACCESS_URI, new Id(2));
    this.logMessage("Personne with id 2: " + temp);

    this.mapReduceOutboundPort.mapSync(
        MAP_REDUCE_URI,
        (_) -> true,
        p -> p.getValue("AGE") // formatting hack
    );
    Integer out = this.mapReduceOutboundPort.reduceSync(
        MAP_REDUCE_URI,
        (a, b) -> a + (int) b,
        (a, b) -> a + b,
        0 // formatting hack
    );

    this.logMessage("Sum of ages: " + out);
  }

  @Override
  public synchronized void finalize() throws Exception {
    this.doPortDisconnection(CONTENT_ACCESS_URI);
    this.doPortDisconnection(MAP_REDUCE_URI);
    super.finalise();
  }

  @Override
  public synchronized void shutdown() throws ComponentShutdownException {
    try {
      this.contentAccessOutboundPort.unpublishPort();
      this.mapReduceOutboundPort.unpublishPort();
    } catch (Exception e) {
      throw new ComponentShutdownException(e);
    }
    super.shutdown();
  }

}
