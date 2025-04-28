// Nothing to see here yet

package com.rboud.cps;

import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;

public class DistributedCVM extends AbstractDistributedCVM {

  public DistributedCVM(String[] args) throws Exception {
    super(args);
  }

  public static void main(String[] args) {
    try {
      DistributedCVM dcvm = new DistributedCVM(args);
      dcvm.startStandardLifeCycle(100000L);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void instantiateAndPublish() throws Exception {
    super.instantiateAndPublish();
  }

  @Override
  public void interconnect() throws Exception {
    super.interconnect();
  }

}
