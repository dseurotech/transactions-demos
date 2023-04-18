package com.eurotech.demos.transactionsless;

import com.eurotech.demos.transactions.DemoEntity;
import org.eclipse.kapua.commons.jpa.AbstractEntityManagerFactory;
import org.eclipse.kapua.commons.jpa.DemoDAO;
import org.eclipse.kapua.commons.jpa.EntityManagerSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConcurrentUpdateDelete {
    final EntityManagerSession ems = new EntityManagerSession(new AbstractEntityManagerFactory("demos") {
    });

    /**
     * This demonstrates the interaction between two different threads (T1 and T2) concurrently operating on the same entity,
     * using the current Kapua model (no transaction management)
     * <p>
     * T1 starts first and fetches the entity, then waits 2 seconds before deleting it
     * T2 starts after 1 second, immediately fetches the entity and updates it
     * the Main thread checks the value of the entity at different points in time
     * <p>
     * This utility method is meant to be invoked by the test {@link #demoConcurrentUpdateDelete()}
     *
     * @param t1Throws Whether T1 should throw just before completing the transaction (demonstating the behaviour of the system in case of rollbacks)
     * @return The final state of the entity, fetched by the main thread after all other threads are complete
     * @throws InterruptedException never, really
     */
    private DemoEntity doDemoConcurrentUpdateDelete(boolean t1Throws) throws InterruptedException {
        final DemoEntity initialEntity = Utils.createEntity(ems, "Entity Content");
        Thread t1 = new Thread(() -> {
            Utils.print("T1", "started");
            final DemoEntity t1Found = Utils.fetchAndPrint(ems, "T1", initialEntity.getId());
            Utils.sleep(2000);
            ems.doTransactedAction(e -> DemoDAO.delete(e, t1Found.getId()));
            Utils.print("T1", "deletes entity");
            if (t1Throws) {
                Utils.print("T1", "Goes baboom!");
                throw new RuntimeException("BABOOM!!");
            }
            Utils.fetchAndPrint(ems, "T1", initialEntity.getId());
        });
        Thread t2 = new Thread(() -> {
            Utils.print("T2", "started");
            final DemoEntity t2Found = Utils.fetchAndPrint(ems, "T2", initialEntity.getId());
            if (t2Found == null) {
                Utils.print("T2", "notices null entity and exits");
                return;
            }
            t2Found.setContent(t2Found.getContent() + " plus T2");
            ;
            ems.doTransactedAction(e -> DemoDAO.update(e, t2Found));
            Utils.print("T2", "changed entity content");
            Utils.fetchAndPrint(ems, "T2", initialEntity.getId());
        });
        t1.start();
        Utils.sleep(1000);
        t2.start();

        Utils.fetchAndPrint(ems, "MAIN", initialEntity.getId());
        t1.join();
        Utils.print("MAIN", "T1 joined");
        Utils.fetchAndPrint(ems, "MAIN", initialEntity.getId());
        t2.join();
        Utils.print("MAIN", "T2 joined");
        return Utils.fetchAndPrint(ems, "MAIN", initialEntity.getId());
    }

    /**
     * This executes {@link #doDemoConcurrentUpdateDelete(boolean)} without and with T1 failing.
     * The two threads are not isolated from each other, and the last to complete overrides all the changes from the other
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoConcurrentUpdateDelete() throws InterruptedException {
        { //T1 NOT throwing
            DemoEntity res = doDemoConcurrentUpdateDelete(false);
            //WRONG: T2 completed correctly (did not see the entity deleted), where is the updated entity?
            Assertions.assertNull(res);
        }
        System.out.println("\n\n\n");
        { //T1 DOES throw
            DemoEntity res = doDemoConcurrentUpdateDelete(true);
            //WRONG: T1 failed, and yet the entity is deleted
            Assertions.assertNull(res);
        }
    }
}
