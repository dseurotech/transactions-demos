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

    private DemoEntity doDemoConcurrentUpdateDelete(boolean t1Throws) throws InterruptedException {
        final DemoEntity initialEntity = Utils.createEntity(ems, "Initial content");
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
            t2Found.setContent("Content from T2");
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

    @Test
    public void demoConcurrentUpdateDelete() throws InterruptedException {
        {
            DemoEntity res = doDemoConcurrentUpdateDelete(false);
            //WRONG: T2 completed correctly, where is the updated entity?
            Assertions.assertNull(res);
        }
        System.out.println("\n\n\n");
        {
            DemoEntity res = doDemoConcurrentUpdateDelete(true);
            //WRONG: T1 failed, and yet the entity is deleted
            Assertions.assertNull(res);
        }
    }
}
