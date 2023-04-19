package com.eurotech.demos.transactionsless;

import com.eurotech.demos.transactions.DemoEntity;
import com.eurotech.demos.transactions.NonVersionedEntity;
import com.eurotech.demos.transactions.VersionedEntity;
import org.eclipse.kapua.commons.jpa.AbstractEntityManagerFactory;
import org.eclipse.kapua.commons.jpa.DemoEntityDAO;
import org.eclipse.kapua.commons.jpa.EntityManagerSession;
import org.javatuples.Triplet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

public class ConcurrentUpdates {
    final EntityManagerSession ems = new EntityManagerSession(new AbstractEntityManagerFactory("demos") {
    });

    /**
     * This demonstrates the interaction between two different threads (T1 and T2) concurrently updating the same entity,
     * using the current Kapua model (no transaction management)
     * <p>
     * T1 starts first and fetches the entity, then waits for some time (configurable) before updating it
     * T2 starts after 1 second, immediately fetches the entity and updates it after 500ms
     * the Main thread checks the value of the entity at different points in time
     * <p>
     * This utility method is meant to be invoked by the test below with different paremeters
     *
     * @param t1Throws Whether T1 should throw just before completing the transaction (demonstating the behaviour of the system in case of rollbacks)
     * @return The final state of the entity, fetched by the main thread after all other threads are complete
     * @throws InterruptedException never, really
     */
    private <E extends DemoEntity> Triplet<DemoEntity, Boolean, Boolean> doDemoConcurrentUpdate(
            Function<String, E> entityCreator,
            Class<E> clazz,
            int t1SleepBeforeUpdate,
            boolean t1Throws) throws InterruptedException {
        System.out.println("\n\n* Concurrent update demo without transactions **************************************");
        Utils.print("TEST", String.format("class: %s,  t1 throws: %s", clazz.getSimpleName(), t1Throws));
        final DemoEntityDAO<E> demoEntityDAO = new DemoEntityDAO<>(clazz);
        final DemoEntity initialEntity = ems.doTransactedAction(e -> demoEntityDAO.create(e, entityCreator.apply("Entity Content")));
        Updater t1 = new Updater<>("T1", demoEntityDAO, initialEntity.getId(), ems, t1SleepBeforeUpdate, t1Throws);
        Updater t2 = new Updater<>("T2", demoEntityDAO, initialEntity.getId(), ems, 500, false);
        t1.start();
        Utils.sleep(1000);
        t2.start();

        Utils.fetchAndPrint(ems, demoEntityDAO, "MAIN", initialEntity.getId());
        t1.join();
        Utils.print("MAIN", "T1 joined");
        Utils.fetchAndPrint(ems, demoEntityDAO, "MAIN", initialEntity.getId());
        t2.join();
        Utils.print("MAIN", "T2 joined");
        return Triplet.with(
                Utils.fetchAndPrint(ems, demoEntityDAO, "MAIN", initialEntity.getId()),
                t1.threadFailed.get(),
                t2.threadFailed.get());
    }

    @Test
    public void t2finishesFirst() throws InterruptedException {
        {//Non versioned: T1 overrules T2 (no failures)
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 2000, false);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {//Versioned: T2 wins, T1 fails
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(VersionedEntity::newEntity, VersionedEntity.class, 2000, false);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void t1finishesFirst() throws InterruptedException {
        {//Non versioned: T2 overrules T1 (no failures)
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 1250, false);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertTrue(res.getValue0().getContent().contains("T2"));
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {//Versioned: T1 wins, T2 fails
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(VersionedEntity::newEntity, VersionedEntity.class, 1250, false);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertTrue(res.getValue0().getContent().contains("T1"));
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
    }

    /**
     * This executes {@link #doDemoConcurrentUpdate(Function, Class, int, boolean)}
     * The two threads are not isolated from each other, and the last to complete overrides all the changes from the other
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void t2finishesFirst_withT1Failing() throws InterruptedException {
        {//Non versioned: T1 overrides T2 (despite failing), T2 does not fail but is not applied
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 2000, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        {//Versioned: T2 overrides T1, T1 fails even before being ordered to do sp
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(VersionedEntity::newEntity, VersionedEntity.class, 2000, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void t1finishesFirst_withT1Failing() throws InterruptedException {
        {//Non versioned: T2 overrides T1
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 1250, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertTrue(res.getValue0().getContent().contains("T2"));
            Assertions.assertFalse(res.getValue2());
        }
        {//Versioned: T1 deletes (despite failing), T2 fails
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentUpdate(VersionedEntity::newEntity, VersionedEntity.class, 1250, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertTrue(res.getValue0().getContent().contains("T1"));
            Assertions.assertTrue(res.getValue2());
        }
    }

}
