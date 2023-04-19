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

public class ConcurrentUpdateDelete {
    final EntityManagerSession ems = new EntityManagerSession(new AbstractEntityManagerFactory("demos") {
    });

    private <E extends DemoEntity> Triplet<DemoEntity, Boolean, Boolean> doDemoConcurrentDeleteUpdate(
            Function<String, E> entityCreator,
            Class<E> clazz,
            int t1SleepBeforeUpdate,
            boolean t1Throws) throws InterruptedException {
        System.out.println("\n\n* Concurrent update demo without transactions **************************************");
        Utils.print("TEST", String.format("class: %s,  t1 throws: %s", clazz.getSimpleName(), t1Throws));
        final DemoEntityDAO<E> demoEntityDAO = new DemoEntityDAO<>(clazz);
        final DemoEntity initialEntity = ems.doTransactedAction(e -> demoEntityDAO.create(e, entityCreator.apply("Entity Content")));
        Deleter t1 = new Deleter<>("T1", demoEntityDAO, initialEntity.getId(), ems, t1SleepBeforeUpdate, t1Throws);
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

    //

    /**
     * This executes {@link #doDemoConcurrentDeleteUpdate(Function, Class, int, boolean)}
     * The two threads are not isolated from each other, and the last to complete overrides all the changes from the other
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void t2finishesFirst() throws InterruptedException {
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 2000, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(VersionedEntity::newEntity, VersionedEntity.class, 2000, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void t1finishesFirst() throws InterruptedException {
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 1250, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(VersionedEntity::newEntity, VersionedEntity.class, 1250, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
    }

    /**
     * This executes {@link #doDemoConcurrentDeleteUpdate(Function, Class, int, boolean)}
     * The two threads are not isolated from each other, and the last to complete overrides all the changes from the other
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void t2finishesFirst_withT1Failing() throws InterruptedException {
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 2000, true);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue2());
        }
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(VersionedEntity::newEntity, VersionedEntity.class, 2000, true);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void t1finishesFirst_withT1Failing() throws InterruptedException {
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(NonVersionedEntity::newEntity, NonVersionedEntity.class, 1250, true);
            Assertions.assertNull(res.getValue0());
            Assertions.assertTrue(res.getValue2());
        }
        {
            Triplet<DemoEntity, Boolean, Boolean> res = doDemoConcurrentDeleteUpdate(VersionedEntity::newEntity, VersionedEntity.class, 1250, true);
            Assertions.assertNull(res.getValue0());
            Assertions.assertTrue(res.getValue2());
        }
    }

}
