package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.jpa.JpaTxManagerFactory;
import org.javatuples.Triplet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.LockModeType;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConcurrentUpdateDelete {

    private JpaTxManagerFactory txManagerFactory;

    @BeforeEach
    void setUp() {
        txManagerFactory = new JpaTxManagerFactory(2);
    }

    /**
     * This demonstrates the interaction between two different threads (T1 and T2) concurrently operating on the same entity
     * T1 starts first and fetches the entity, then waits 2 seconds before deleting it
     * T2 starts after 1 second, immediately fetches the entity and updates it
     * the Main thread checks the value of the entity at different points in time
     * <p>
     * This utility method is meant to be invoked by the tests below
     *
     * @param t1LockType The type of database lock to be used reading entities in T1
     * @param t2LockType The type of database lock to be used reading entities in T1
     * @param t1Throws   Whether T1 should throw just before completing the transaction (demonstating the behaviour of the system in case of rollbacks)
     * @return The final state of the entity, fetched by the main thread after all other threads are complete
     * @throws InterruptedException never, really
     */
    private <E extends DemoEntity> Triplet<E, Boolean, Boolean> demoConcurrentDeleteUpdate(
            Class<E> clazz,
            Function<String, E> entityCreator,
            LockModeType t1LockType,
            int t1SleepBeforeUpdate,
            LockModeType t2LockType,
            boolean t1Throws) throws InterruptedException {
        System.out.println("\n\n* Concurrent delete and update demo with transactions **************************************");
        Utils.print("TEST", String.format("class: %s, t1 lock: %s, t1 throws: %s, t2 lock: %s", clazz.getSimpleName(), t1LockType, t1Throws, t2LockType));
        final DemoEntityRepository<E> repo = new DemoEntityRepository<>(clazz);
        final TxManager txManager = txManagerFactory.create("demos");
        Supplier<TxManager> txSupplier = () -> txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> repo.create(tx, entityCreator.apply("Entity Content")));
        final Deleter t1 = new Deleter(
                "T1",
                clazz,
                txSupplier,
                initialEntity.getId(),
                t1LockType,
                t1SleepBeforeUpdate,
                t1LockType,
                t1Throws,
                new DemoEntityRepository(clazz));
        final Updater t2 = new Updater(
                "T2",
                clazz,
                txSupplier,
                initialEntity.getId(),
                t2LockType,
                500,
                t2LockType,
                false,
                new DemoEntityRepository(clazz));
        t1.start();
        Utils.sleep(1000);
        t2.start();
        txManager.execute(tx -> Utils.fetchAndPrint(tx, clazz, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        Utils.print("MAIN", "T1 joined");
        txManager.execute(tx -> Utils.fetchAndPrint(tx, clazz, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t2.join();
        Utils.print("MAIN", "T2 joined");
        return Triplet.with(
                txManager.execute(tx -> Utils.fetchAndPrint(tx, clazz, "MAIN", initialEntity.getId(), LockModeType.NONE)),
                t1.threadFailed.get(),
                t2.threadFailed.get());
    }

    @Test
    public void lockMode_Null() throws InterruptedException {
        //Non versioned, T1 finishes last: deleted, no fail (T2 succeeds but has no effect)
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 2000, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        //Non versioned, T1 finishes first: deleted, T2 fails
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 1250, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        //Versioned: T1 fails and recovers, deleting the entity
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 2000, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 1250, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_None() throws InterruptedException {
        //Non versioned, T1 finishes last: deleted, no fail (T2 succeeds but has no effect)
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.NONE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        //Non versioned, T1 finishes first: deleted, T2 fails
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.NONE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        //Versioned: T1 fails and recovers, deleting the entity
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.NONE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.NONE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_Optimistic() throws InterruptedException {
        //Non versioned:  both thread fail
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.OPTIMISTIC, 2000, LockModeType.OPTIMISTIC, false);
            Assertions.assertEquals(0, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        //Versioned: T1 fails and recovers, deleting the entity
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.OPTIMISTIC, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.OPTIMISTIC, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_PessimisticWrite() throws InterruptedException {
        //Non versioned: T2 notices the delete and does nothing
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        //Versioned: T2 notices the delete and does nothing
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_NullRead() throws InterruptedException {
        //Non versioned: T2 fails
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        //Versioned: T2 fails and recovers, noticing null entity and returning
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());

        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());

        }
    }

    @Test
    public void lockMode_PessimisticWrite_PessimisticRead() throws InterruptedException {
        //Non versioned: T2 notices the deleted entity and exits
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        //Versioned: T2 notices the deleted entity and exits
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertNull(res.getValue0());
            Assertions.assertFalse(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }


    @Test
    public void lockMode_Null_withT1Throwing() throws InterruptedException {
        //Non Versioned: T1 rolls back, T2 passes
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 2000, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 1250, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        // Versioned: T1 finishes last: T1 rolls back, T2 fails and recovers
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 2000, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        // Versioned: T1 finishes first: T1 rolls back, T2 completes
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 1250, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_None_withT1Throwing() throws InterruptedException {
        //Non Versioned: T1 rolls back, T2 passes
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 2000, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 1250, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        // Versioned: T1 finishes last: T1 rolls back, T2 fails and recovers
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 2000, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        // Versioned: T1 finishes first: T1 rolls back, T2 completes
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 1250, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_Optimistic_withT1Throwing() throws InterruptedException {
        //Non versioned:  both thread fail
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.OPTIMISTIC, 2000, LockModeType.OPTIMISTIC, true);
            Assertions.assertEquals(0, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertTrue(res.getValue2());
        }
        //Versioned: T2 fails and recovers
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.OPTIMISTIC, 2000, LockModeType.OPTIMISTIC, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_withT1Throwing() throws InterruptedException {
        //Non versioned: T2 notices the delete and exits
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        //Versioned: T2 notices the delete and exits
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_NullRead_withT1Throwing() throws InterruptedException {
        //Non versioned: T2 notices the delete and exits
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
        //Versioned: T2 notices the delete and exits
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertFalse(res.getValue2());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_PessimisticRead_withT1Throwing() throws InterruptedException {
        //Non versioned: T2 notices the delete and exits
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<NonVersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        //Versioned: T2 notices the delete and exits
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
        {
            final Triplet<VersionedEntity, Boolean, Boolean> res = demoConcurrentDeleteUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getValue0().getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getValue0().getContent());
            Assertions.assertTrue(res.getValue1());
            Assertions.assertFalse(res.getValue2());
        }
    }
}
