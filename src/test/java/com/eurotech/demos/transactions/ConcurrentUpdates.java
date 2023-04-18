package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.jpa.JpaTxManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.LockModeType;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConcurrentUpdates {

    private JpaTxManagerFactory txManagerFactory;

    @BeforeEach
    void setUp() {
        txManagerFactory = new JpaTxManagerFactory(2);
    }

    /**
     * This demonstrates the interaction between two different threads (T1 and T2) concurrently updating the same entity
     * T1 starts first and fetches the entity, then waits 2 seconds before updating it
     * T2 starts after 1 second, immediately fetches the entity and updates it
     * the Main thread checks the value of the entity at different points in time
     * <p>
     * This utility method is meant to be invoked by different scenarios
     *
     * @param t1LockType The type of database lock to be used reading entities in T1
     * @param t2LockType The type of database lock to be used reading entities in T1
     * @param t1Throws   Whether T1 should throw just before completing the transaction (demonstating the behaviour of the system in case of rollbacks)
     * @return The final state of the entity, fetched by the main thread after all other threads are complete
     * @throws InterruptedException never, really
     */
    private <E extends DemoEntity> DemoEntity demoConcurrentUpdate(
            Class<E> clazz,
            Function<String, E> entityCreator,
            LockModeType t1LockType,
            int t1SleepBeforeUpdate,
            LockModeType t2LockType,
            boolean t1Throws) throws InterruptedException {
        System.out.println("\n\n* Concurrent update demo with transactions **************************************");
        Utils.print("TEST", String.format("t1 lock: %s, t1 throws: %s, t2 lock: %s", t1LockType, t1Throws, t2LockType));
        final DemoEntityRepository<E> repo = new DemoEntityRepository<>(clazz);
        final TxManager txManager = txManagerFactory.create("demos");
        Supplier<TxManager> txSupplier = () -> txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> repo.create(tx, entityCreator.apply("Entity Content")));
        final Thread t1 = new Thread(new Updater(
                "T1",
                clazz,
                txSupplier,
                initialEntity.getId(),
                t1LockType,
                t1SleepBeforeUpdate,
                t1LockType,
                t1Throws,
                new DemoEntityRepository(clazz)));
        final Thread t2 = new Thread(new Updater(
                "T2",
                clazz,
                txSupplier,
                initialEntity.getId(),
                t2LockType,
                500,
                t2LockType,
                false,
                new DemoEntityRepository(clazz)));
        t1.start();
        Utils.sleep(1000);
        t2.start();
        txManager.execute(tx -> Utils.fetchAndPrint(tx, clazz, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        Utils.print("MAIN", "T1 joined");
        txManager.execute(tx -> Utils.fetchAndPrint(tx, clazz, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t2.join();
        Utils.print("MAIN", "T2 joined");
        return txManager.execute(tx -> Utils.fetchAndPrint(tx, clazz, "MAIN", initialEntity.getId(), LockModeType.NONE));
    }

    /**
     * Use this test to play around with the various parameters of {@link #demoConcurrentUpdate(Class, Function, LockModeType, int, LockModeType, boolean)}
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoConcurrentUpdate() throws InterruptedException {
//        demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity, LockModeType.NONE, LockModeType.NONE, false);
//        demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity, null, null, false);
        demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, false);
    }

    @Test
    public void lockMode_Null() throws InterruptedException {
        //Non versioned:  the last to close wins, overrides the other
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 2000, null, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 1250, null, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: merge the result, in order of transaction close
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 2000, null, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2 plus T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 1250, null, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_None() throws InterruptedException {
        //Non versioned:  the last to close wins, overrides the other
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.NONE, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.NONE, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: merge the result, in order of transaction close
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.NONE, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2 plus T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.NONE, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_Optimistic() throws InterruptedException {
        //Non versioned:  both thread fail
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.OPTIMISTIC, 2000, LockModeType.OPTIMISTIC, false);
            Assertions.assertEquals(0, res.getChangesCounter());
            Assertions.assertEquals("Entity Content", res.getContent());
        }
        //Versioned: merge the result, in order of transaction close
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.OPTIMISTIC, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2 plus T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.OPTIMISTIC, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_PessimisticWrite() throws InterruptedException {
        //Non versioned: Merge in locking order
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        //Versioned: Merge in locking order
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_NullRead() throws InterruptedException {
        //Non versioned: T1 is ignored completely
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: Merge in locking order
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_PessimisticRead() throws InterruptedException {
        //Non versioned: Merge in locking order
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        //Versioned: Merge in locking order
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T1 plus T2", res.getContent());
        }
    }


    @Test
    public void lockMode_Null_withT1Throwing() throws InterruptedException {
        //Non Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 2000, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    null, 1250, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 2000, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    null, 1250, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_None_withT1Throwing() throws InterruptedException {
        //Non Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 2000, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.NONE, 1250, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_Optimistic_withT1Throwing() throws InterruptedException {
        //Non versioned:  both thread fail
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.OPTIMISTIC, 2000, LockModeType.OPTIMISTIC, true);
            Assertions.assertEquals(0, res.getChangesCounter());
            Assertions.assertEquals("Entity Content", res.getContent());
        }
        //Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.OPTIMISTIC, 2000, LockModeType.OPTIMISTIC, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_withT1Throwing() throws InterruptedException {
        //Non versioned:  both thread fail
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_WRITE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_NullRead_withT1Throwing() throws InterruptedException {
        //Non versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, null, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
    }

    @Test
    public void lockMode_PessimisticWrite_PessimisticRead_withT1Throwing() throws InterruptedException {
        //Non versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(NonVersionedEntity.class, NonVersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        //Versioned: T2 passes
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 2000, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(VersionedEntity.class, VersionedEntity::newEntity,
                    LockModeType.PESSIMISTIC_WRITE, 1250, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Entity Content plus T2", res.getContent());
        }
    }
}
