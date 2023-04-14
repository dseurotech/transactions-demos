package com.eurotech.demos.transactions;

import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.jpa.JpaAwareTxContext;
import com.eurotech.persistence.transactions.jpa.JpaTxManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;

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
     * This utility method is meant to be invoked by the tests {@link #demoConcurrentUpdate()} (which allows to play with different combinations of the parameters)
     * and/or {@link #demoConcurrentUpdateScenarios()} and {@link #demoConcurrentUpdateScenariosWithT1Throwing()} for a full execution of common scenarios
     *
     * @param t1LockType The type of database lock to be used reading entities in T1
     * @param flushT1    Whether T1 should flush after updating its entity - this is the common go-to solution found on
     *                   stack overflow-like sites, here used to demonstrate its ineffectiveness (and sometime unexpected results)
     * @param t2LockType The type of database lock to be used reading entities in T1
     * @param t1Throws   Whether T1 should throw just before completing the transaction (demonstating the behaviour of the system in case of rollbacks)
     * @return The final state of the entity, fetched by the main thread after all other threads are complete
     * @throws InterruptedException never, really
     */
    private DemoEntity demoConcurrentUpdate(LockModeType t1LockType, boolean flushT1, LockModeType t2LockType, boolean t1Throws) throws InterruptedException {
        System.out.println("\n\n* Concurrent update demo with transactions **************************************");
        Utils.print("TEST", String.format("t1 lock: %s, t1 flushes: %s, t1 throws: %s, t2 lock: %s", t1LockType, flushT1, t1Throws, t2LockType));

        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> Utils.createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            Utils.print("T1", "started");
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Entity = Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), t1LockType);
                t1Entity.setContent("Content from T1");
                Utils.print("T1", "changed entity content");
                Utils.sleep(2000);
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                //Completely superfluous
                em.persist(t1Entity);
                if (flushT1) {
                    em.flush();
                }
                Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), t1LockType);
                if (t1Throws) {
                    Utils.print("T1", "Goes baboom!");
                    throw new RuntimeException("BABOOM!!");
                }
                return Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), t1LockType);
            });
        });
        Thread t2 = new Thread(() -> {
            Utils.print("T2", "started");
            final TxManager txManagerT2 = txManagerFactory.create("demos");
            txManagerT2.execute(tx -> {
                final DemoEntity t2Entity = Utils.fetchAndPrint(tx, "T2", initialEntity.getId(), t2LockType);
                t2Entity.setContent("Content from T2");
                Utils.print("T2", "changed entity content");
                return Utils.fetchAndPrint(tx, "T2", initialEntity.getId(), t2LockType);
            });
        });
        t1.start();
        Utils.sleep(1000);
        t2.start();
        txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        Utils.print("MAIN", "T1 joined");
        txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t2.join();
        Utils.print("MAIN", "T2 joined");
        return txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
    }

    /**
     * Use this test to play around with the various parameters of {@link #demoConcurrentUpdate(LockModeType, boolean, LockModeType, boolean)}
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoConcurrentUpdate() throws InterruptedException {
        demoConcurrentUpdate(LockModeType.NONE, false, LockModeType.NONE, false);
    }

    /**
     * Use this test to see different standard scenarios in action, played by {@link #demoConcurrentUpdate(LockModeType, boolean, LockModeType, boolean)}
     * All tests in this set will NOT have T1 throwing exception
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoConcurrentUpdateScenarios() throws InterruptedException {
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.NONE, false, LockModeType.NONE, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.NONE, true, LockModeType.NONE, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.NONE, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.NONE, false);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.PESSIMISTIC_READ, false);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
    }

    /**
     * Use this test to see different standard scenarios in action, played by {@link #demoConcurrentUpdate(LockModeType, boolean, LockModeType, boolean)}
     * All tests in this set WILL HAVE T1 throwing exception
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoConcurrentUpdateScenariosWithT1Throwing() throws InterruptedException {
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.NONE, false, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.NONE, true, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.NONE, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.PESSIMISTIC_READ, true);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
    }


    /**
     * This is the same as {@link #demoConcurrentUpdate(LockModeType, boolean, LockModeType, boolean)},
     * but with both thread always reading in {@link LockModeType#PESSIMISTIC_READ}, and promoting the lock to {@link LockModeType#PESSIMISTIC_WRITE}
     * just before applying changes to the entity
     * <p>
     * Execute the two scenarios (either with T1 throwing and rolling back, and not) using {@link #demoConcurrentUpdateScenariosWithPromotion()}
     *
     * @param t1Throws Whether T1 should throw just before completing the transaction (demonstating the behaviour of the system in case of rollbacks)
     * @return The final state of the entity, fetched by the main thread after all other threads are complete
     * @throws InterruptedException never, really
     */
    private DemoEntity demoConcurrentUpdateWithPromotion(boolean t1Throws) throws InterruptedException {
        System.out.println("\n\n* Concurrent update demo with transaction promotion **************************************");
        Utils.print("TEST", String.format("t1 throws: %s", t1Throws));

        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> Utils.createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            Utils.print("T1", "started");
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Entity = Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.PESSIMISTIC_READ);
                t1Entity.setContent("Content from T1");
                Utils.print("T1", "changed entity content");
                Utils.sleep(2000);
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                em.lock(t1Entity, LockModeType.PESSIMISTIC_WRITE);
                //Completely superfluous
                em.persist(t1Entity);
                Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.PESSIMISTIC_READ);
                if (t1Throws) {
                    Utils.print("T1", "Goes baboom!");
                    throw new RuntimeException("BABOOM!!");
                }
                return Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.PESSIMISTIC_READ);
            });
        });
        Thread t2 = new Thread(() -> {
            Utils.print("T2", "started");
            final TxManager txManagerT2 = txManagerFactory.create("demos");
            txManagerT2.execute(tx -> {
                final DemoEntity t2Entity = Utils.fetchAndPrint(tx, "T2", initialEntity.getId(), LockModeType.PESSIMISTIC_READ);
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                em.lock(t2Entity, LockModeType.PESSIMISTIC_WRITE);
                t2Entity.setContent("Content from T2");
                Utils.print("T2", "changed entity content");
                return Utils.fetchAndPrint(tx, "T2", initialEntity.getId(), LockModeType.PESSIMISTIC_READ);
            });
        });
        t1.start();
        Utils.sleep(1000);
        t2.start();
        txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.PESSIMISTIC_READ));
        t1.join();
        Utils.print("MAIN", "T1 joined");
        txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.PESSIMISTIC_READ));
        t2.join();
        Utils.print("MAIN", "T2 joined");
        return txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.PESSIMISTIC_READ));
    }

    /**
     * Executes relevant scenarios for {@link #demoConcurrentUpdateWithPromotion(boolean)}, demonstrating the correctness of the approach
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoConcurrentUpdateScenariosWithPromotion() throws InterruptedException {
        {
            final DemoEntity res = demoConcurrentUpdateWithPromotion(false);
            //both updates have been taken into consideration, sequentially
            Assertions.assertEquals(2, res.getChangesCounter());
            //The second one to be completed wins
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdateWithPromotion(true);
            //Only the completed transaction concurs to changes count
            Assertions.assertEquals(1, res.getChangesCounter());
            //The only one to be completed wins
            Assertions.assertEquals("Content from T2", res.getContent());
        }
    }
}
