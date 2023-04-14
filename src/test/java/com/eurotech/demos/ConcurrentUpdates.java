package com.eurotech.demos;

import com.eurotech.demos.transactions.DemoEntity;
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

    private DemoEntity demoConcurrentUpdate(LockModeType t1LockType, boolean flushT1, LockModeType t2LockType) throws InterruptedException {
        System.out.println("\n\n***************************************");
        Utils.print("TEST", String.format("t1 lock: %s, t1 flushes: %s, t2 lock: %s", t1LockType, flushT1, t2LockType));

        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> Utils.createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            Utils.print("T1", "started");
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Found = Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), t1LockType);
                t1Found.setContent("Content from T1");
                Utils.print("T1", "changed entity content");
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                //Completely superfluous
                em.persist(t1Found);
                if (flushT1) {
                    em.flush();
                }
                Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                Utils.sleep(2000);
                return Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
            });
        });
        Thread t2 = new Thread(() -> {
            Utils.print("T2", "started");
            final TxManager txManagerT2 = txManagerFactory.create("demos");
            txManagerT2.execute(tx -> {
                final DemoEntity t2Found = Utils.fetchAndPrint(tx, "T2", initialEntity.getId(), t2LockType);
                t2Found.setContent("Content from T2");
                Utils.print("T2", "changed entity content");
                return Utils.fetchAndPrint(tx, "T2", initialEntity.getId(), LockModeType.NONE);
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

    @Test
    public void demoConcurrentUpdate() throws InterruptedException {
        demoConcurrentUpdate(LockModeType.NONE, false, LockModeType.NONE);
    }

    @Test
    public void demoConcurrentUpdateScenarios() throws InterruptedException {
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.NONE, false, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T1", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.NONE, true, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.PESSIMISTIC_READ);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdate(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.PESSIMISTIC_READ);
            Assertions.assertEquals(2, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
    }
}