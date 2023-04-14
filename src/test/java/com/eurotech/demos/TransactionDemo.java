package com.eurotech.demos;

import com.eurotech.demos.transactions.DemoEntity;
import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.jpa.JpaAwareTxContext;
import com.eurotech.persistence.transactions.jpa.JpaTxManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;

public class TransactionDemo {

    private JpaTxManagerFactory txManagerFactory;

    @BeforeEach
    void setUp() {
        txManagerFactory = new JpaTxManagerFactory(2);
    }

    @Test
    public void demoAutoUpdate() throws InterruptedException {
        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> Utils.createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Found = Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                Utils.sleep(100);
                t1Found.setContent("Changed content");
                Utils.print("T1", "changed entity content");
                Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                //Even flushing, the change is not seen from the outside thread
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                em.flush();
                Utils.sleep(200);
                return Utils.fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
            });
        });
        t1.start();
        Utils.sleep(150);
        txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        Utils.print("MAIN", "T1 joined");
        txManager.execute(tx -> Utils.fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
    }
}
