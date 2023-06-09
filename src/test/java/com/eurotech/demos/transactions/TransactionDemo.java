package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
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

    /**
     * This demonstrates the transaction isolation, and the fact that changes within the tx boundary are automatically persisted
     * at the end of the transaction, even without an explicit merge/persist of the entity
     *
     * @throws InterruptedException never, really
     */
    @Test
    public void demoAutoUpdate() throws InterruptedException {
        final DemoEntityRepository<NonVersionedEntity> repo = new DemoEntityRepository<>(NonVersionedEntity.class);
        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> repo.create(tx, NonVersionedEntity.newEntity("Entity Content")));
        Thread t1 = new Thread(() -> {
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Found = Utils.fetchAndPrint(tx, NonVersionedEntity.class, "T1", initialEntity.getId(), LockModeType.NONE);
                Utils.sleep(100);
                t1Found.setContent("Changed content");
                Utils.print("T1", "changed entity content");
                Utils.fetchAndPrint(tx, NonVersionedEntity.class, "T1", initialEntity.getId(), LockModeType.NONE);
                //Even flushing, the change is not seen from the outside thread until the session is concluded
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                em.flush();
                Utils.sleep(200);
                return Utils.fetchAndPrint(tx, NonVersionedEntity.class, "T1", initialEntity.getId(), LockModeType.NONE);
            });
        });
        t1.start();
        Utils.sleep(150);
        txManager.execute(tx -> Utils.fetchAndPrint(tx, NonVersionedEntity.class, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        Utils.print("MAIN", "T1 joined");
        txManager.execute(tx -> Utils.fetchAndPrint(tx, NonVersionedEntity.class, "MAIN", initialEntity.getId(), LockModeType.NONE));
    }
}
