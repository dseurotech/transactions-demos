package com.eurotech.demos;

import com.eurotech.demos.transactions.DemoEntity;
import com.eurotech.persistence.transactions.TxContext;
import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.jpa.JpaAwareTxContext;
import com.eurotech.persistence.transactions.jpa.JpaTxManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class TransactionsDemos {

    private JpaTxManagerFactory txManagerFactory;

    @BeforeEach
    void setUp() {
        txManagerFactory = new JpaTxManagerFactory(2);
    }

    @Test
    public void demoAutoUpdate() throws InterruptedException {
        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Found = fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                sleep(100);
                t1Found.setContent("Changed content");
                print("T1", "changed entity content");
                fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                //Even flushing, the change is not seen from the outside thread
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                em.flush();
                sleep(200);
                return fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
            });
        });
        t1.start();
        sleep(150);
        txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        print("MAIN", "T1 joined");
        txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
    }

    private DemoEntity demoConcurrentUpdate(LockModeType t1LockType, boolean flushT1, LockModeType t2LockType) throws InterruptedException {
        System.out.println("\n\n***************************************");
        print("TEST", String.format("t1 lock: %s, t1 flushes: %s, t2 lock: %s", t1LockType, flushT1, t2LockType));

        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            print("T1", "started");
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Found = fetchAndPrint(tx, "T1", initialEntity.getId(), t1LockType);
                t1Found.setContent("Content from T1");
                print("T1", "changed entity content");
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                //Completely superfluous
                em.persist(t1Found);
                if (flushT1) {
                    em.flush();
                }
                fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                sleep(2000);
                return fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
            });
        });
        Thread t2 = new Thread(() -> {
            print("T2", "started");
            final TxManager txManagerT2 = txManagerFactory.create("demos");
            txManagerT2.execute(tx -> {
                final DemoEntity t2Found = fetchAndPrint(tx, "T2", initialEntity.getId(), t2LockType);
                t2Found.setContent("Content from T2");
                print("T2", "changed entity content");
                return fetchAndPrint(tx, "T2", initialEntity.getId(), LockModeType.NONE);
            });
        });
        t1.start();
        sleep(1000);
        t2.start();
        txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        print("MAIN", "T1 joined");
        txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t2.join();
        print("MAIN", "T2 joined");
        return txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
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

    private DemoEntity demoConcurrentUpdateDelete(LockModeType t1LockType, boolean flushT1, LockModeType t2LockType) throws InterruptedException {
        System.out.println("\n\n***************************************");
        print("TEST", String.format("t1 lock: %s, t1 flushes: %s, t2 lock: %s", t1LockType, flushT1, t2LockType));

        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntity initialEntity = txManager.execute(tx -> createEntity(tx, "Initial content"));
        Thread t1 = new Thread(() -> {
            print("T1", "started");
            final TxManager txManagerT1 = txManagerFactory.create("demos");
            txManagerT1.execute(tx -> {
                final DemoEntity t1Found = fetchAndPrint(tx, "T1", initialEntity.getId(), t1LockType);
                final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
                em.remove(t1Found);
                print("T1", "deletes entity");
                if (flushT1) {
                    em.flush();
                }
                fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
                sleep(2000);
                return fetchAndPrint(tx, "T1", initialEntity.getId(), LockModeType.NONE);
            });
        });
        Thread t2 = new Thread(() -> {
            print("T2", "started");
            final TxManager txManagerT2 = txManagerFactory.create("demos");
            txManagerT2.execute(tx -> {
                final DemoEntity t2Found = fetchAndPrint(tx, "T2", initialEntity.getId(), t2LockType);
                if (t2Found == null) {
                    print("T2", "notices null entity and exits");
                    return null;
                }
                t2Found.setContent("Content from T2");
                print("T2", "changed entity content");
                return fetchAndPrint(tx, "T2", initialEntity.getId(), LockModeType.NONE);
            });
        });
        t1.start();
        sleep(1000);
        t2.start();
        txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t1.join();
        print("MAIN", "T1 joined");
        txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
        t2.join();
        print("MAIN", "T2 joined");
        return txManager.execute(tx -> fetchAndPrint(tx, "MAIN", initialEntity.getId(), LockModeType.NONE));
    }

    @Test
    public void demoConcurrentUpdateDelete() throws InterruptedException {
        demoConcurrentUpdateDelete(LockModeType.NONE, false, LockModeType.NONE);
    }

    @Test
    public void demoConcurrentUpdateDeleteScenarios() throws InterruptedException {
        {
            final DemoEntity res = demoConcurrentUpdateDelete(LockModeType.NONE, false, LockModeType.NONE);
            Assertions.assertNull(res);
        }
        {
            final DemoEntity res = demoConcurrentUpdateDelete(LockModeType.NONE, true, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdateDelete(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {
            final DemoEntity res = demoConcurrentUpdateDelete(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.NONE);
            Assertions.assertEquals(1, res.getChangesCounter());
            Assertions.assertEquals("Content from T2", res.getContent());
        }
        {

            final DemoEntity res = demoConcurrentUpdateDelete(LockModeType.PESSIMISTIC_WRITE, false, LockModeType.PESSIMISTIC_READ);
            Assertions.assertNull(res);
        }
        {

            final DemoEntity res = demoConcurrentUpdateDelete(LockModeType.PESSIMISTIC_WRITE, true, LockModeType.PESSIMISTIC_READ);
            Assertions.assertNull(res);
        }
    }

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private static String time() {
        return "Time: " + formatter.format(Instant.now());
    }

    private static void sleep(int n) {
        try {
            Thread.sleep(n);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static DemoEntity createEntity(TxContext tx, String content) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        // create new demoEntity
        final DemoEntity demoEntity = new DemoEntity();
        demoEntity.setContent(content);
        em.persist(demoEntity);
        return demoEntity;
    }

    private static DemoEntity fetchAndPrint(TxContext tx, String thread, Long id, LockModeType lockModeType) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        final DemoEntity found = em.find(DemoEntity.class, id, lockModeType);
        print(thread, found);
        return found;
    }

    private static void print(String thread, Object message) {
        System.out.println(time() + " - " + thread + ": " + message);
    }

    private static void printEntitiesList(TxContext tx) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        final Query q = em.createQuery("select t from DemoEntity t");
        final List<DemoEntity> demoEntityList = q.getResultList();
        for (DemoEntity tod : demoEntityList) {
            System.out.println(tod);
        }
        System.out.println("Size: " + demoEntityList.size());
    }
}
