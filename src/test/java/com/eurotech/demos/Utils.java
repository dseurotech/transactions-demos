package com.eurotech.demos;

import com.eurotech.demos.transactions.DemoEntity;
import com.eurotech.persistence.transactions.TxContext;
import com.eurotech.persistence.transactions.jpa.JpaAwareTxContext;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Utils {

    protected static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    protected static String time() {
        return "Time: " + formatter.format(Instant.now());
    }

    protected static void sleep(int n) {
        try {
            Thread.sleep(n);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected static DemoEntity createEntity(TxContext tx, String content) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        // create new demoEntity
        final DemoEntity demoEntity = new DemoEntity();
        demoEntity.setContent(content);
        em.persist(demoEntity);
        return demoEntity;
    }

    protected static DemoEntity fetchAndPrint(TxContext tx, String thread, Long id, LockModeType lockModeType) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        final DemoEntity found = em.find(DemoEntity.class, id, lockModeType);
        print(thread, found);
        return found;
    }

    protected static void print(String thread, Object message) {
        System.out.println(time() + " - " + thread + ": " + message);
    }

    protected static void printEntitiesList(TxContext tx) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        final Query q = em.createQuery("select t from DemoEntity t");
        final List<DemoEntity> demoEntityList = q.getResultList();
        for (DemoEntity tod : demoEntityList) {
            System.out.println(tod);
        }
        System.out.println("Size: " + demoEntityList.size());
    }
}
