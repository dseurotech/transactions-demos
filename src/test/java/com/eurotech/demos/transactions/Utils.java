package com.eurotech.demos.transactions;

import com.eurotech.persistence.transactions.TxContext;
import com.eurotech.persistence.transactions.jpa.JpaAwareTxContext;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

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

    protected static <E extends DemoEntity> E fetchAndPrint(TxContext tx, Class<E> clazz, String thread, Long id, LockModeType lockModeType) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        final E found = em.find(clazz, id, lockModeType);
        print(thread, found);
        return found;
    }

    protected static void print(String thread, Object message) {
        System.out.println(time() + " - " + thread + ": " + message);
    }

}
