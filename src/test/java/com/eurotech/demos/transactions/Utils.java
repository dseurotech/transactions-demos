package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
import com.eurotech.persistence.transactions.TxContext;

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
        final E found = new DemoEntityRepository<>(clazz).find(tx, id, lockModeType).orElse(null);
        print(thread, found);
        return found;
    }

    protected static void print(String thread, Object message) {
        System.out.println(time() + " - " + thread + ": " + message);
    }

}
