package com.eurotech.demos.transactionsless;

import com.eurotech.demos.transactions.DemoEntity;
import org.eclipse.kapua.commons.jpa.DemoDAO;
import org.eclipse.kapua.commons.jpa.EntityManagerSession;

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

    protected static DemoEntity createEntity(EntityManagerSession ems, String content) {
        // create new demoEntity
        final DemoEntity demoEntity = new DemoEntity();
        demoEntity.setContent(content);
        return ems.doTransactedAction(e -> DemoDAO.create(e, demoEntity));
    }

    protected static DemoEntity fetchAndPrint(EntityManagerSession ems, String thread, Long id) {
        final DemoEntity found = ems.doAction(e -> DemoDAO.find(e, id));
        print(thread, found);
        return found;
    }

    protected static void print(String thread, Object message) {
        System.out.println(time() + " - " + thread + ": " + message);
    }
}
