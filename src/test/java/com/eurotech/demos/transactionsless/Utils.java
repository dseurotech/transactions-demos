package com.eurotech.demos.transactionsless;

import com.eurotech.demos.transactions.DemoEntity;
import org.eclipse.kapua.commons.jpa.DemoEntityDAO;
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

    protected static <E extends DemoEntity> DemoEntity fetchAndPrint(EntityManagerSession ems, DemoEntityDAO demoEntityDAO, String thread, Long id) {
        final DemoEntity found = ems.doAction(e -> demoEntityDAO.find(e, id));
        print(thread, found);
        return found;
    }

    protected static void print(String thread, Object message) {
        System.out.println(time() + " - " + thread + ": " + message);
    }
}
