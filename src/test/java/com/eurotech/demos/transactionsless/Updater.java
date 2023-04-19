package com.eurotech.demos.transactionsless;

import com.eurotech.demos.transactions.DemoEntity;
import org.eclipse.kapua.commons.jpa.DemoEntityDAO;
import org.eclipse.kapua.commons.jpa.EntityManagerSession;

import java.util.concurrent.atomic.AtomicBoolean;

public class Updater<E extends DemoEntity> extends Thread {

    private final String threadName;
    private final DemoEntityDAO demoEntityDAO;
    private final Long idToUpdate;
    private final EntityManagerSession ems;
    private final int sleepBetweenReadAndUpdateMs;
    private final boolean throwAfterUpdate;
    public final AtomicBoolean threadFailed = new AtomicBoolean(false);


    public Updater(String threadName, DemoEntityDAO demoEntityDAO, Long idToUpdate, EntityManagerSession ems, int sleepBetweenReadAndUpdateMs, boolean throwAfterUpdate) {
        this.threadName = threadName;
        this.demoEntityDAO = demoEntityDAO;
        this.idToUpdate = idToUpdate;
        this.ems = ems;
        this.sleepBetweenReadAndUpdateMs = sleepBetweenReadAndUpdateMs;
        this.throwAfterUpdate = throwAfterUpdate;
    }

    @Override
    public void run() {
        try {
            Utils.print(threadName, "started");
            final DemoEntity foundEntity = Utils.fetchAndPrint(ems, demoEntityDAO, threadName, idToUpdate);
            Utils.sleep(sleepBetweenReadAndUpdateMs);
            if (foundEntity == null) {
                Utils.print(threadName, "notices null entity and exits");
                return;
            }
            foundEntity.setContent(foundEntity.getContent() + " plus " + threadName);
            ems.doTransactedAction(e -> demoEntityDAO.update(e, foundEntity));
            Utils.print(threadName, "changed entity content");
            if (throwAfterUpdate) {
                Utils.print(threadName, "Goes baboom!");
                throw new RuntimeException("BABOOM!!");
            }
            Utils.fetchAndPrint(ems, demoEntityDAO, threadName, idToUpdate);
        } catch (Throwable t) {
            Utils.print(threadName, "failed: " + t.getMessage());
            this.threadFailed.set(true);
        }
    }
}
