package com.eurotech.demos.transactionsless;

import com.eurotech.demos.transactions.DemoEntity;
import org.eclipse.kapua.commons.jpa.DemoEntityDAO;
import org.eclipse.kapua.commons.jpa.EntityManagerSession;

import java.util.concurrent.atomic.AtomicBoolean;

public class Deleter<E extends DemoEntity> extends Thread {

    private final String threadName;
    private final DemoEntityDAO demoEntityDAO;
    private final Long idToDelete;
    private final EntityManagerSession ems;
    private final int sleepBetweenReadAndDeleteMs;
    private final boolean throwAfterUpdate;
    public final AtomicBoolean threadFailed = new AtomicBoolean(false);


    public Deleter(String threadName, DemoEntityDAO demoEntityDAO, Long idToDelete, EntityManagerSession ems, int sleepBetweenReadAndDeleteMs, boolean throwAfterUpdate) {
        this.threadName = threadName;
        this.demoEntityDAO = demoEntityDAO;
        this.idToDelete = idToDelete;
        this.ems = ems;
        this.sleepBetweenReadAndDeleteMs = sleepBetweenReadAndDeleteMs;
        this.throwAfterUpdate = throwAfterUpdate;
    }

    @Override
    public void run() {
        try {
            Utils.print(threadName, "started");
            final DemoEntity foundEntity = Utils.fetchAndPrint(ems, demoEntityDAO, threadName, idToDelete);
            foundEntity.setContent(foundEntity.getContent() + " plus " + threadName);
            Utils.sleep(sleepBetweenReadAndDeleteMs);
            ems.doTransactedAction(e -> demoEntityDAO.delete(e, foundEntity.getId()));
            Utils.print(threadName, "changed entity content");
            if (throwAfterUpdate) {
                Utils.print(threadName, "Goes baboom!");
                throw new RuntimeException("BABOOM!!");
            }
            Utils.fetchAndPrint(ems, demoEntityDAO, threadName, idToDelete);
        } catch (Throwable t) {
            Utils.print(threadName, "failed: " + t.getMessage());
            this.threadFailed.set(true);
        }
    }
}
