package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
import com.eurotech.persistence.transactions.TxManager;

import javax.persistence.LockModeType;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class Updater<E extends DemoEntity> extends Thread {
    private final String threadName;
    private final Class<E> clazz;
    private final Supplier<TxManager> txManagerSupplier;
    private final Long idToUpdate;
    private final LockModeType readLockModeType;
    private final int sleepBetweenReadAndUpdateMs;
    private final LockModeType preUpdateLockModeType;
    private final boolean throwAfterUpdate;
    private final DemoEntityRepository demoEntityRepository;
    public final AtomicBoolean threadFailed = new AtomicBoolean(false);

    public Updater(
            String threadName,
            Class<E> clazz,
            Supplier<TxManager> txManagerSupplier,
            Long idToUpdate,
            LockModeType readLockModeType,
            int sleepBetweenReadAndUpdateMs,
            LockModeType preUpdateLockModeType,
            boolean throwAfterUpdate,
            DemoEntityRepository demoEntityRepository) {
        this.threadName = threadName;
        this.clazz = clazz;
        this.txManagerSupplier = txManagerSupplier;
        this.idToUpdate = idToUpdate;
        this.readLockModeType = readLockModeType;
        this.sleepBetweenReadAndUpdateMs = sleepBetweenReadAndUpdateMs;
        this.preUpdateLockModeType = preUpdateLockModeType;
        this.throwAfterUpdate = throwAfterUpdate;
        this.demoEntityRepository = demoEntityRepository;
    }

    @Override
    public void run() {
        try {
            Utils.print(threadName, "started");
            txManagerSupplier.get().execute(tx -> {
                final DemoEntity entity = Utils.fetchAndPrint(tx, clazz, threadName, idToUpdate, readLockModeType);

                Utils.sleep(sleepBetweenReadAndUpdateMs);
                if (entity == null) {
                    Utils.print(threadName, "notices null entity and exits");
                    return null;
                }

                entity.setContent(entity.getContent() + " plus " + threadName);
                //Completely superfluous
                demoEntityRepository.update(tx, entity, readLockModeType);
                Utils.print(threadName, "changed entity content");

                final E res = Utils.fetchAndPrint(tx, clazz, threadName, idToUpdate, readLockModeType);
                if (throwAfterUpdate) {
                    Utils.print(threadName, "Goes baboom!");
                    throw new RuntimeException("BABOOM!!");
                }
                return res;
            });
        } catch (Throwable t) {
            Utils.print(threadName, "failed");
            this.threadFailed.set(true);
        }
    }
}
