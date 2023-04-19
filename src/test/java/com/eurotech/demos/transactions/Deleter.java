package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
import com.eurotech.persistence.transactions.TxManager;

import javax.persistence.LockModeType;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class Deleter<E extends DemoEntity> extends Thread {
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

    public Deleter(
            String threadName,
            Class<E> clazz,
            Supplier<TxManager> txManagerSupplier,
            Long idToDelete,
            LockModeType readLockModeType,
            int sleepBetweenReadAndUpdateMs,
            LockModeType preDeleteLockModeType,
            boolean throwAfterDelete,
            DemoEntityRepository demoEntityRepository) {
        this.threadName = threadName;
        this.clazz = clazz;
        this.txManagerSupplier = txManagerSupplier;
        this.idToUpdate = idToDelete;
        this.readLockModeType = readLockModeType;
        this.sleepBetweenReadAndUpdateMs = sleepBetweenReadAndUpdateMs;
        this.preUpdateLockModeType = preDeleteLockModeType;
        this.throwAfterUpdate = throwAfterDelete;
        this.demoEntityRepository = demoEntityRepository;
    }

    @Override
    public void run() {
        try {


            Utils.print(threadName, "started");
            txManagerSupplier.get().execute(tx -> {
                final DemoEntity entity = Utils.fetchAndPrint(tx, clazz, threadName, idToUpdate, readLockModeType);

                Utils.sleep(sleepBetweenReadAndUpdateMs);

                demoEntityRepository.delete(tx, entity);
                Utils.print(threadName, "deleted the entity");

                final E res = Utils.fetchAndPrint(tx, clazz, threadName, idToUpdate, readLockModeType);
                if (throwAfterUpdate) {
                    Utils.print(threadName, "Goes baboom!");
                    throw new RuntimeException("BABOOM!!");
                }
                return res;
            });
        } catch (Throwable t) {
            this.threadFailed.set(true);
        }
    }
}
